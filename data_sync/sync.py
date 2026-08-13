# Copyright (c) 2026, vikas moin and contributors
# For license information, please see license.txt
"""Two-way document sync between two live Frappe servers.

Outgoing: a `*` doc_events hook captures every change on a whitelisted DocType,
writes a `Doc Sync Queue` row (type = Outgoing) and pushes it to the target
server. Failures are kept on the row and retried by the scheduler.

Incoming: `data_sync.api.receive` logs a `Doc Sync Queue` row (type = Incoming)
and applies the document locally. While applying, `frappe.flags.in_data_sync`
is set so the outgoing hook ignores the change and the two servers cannot
ping-pong the same document forever.
"""

import json

import frappe
import requests
from frappe.utils import cint, now_datetime

# Keys that are local to a site or recomputed on save - never shipped.
VOLATILE_FIELDS = {
	"_user_tags",
	"_comments",
	"_assign",
	"_liked_by",
	"__islocal",
	"__unsaved",
	"__onload",
	"__last_sync_on",
	"_seen",
}

# Audit fields. They are sent with the payload, but never handed to
# insert()/update() - frappe overwrites owner/creation with the session user and
# always stamps modified_by, and feeding it the remote `modified` would raise
# TimestampMismatchError. They are written back with restore_audit_fields()
# after the save so the synced document keeps the real originating user.
AUDIT_FIELDS = ("owner", "creation", "modified", "modified_by")

# DocTypes that must never sync, whatever the settings say.
BLOCKED_DOCTYPES = {
	"Doc Sync Queue",
	"Doc Sync Settings",
	"Doc Sync DocType",
	"DocType",
	"DocField",
	"Custom Field",
	"Property Setter",
	"Version",
	"Activity Log",
	"Access Log",
	"Error Log",
	"Scheduled Job Log",
	"Route History",
	"View Log",
	"Comment",
	"Notification Log",
	"Notification Settings",
	"Email Queue",
	"Email Queue Recipient",
	"Prepared Report",
	"Deleted Document",
	"Session Default",
	"Data Import",
	"Document Follow",
	"Patch Log",
	"Installed Applications",
	"Package Release",
}

EVENT_BY_METHOD = {
	"after_insert": "Insert",
	"on_update": "Update",
	"on_submit": "Submit",
	"on_cancel": "Cancel",
	"on_trash": "Delete",
}


# ---------------------------------------------------------------------------
# settings
# ---------------------------------------------------------------------------


def get_settings():
	"""Cached settings, or None when the app is not usable yet (install/migrate)."""
	try:
		return frappe.get_cached_doc("Doc Sync Settings")
	except Exception:
		return None


def get_doctype_rule(settings, doctype):
	for row in settings.sync_doctypes or []:
		if row.ref_doctype == doctype:
			return row if row.enabled else None
	return None


def is_event_allowed(rule, event):
	return {
		"Insert": rule.sync_insert,
		"Update": rule.sync_update,
		"Submit": rule.sync_submit_cancel,
		"Cancel": rule.sync_submit_cancel,
		"Delete": rule.sync_delete,
	}.get(event)


# ---------------------------------------------------------------------------
# outgoing
# ---------------------------------------------------------------------------


def capture(doc, method=None):
	"""`doc_events` entry point for every DocType (hooked with `*`)."""
	event = EVENT_BY_METHOD.get(method)
	if not event:
		return

	# on_update also runs as part of insert - after_insert already covered it.
	if event == "Update" and getattr(doc.flags, "in_insert", False):
		return

	# The change arrived from the other server: do not send it back.
	if frappe.flags.in_data_sync:
		return

	if frappe.flags.in_install or frappe.flags.in_migrate or frappe.flags.in_patch:
		return

	if doc.doctype in BLOCKED_DOCTYPES:
		return

	settings = get_settings()
	if not settings or not settings.enabled or not settings.target_url:
		return

	rule = get_doctype_rule(settings, doc.doctype)
	if not rule or not is_event_allowed(rule, event):
		return

	payload = frappe.as_json(build_payload(doc, event))

	# A single request can save the same document more than once - a controller
	# calling self.save() from inside its own on_update, for instance. Fold the
	# change into the row that is still waiting to be pushed rather than queue
	# the same document twice: the payload is the whole document, so refreshing
	# it means the latest state is what goes out.
	pending = find_pending_entry(doc.doctype, doc.name, event)
	if pending:
		frappe.db.set_value(
			"Doc Sync Queue",
			pending,
			{"payload": payload, "status": "Queued", "error_reason": None},
			update_modified=False,
		)
		enqueue_push(pending)
		return

	entry = frappe.get_doc(
		{
			"doctype": "Doc Sync Queue",
			"type": "Outgoing",
			"event": event,
			"ref_doctype": doc.doctype,
			"ref_docname": doc.name,
			"status": "Queued",
			"origin_site": settings.site_identifier or frappe.local.site,
			"payload": payload,
		}
	)
	entry.flags.ignore_permissions = True
	entry.insert(ignore_permissions=True)
	entry.db_set(
		"idempotency_key",
		f"{entry.origin_site}:{entry.name}",
		update_modified=False,
	)

	enqueue_push(entry.name)


def find_pending_entry(doctype, docname, event):
	"""Name of an Outgoing row for this document that has not been pushed yet.

	A Delete is never merged into an upsert (or the other way round) - the two
	are not interchangeable. The existing row keeps its own event, so an Insert
	that is saved again in the same request still goes out as an Insert.
	"""
	if event == "Delete":
		return None

	return frappe.db.get_value(
		"Doc Sync Queue",
		{
			"type": "Outgoing",
			"status": "Queued",
			"ref_doctype": doctype,
			"ref_docname": docname,
			"event": ["!=", "Delete"],
		},
		"name",
		order_by="creation desc",
	)


def enqueue_push(entry_name):
	"""Queue the push. `deduplicate` keeps one in-flight job per queue row, so a
	coalesced row is not pushed twice concurrently."""
	frappe.enqueue(
		"data_sync.sync.push_entry",
		queue="short",
		enqueue_after_commit=True,
		job_id=f"data_sync::push::{entry_name}",
		deduplicate=True,
		entry_name=entry_name,
	)


def build_payload(doc, event):
	if event == "Delete":
		return {"doctype": doc.doctype, "name": doc.name}

	data = doc.as_dict(convert_dates_to_str=True)
	return clean_payload(data)


def clean_payload(data):
	if not isinstance(data, dict):
		return data

	cleaned = {}
	for key, value in data.items():
		if key in VOLATILE_FIELDS:
			continue
		if isinstance(value, list):
			cleaned[key] = [clean_payload(row) for row in value]
		else:
			cleaned[key] = value
	return cleaned


def push_entry(entry_name):
	"""Send one Outgoing queue row to the target server."""
	entry = frappe.get_doc("Doc Sync Queue", entry_name)
	if entry.type != "Outgoing" or entry.status == "Synced":
		return

	settings = get_settings()
	if not settings or not settings.enabled or not settings.target_url:
		mark_failed(entry, "Sync is disabled or Target URL is not set")
		return

	body = {
		"doctype": entry.ref_doctype,
		"docname": entry.ref_docname,
		"event": entry.event,
		"origin_site": entry.origin_site,
		"idempotency_key": entry.idempotency_key or f"{entry.origin_site}:{entry.name}",
		"payload": entry.payload,
	}

	try:
		response = requests.post(
			f"{settings.target_url.rstrip('/')}/api/method/data_sync.api.receive",
			json=body,
			headers=build_headers(settings),
			timeout=cint(settings.request_timeout) or 30,
		)
	except Exception as e:
		# No HTTP response at all (DNS, timeout, TLS) - log the traceback instead.
		mark_failed(entry, f"{type(e).__name__}: {e}", response=frappe.get_traceback())
		return

	log = format_response(response)
	if response.status_code == 200:
		entry.db_set(
			{
				"status": "Synced",
				"error_reason": None,
				"response": log,
				"last_attempt_on": now_datetime(),
				"synced_on": now_datetime(),
			},
			update_modified=False,
		)
		frappe.db.commit()
	else:
		mark_failed(entry, f"HTTP {response.status_code} {response.reason or ''}".strip(), response=log)


# Hard ceiling so a runaway HTML error page cannot blow up the row.
MAX_RESPONSE_LOG = 500_000


def format_response(response):
	"""Full response log for the queue row - status, headers and complete body."""
	body = response.text or ""
	truncated = len(body) > MAX_RESPONSE_LOG
	if truncated:
		body = body[:MAX_RESPONSE_LOG]

	log = {
		"status_code": response.status_code,
		"reason": response.reason,
		"url": response.url,
		"elapsed_seconds": response.elapsed.total_seconds() if response.elapsed else None,
		"headers": {
			k: v for k, v in response.headers.items() if k.lower() not in ("set-cookie", "authorization")
		},
		"body": body,
	}
	if truncated:
		log["body_truncated"] = True
		log["body_length"] = len(response.text or "")

	return frappe.as_json(log)


def build_headers(settings):
	headers = {"Content-Type": "application/json"}
	secret = settings.get_password("api_secret", raise_exception=False)
	if settings.api_key and secret:
		headers["Authorization"] = f"token {settings.api_key}:{secret}"
	return headers


def mark_failed(entry, reason, response=None):
	settings = get_settings()
	max_retries = cint(settings.max_retries) if settings else 5
	retry_count = cint(entry.retry_count) + 1

	entry.db_set(
		{
			"status": "Failed",
			"retry_count": retry_count,
			"error_reason": reason,
			"response": response,
			"last_attempt_on": now_datetime(),
		},
		update_modified=False,
	)
	frappe.db.commit()

	frappe.log_error(
		title=f"Doc Sync {entry.type} failed: {entry.ref_doctype} {entry.ref_docname}",
		message=f"{reason}\n\n{response or ''}",
	)

	if retry_count >= max_retries:
		frappe.logger("data_sync").error(
			f"Doc Sync Queue {entry.name} gave up after {retry_count} attempts: {reason}"
		)

	# ponytail: only the first failure is mailed - retries of the same row would
	# otherwise send one mail per attempt. Move to `>= max_retries` if the team
	# would rather hear about it once it has given up instead.
	if retry_count == 1:
		notify_failure(settings, entry, reason)


def notify_failure(settings, entry, reason):
	recipients = [r.user for r in (settings.notify_users if settings else []) if r.user]
	if not recipients:
		return

	try:
		frappe.sendmail(
			recipients=recipients,
			subject=f"Doc Sync failed: {entry.ref_doctype} {entry.ref_docname}",
			message=(
				f"<p>A {entry.type.lower()} sync failed on <b>{frappe.local.site}</b>.</p>"
				f"<ul><li>DocType: {frappe.utils.escape_html(entry.ref_doctype)}</li>"
				f"<li>Document: {frappe.utils.escape_html(entry.ref_docname)}</li>"
				f"<li>Event: {entry.event}</li>"
				f"<li>Queue Row: {entry.name}</li></ul>"
				f"<p><b>Error</b><br><pre>{frappe.utils.escape_html(reason or '')}</pre></p>"
			),
			reference_doctype="Doc Sync Queue",
			reference_name=entry.name,
			now=False,
		)
	except Exception:
		# Never let a mail problem take down the sync worker.
		frappe.log_error(title=f"Doc Sync failure mail not sent: {entry.name}")


# ---------------------------------------------------------------------------
# incoming
# ---------------------------------------------------------------------------


def log_incoming(doctype, docname, event, origin_site, idempotency_key, payload):
	"""Record an incoming change. Returns (entry, is_duplicate)."""
	existing = frappe.db.get_value(
		"Doc Sync Queue", {"idempotency_key": idempotency_key, "type": "Incoming"}, "name"
	)
	if existing:
		return frappe.get_doc("Doc Sync Queue", existing), True

	entry = frappe.get_doc(
		{
			"doctype": "Doc Sync Queue",
			"type": "Incoming",
			"event": event,
			"ref_doctype": doctype,
			"ref_docname": docname,
			"status": "Queued",
			"origin_site": origin_site,
			"idempotency_key": idempotency_key,
			"payload": payload if isinstance(payload, str) else frappe.as_json(payload),
		}
	)
	entry.flags.ignore_permissions = True
	entry.insert(ignore_permissions=True)
	return entry, False


def apply_entry(entry_name):
	"""Apply one Incoming queue row to the local database."""
	entry = frappe.get_doc("Doc Sync Queue", entry_name)
	if entry.type != "Incoming":
		return

	payload = json.loads(entry.payload) if entry.payload else {}

	frappe.flags.in_data_sync = True
	try:
		if entry.event == "Delete":
			delete_doc(entry.ref_doctype, entry.ref_docname)
		else:
			upsert_doc(entry.ref_doctype, entry.ref_docname, payload)

		frappe.db.commit()
		entry.db_set(
			{
				"status": "Synced",
				"error_reason": None,
				"last_attempt_on": now_datetime(),
				"synced_on": now_datetime(),
			},
			update_modified=False,
		)
		frappe.db.commit()
	except Exception as e:
		frappe.db.rollback()
		mark_failed(entry, f"{type(e).__name__}: {e}", response=frappe.get_traceback())
		raise
	finally:
		frappe.flags.in_data_sync = False


def delete_doc(doctype, docname):
	if not frappe.db.exists(doctype, docname):
		return
	frappe.delete_doc(
		doctype,
		docname,
		force=True,
		ignore_permissions=True,
		ignore_missing=True,
		delete_permanently=True,
	)


def upsert_doc(doctype, docname, payload):
	payload = clean_payload(payload)
	payload["doctype"] = doctype
	payload["name"] = docname
	target_docstatus = cint(payload.get("docstatus"))

	# Kept aside for restore_audit_fields(); see AUDIT_FIELDS.
	audit = {field: payload.pop(field, None) for field in AUDIT_FIELDS}

	if frappe.db.exists(doctype, docname):
		doc = frappe.get_doc(doctype, docname)
		local_docstatus = cint(doc.docstatus)

		# Nothing to do for an already cancelled document.
		if local_docstatus == 2:
			return

		payload.pop("docstatus", None)
		doc.update(payload)
		doc.flags.ignore_permissions = True
		doc.flags.ignore_mandatory = True
		doc.flags.ignore_links = True
		doc.flags.ignore_validate_update_after_submit = True
		doc.save(ignore_permissions=True)

		if target_docstatus == 1 and local_docstatus == 0:
			doc.submit()
		elif target_docstatus == 2 and local_docstatus == 1:
			doc.cancel()

		restore_audit_fields(doc, audit, is_new=False)
		return

	# Insert as a draft first so validation runs the same way it did on the
	# origin server, then move it to the remote docstatus.
	payload["docstatus"] = 0
	doc = frappe.get_doc(payload)
	doc.flags.ignore_permissions = True
	doc.flags.ignore_mandatory = True
	doc.flags.ignore_links = True
	doc.insert(
		ignore_permissions=True,
		set_name=docname,
		set_child_names=False,
	)

	if target_docstatus == 1:
		doc.submit()
	elif target_docstatus == 2:
		doc.submit()
		doc.cancel()

	restore_audit_fields(doc, audit, is_new=True)


def resolve_user(email):
	"""Return the email only if that User exists here, else None."""
	if email and frappe.db.exists("User", email):
		return email
	return None


def restore_audit_fields(doc, audit, is_new):
	"""Stamp the synced document with the user who made the change on the origin
	server instead of the API user this request authenticated as.

	If that user does not exist on this server the local value is left alone,
	so the document is never pointed at a User that isn't there.
	"""
	values = {}

	modified_by = resolve_user(audit.get("modified_by"))
	if modified_by:
		values["modified_by"] = modified_by

	if is_new:
		owner = resolve_user(audit.get("owner"))
		if owner:
			values["owner"] = owner
		if audit.get("creation"):
			values["creation"] = audit["creation"]

	if not values:
		return

	frappe.db.set_value(doc.doctype, doc.name, values, update_modified=False)
	doc.update(values)

	# Child rows are audited along with their parent.
	child_values = {k: v for k, v in values.items() if k in ("owner", "modified_by")}
	if not child_values:
		return

	for df in doc.meta.get_table_fields():
		table = frappe.qb.DocType(df.options)
		query = frappe.qb.update(table).where(
			(table.parent == doc.name) & (table.parenttype == doc.doctype)
		)
		for field, value in child_values.items():
			query = query.set(table[field], value)
		query.run()


# ---------------------------------------------------------------------------
# scheduler
# ---------------------------------------------------------------------------


def retry_failed():
	"""Retry queue rows that never made it through. Hooked to cron."""
	settings = get_settings()
	if not settings or not settings.enabled:
		return

	max_retries = cint(settings.max_retries) or 5
	limit = cint(settings.batch_size) or 50

	rows = frappe.get_all(
		"Doc Sync Queue",
		filters={
			"status": ["in", ["Queued", "Failed"]],
			"retry_count": ["<", max_retries],
		},
		fields=["name", "type"],
		order_by="creation asc",
		limit=limit,
	)

	for row in rows:
		try:
			if row.type == "Outgoing":
				push_entry(row.name)
			else:
				apply_entry(row.name)
		except Exception:
			frappe.db.rollback()
			frappe.log_error(title=f"Doc Sync retry failed: {row.name}")


# ---------------------------------------------------------------------------
# manual actions
# ---------------------------------------------------------------------------


@frappe.whitelist()
def retry_entry(entry_name):
	"""Retry a single queue row from the form. Resets the retry counter."""
	frappe.only_for("System Manager")
	entry = frappe.get_doc("Doc Sync Queue", entry_name)
	entry.db_set({"retry_count": 0, "status": "Queued"}, update_modified=False)
	frappe.db.commit()

	if entry.type == "Outgoing":
		push_entry(entry.name)
	else:
		apply_entry(entry.name)

	return frappe.db.get_value("Doc Sync Queue", entry_name, ["status", "error_reason"], as_dict=True)


@frappe.whitelist()
def resync_doc(doctype, docname):
	"""Push the current state of a document to the target server on demand."""
	frappe.only_for("System Manager")
	doc = frappe.get_doc(doctype, docname)
	capture(doc, "on_update")
	return True


@frappe.whitelist()
def test_connection():
	"""Ping the target server with the configured credentials."""
	frappe.only_for("System Manager")
	settings = get_settings()
	if not settings or not settings.target_url:
		frappe.throw("Target URL is not set")

	try:
		response = requests.get(
			f"{settings.target_url.rstrip('/')}/api/method/data_sync.api.ping",
			headers=build_headers(settings),
			timeout=cint(settings.request_timeout) or 30,
		)
	except Exception as e:
		return {"ok": False, "message": f"{type(e).__name__}: {e}"}

	if response.status_code == 200:
		payload = response.json().get("message") or {}
		return {
			"ok": True,
			"message": payload.get("message", "ok") if isinstance(payload, dict) else str(payload),
		}

	return {"ok": False, "message": f"HTTP {response.status_code}: {(response.text or '')[:500]}"}
