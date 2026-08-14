# Copyright (c) 2026, vikas moin and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

from data_sync.sync import BLOCKED_DOCTYPES


class DocSyncSettings(Document):
	def validate(self):
		self.validate_target_url()
		self.validate_doctypes()

		if self.enabled:
			if not self.site_identifier:
				frappe.throw("Site Identifier is required to enable Data Sync")
			if not self.target_url:
				frappe.throw("Target URL is required to enable Data Sync")

	def validate_target_url(self):
		if not self.target_url:
			return

		self.target_url = self.target_url.strip().rstrip("/")
		if not self.target_url.startswith(("http://", "https://")):
			frappe.throw("Target URL must start with http:// or https://")

	def validate_doctypes(self):
		seen = set()
		for row in self.sync_doctypes or []:
			if row.ref_doctype in BLOCKED_DOCTYPES:
				frappe.throw(f"Row {row.idx}: {row.ref_doctype} cannot be synced")

			if frappe.get_meta(row.ref_doctype).istable:
				frappe.throw(
					f"Row {row.idx}: {row.ref_doctype} is a child table. "
					"Sync its parent DocType instead - child rows travel with it."
				)

			if row.ref_doctype in seen:
				frappe.throw(f"Row {row.idx}: {row.ref_doctype} is listed twice")
			seen.add(row.ref_doctype)
