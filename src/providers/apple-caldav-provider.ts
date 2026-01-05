/**
 * Apple iCloud Calendar Provider (CalDAV)
 *
 * Implementation of CalDAV protocol for Apple iCloud Calendar integration.
 * Uses Basic Authentication with App-Specific Password.
 *
 * CalDAV Specification: RFC 4791
 * iCloud CalDAV: https://caldav.icloud.com/
 *
 * IMPORTANT: Users must use an App-Specific Password generated from
 * https://appleid.apple.com/account/manage - NOT their Apple ID password.
 *
 * @module apple-caldav-provider
 */

import { requestUrl } from "obsidian";
import {
	CalendarProviderBase,
	CalendarListEntry,
	FetchEventsOptions,
	ProviderError,
	WriteResult,
	UpdateEventOptions,
	formatDateForCaldav,
} from "./calendar-provider-base";
import { AppleCaldavSourceConfig } from "../types/calendar-provider";
import { IcsEvent } from "../types/ics";
import { IcsParser } from "../parsers/ics-parser";

// ============================================================================
// CalDAV Configuration
// ============================================================================

/**
 * Default iCloud CalDAV server URL
 */
const DEFAULT_CALDAV_SERVER = "https://caldav.icloud.com/";

/**
 * CalDAV XML namespaces (kept for reference)
 */
const NS = {
	DAV: "DAV:",
	CALDAV: "urn:ietf:params:xml:ns:caldav",
	APPLE: "http://apple.com/ns/ical/",
	CS: "http://calendarserver.org/ns/",
};

// ============================================================================
// Apple CalDAV Provider
// ============================================================================

/**
 * Provider implementation for Apple iCloud Calendar via CalDAV
 */
export class AppleCaldavProvider extends CalendarProviderBase<AppleCaldavSourceConfig> {
	constructor(config: AppleCaldavSourceConfig) {
		super(config);
	}

	// =========================================================================
	// Connection Management
	// =========================================================================

	/**
	 * Connect and validate credentials
	 */
	async connect(): Promise<boolean> {
		if (!this.config.appSpecificPassword) {
			this.updateStatus({
				status: "error",
				error: "App-specific password not configured",
			});
			return false;
		}

		try {
			// Simple connectivity check with PROPFIND on root
			await this.makePropfindRequest(
				this.config.serverUrl || DEFAULT_CALDAV_SERVER,
				0,
				this.buildPropfindBody(["d:current-user-principal"]),
			);

			this.updateStatus({ status: "idle" });
			return true;
		} catch (error) {
			this.handleError(error, "Connection");
			return false;
		}
	}

	/**
	 * Disconnect (no-op for Basic Auth)
	 */
	async disconnect(): Promise<void> {
		// Nothing to revoke for Basic Auth
		this.updateStatus({ status: "disabled" });
	}

	// =========================================================================
	// Calendar Operations
	// =========================================================================

	/**
	 * List all accessible calendars
	 */
	async listCalendars(): Promise<CalendarListEntry[]> {
		if (!(await this.connect())) {
			throw new ProviderError(
				"Not authenticated with iCloud Calendar",
				"auth",
			);
		}

		try {
			// Step 1: Discover calendar home set
			const homeSetUrl = await this.discoverCalendarHomeSet();

			// Step 2: List calendars in home set
			const calendars = await this.listCalendarsInHomeSet(homeSetUrl);

			return calendars;
		} catch (error) {
			throw ProviderError.from(error, "List calendars");
		}
	}

	/**
	 * Discover the calendar home set URL
	 */
	private async discoverCalendarHomeSet(): Promise<string> {
		// First, get the current user principal
		const principalResponse = await this.makePropfindRequest(
			this.config.serverUrl || DEFAULT_CALDAV_SERVER,
			0,
			this.buildPropfindBody(["d:current-user-principal"]),
		);

		const principalHref = this.extractHref(
			principalResponse,
			"current-user-principal",
		);

		if (!principalHref) {
			throw new ProviderError(
				"Could not discover user principal",
				"not_found",
			);
		}

		const principalUrl = new URL(
			principalHref,
			this.config.serverUrl,
		).toString();

		// Then, get the calendar home set from the principal
		const homeSetResponse = await this.makePropfindRequest(
			principalUrl,
			0,
			this.buildPropfindBody(["c:calendar-home-set"]),
		);

		const homeSetHref = this.extractHref(
			homeSetResponse,
			"calendar-home-set",
		);

		if (!homeSetHref) {
			throw new ProviderError(
				"Could not discover calendar home set",
				"not_found",
			);
		}

		return new URL(homeSetHref, this.config.serverUrl).toString();
	}

	/**
	 * List calendars in the calendar home set
	 */
	private async listCalendarsInHomeSet(
		homeSetUrl: string,
	): Promise<CalendarListEntry[]> {
		const response = await this.makePropfindRequest(
			homeSetUrl,
			1, // Depth 1 to get children
			this.buildPropfindBody([
				"d:resourcetype",
				"d:displayname",
				"apple:calendar-color",
				"c:calendar-description",
				"cs:getctag",
			]),
		);

		const calendars: CalendarListEntry[] = [];

		// Parse multi-status response
		const responses = this.parseMultiStatusResponses(response);

		for (const resp of responses) {
			// Check if this is a calendar collection
			if (!resp.isCalendar) continue;

			calendars.push({
				id: resp.href,
				name:
					resp.displayName ||
					this.extractCalendarNameFromHref(resp.href),
				color: resp.color,
				primary: false, // CalDAV doesn't have a concept of primary calendar
				description: resp.description,
				canWrite: true, // Assume writable for own calendars
			});
		}

		return calendars;
	}

	/**
	 * Fetch events within the specified options
	 */
	async getEvents(options: FetchEventsOptions): Promise<IcsEvent[]> {
		if (!(await this.connect())) {
			return [];
		}

		const allEvents: IcsEvent[] = [];
		this.setSyncing(true);

		try {
			// Determine which calendars to fetch
			const calendarHrefs = options.calendarIds?.length
				? options.calendarIds
				: this.config.calendarHrefs;

			if (calendarHrefs.length === 0) {
				console.warn("[AppleCaldavProvider] No calendars configured");
				return [];
			}

			// Fetch events from each calendar
			for (const calHref of calendarHrefs) {
				// Check for cancellation
				if (options.signal?.aborted) {
					throw new ProviderError("Request cancelled", "unknown");
				}

				try {
					const events = await this.fetchEventsFromCalendar(
						calHref,
						options,
					);
					allEvents.push(...events);
				} catch (error) {
					console.error(
						`[AppleCaldavProvider] Error fetching ${calHref}:`,
						error,
					);
				}
			}

			this.updateStatus({
				status: "idle",
				lastSync: Date.now(),
				eventCount: allEvents.length,
			});
		} catch (error) {
			this.handleError(error, "Fetch events");
		} finally {
			this.setSyncing(false);
		}

		return allEvents;
	}

	/**
	 * Fetch events from a single calendar using CalDAV REPORT
	 */
	private async fetchEventsFromCalendar(
		calendarHref: string,
		options: FetchEventsOptions,
	): Promise<IcsEvent[]> {
		const calendarUrl = new URL(
			calendarHref,
			this.config.serverUrl,
		).toString();

		// Build calendar-query REPORT
		const startStr = formatDateForCaldav(options.range.start);
		const endStr = formatDateForCaldav(options.range.end);

		const reportBody = `<?xml version="1.0" encoding="utf-8"?>
<c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
	<d:prop>
		<d:getetag/>
		<d:href/>
		<c:calendar-data/>
	</d:prop>
	<c:filter>
		<c:comp-filter name="VCALENDAR">
			<c:comp-filter name="VEVENT">
				<c:time-range start="${startStr}" end="${endStr}"/>
			</c:comp-filter>
		</c:comp-filter>
	</c:filter>
</c:calendar-query>`;

		const response = await requestUrl({
			url: calendarUrl,
			method: "REPORT",
			headers: {
				Authorization: this.getAuthHeader(),
				"Content-Type": "application/xml; charset=utf-8",
				Depth: "1",
			},
			body: reportBody,
			throw: false,
		});

		if (response.status === 401) {
			throw new ProviderError(
				"Authentication failed - check your App-Specific Password",
				"auth",
			);
		}

		if (response.status >= 400) {
			throw new ProviderError(
				`CalDAV REPORT failed: ${response.status}`,
				"unknown",
			);
		}

		// Parse the multi-status response and extract ICS data with metadata
		const eventDataList = this.extractCalendarDataWithMetadata(
			response.text,
		);
		const events: IcsEvent[] = [];

		for (const eventData of eventDataList) {
			try {
				// Use existing IcsParser to parse the ICS content
				const parsed = IcsParser.parse(
					eventData.icsContent,
					this.config as any,
				);

				// Enhance events with CalDAV metadata for write operations
				for (const event of parsed.events) {
					event.providerEventId = eventData.href;
					event.providerCalendarId = calendarHref;
					event.etag = eventData.etag;
					event.canEdit = true;
					events.push(event);
				}
			} catch (error) {
				console.warn(
					"[AppleCaldavProvider] Failed to parse ICS block:",
					error,
				);
			}
		}

		return events;
	}

	// =========================================================================
	// Write Operations (Two-way Sync)
	// =========================================================================

	/**
	 * Check if this provider supports write operations
	 */
	override supportsWrite(): boolean {
		return true;
	}

	/**
	 * Check if a specific calendar can be written to
	 */
	override canWriteToCalendar(calendarId?: string): boolean {
		// For CalDAV, we assume all own calendars are writable
		return !!this.config.appSpecificPassword;
	}

	/**
	 * Create a new event in the calendar
	 */
	override async createEvent(
		event: IcsEvent,
		calendarId?: string,
	): Promise<WriteResult> {
		if (!(await this.connect())) {
			return { success: false, error: "Not authenticated" };
		}

		const targetCalendarId =
			calendarId ||
			event.providerCalendarId ||
			this.config.calendarHrefs[0];

		if (!targetCalendarId) {
			return { success: false, error: "No calendar specified" };
		}

		// Generate UID if missing
		if (!event.uid) {
			event.uid = this.generateUUID();
		}

		try {
			const calendarUrl = new URL(
				targetCalendarId,
				this.config.serverUrl,
			).toString();

			// Construct resource URL: calendar URL + UID + .ics
			const resourceUrl = `${calendarUrl}${calendarUrl.endsWith("/") ? "" : "/"}${event.uid}.ics`;

			const icsBody = this.generateIcsString(event);

			const response = await requestUrl({
				url: resourceUrl,
				method: "PUT",
				headers: {
					Authorization: this.getAuthHeader(),
					"Content-Type": "text/calendar; charset=utf-8",
					"If-None-Match": "*", // Ensure we don't overwrite existing
				},
				body: icsBody,
				throw: false,
			});

			if (
				response.status === 201 ||
				response.status === 204 ||
				response.status === 200
			) {
				// Fetch the ETag from response if available
				const newEtag =
					response.headers["etag"] || response.headers["ETag"];

				console.log(
					`[AppleCaldavProvider] Created event: ${event.uid}`,
				);

				return {
					success: true,
					event: {
						...event,
						providerEventId: resourceUrl,
						providerCalendarId: targetCalendarId,
						etag: newEtag,
					},
				};
			} else if (response.status === 412) {
				return {
					success: false,
					error: "Event already exists (UID conflict)",
					conflict: true,
				};
			} else if (response.status === 401) {
				return {
					success: false,
					error: "Authentication failed - check your App-Specific Password",
				};
			} else if (response.status === 403) {
				return {
					success: false,
					error: "Permission denied - you may not have write access to this calendar",
				};
			} else {
				return {
					success: false,
					error: `Create failed: HTTP ${response.status}`,
				};
			}
		} catch (error) {
			const errorMessage =
				error instanceof Error ? error.message : String(error);
			console.error(
				"[AppleCaldavProvider] Failed to create event:",
				error,
			);
			return { success: false, error: errorMessage };
		}
	}

	/**
	 * Update an existing event
	 */
	override async updateEvent(
		options: UpdateEventOptions,
	): Promise<WriteResult> {
		if (!(await this.connect())) {
			return { success: false, error: "Not authenticated" };
		}

		const { event, originalEvent, calendarId } = options;

		// Determine the resource URL
		let resourceUrl = event.providerEventId;

		// Fallback: construct it from calendarId + uid if providerEventId is missing
		if (!resourceUrl && (calendarId || event.providerCalendarId)) {
			const targetCalendarId = calendarId || event.providerCalendarId;
			if (targetCalendarId && event.uid) {
				const baseUrl = new URL(
					targetCalendarId,
					this.config.serverUrl,
				).toString();
				resourceUrl = `${baseUrl}${baseUrl.endsWith("/") ? "" : "/"}${event.uid}.ics`;
			}
		}

		if (!resourceUrl) {
			return {
				success: false,
				error: "Cannot determine event URL for update",
			};
		}

		// Resolve absolute URL
		const fullUrl = new URL(resourceUrl, this.config.serverUrl).toString();

		try {
			const icsBody = this.generateIcsString(event);

			const headers: Record<string, string> = {
				Authorization: this.getAuthHeader(),
				"Content-Type": "text/calendar; charset=utf-8",
			};

			// Optimistic locking with ETag
			const etag = originalEvent?.etag || event.etag;
			if (etag) {
				headers["If-Match"] = etag;
			}

			const response = await requestUrl({
				url: fullUrl,
				method: "PUT",
				headers,
				body: icsBody,
				throw: false,
			});

			if (response.status === 204 || response.status === 200) {
				const newEtag =
					response.headers["etag"] || response.headers["ETag"];

				console.log(
					`[AppleCaldavProvider] Updated event: ${event.uid}`,
				);

				return {
					success: true,
					event: {
						...event,
						etag: newEtag,
					},
				};
			} else if (response.status === 412) {
				return {
					success: false,
					error: "Conflict: The event was modified on the server. Please refresh and try again.",
					conflict: true,
				};
			} else if (response.status === 401) {
				return {
					success: false,
					error: "Authentication failed - check your App-Specific Password",
				};
			} else if (response.status === 403) {
				return {
					success: false,
					error: "Permission denied - you may not have write access to this calendar",
				};
			} else if (response.status === 404) {
				return {
					success: false,
					error: "Event not found - it may have been deleted",
				};
			} else {
				return {
					success: false,
					error: `Update failed: HTTP ${response.status}`,
				};
			}
		} catch (error) {
			const errorMessage =
				error instanceof Error ? error.message : String(error);
			console.error(
				"[AppleCaldavProvider] Failed to update event:",
				error,
			);
			return { success: false, error: errorMessage };
		}
	}

	/**
	 * Delete an event from the calendar
	 */
	override async deleteEvent(
		eventId: string,
		calendarId?: string,
		etag?: string,
	): Promise<WriteResult> {
		if (!(await this.connect())) {
			return { success: false, error: "Not authenticated" };
		}

		// Determine resource URL
		let resourceUrl = eventId;

		// If eventId looks like a UID (no slashes) and we have calendarId, construct URL
		if (!eventId.includes("/") && calendarId) {
			const baseUrl = new URL(
				calendarId,
				this.config.serverUrl,
			).toString();
			resourceUrl = `${baseUrl}${baseUrl.endsWith("/") ? "" : "/"}${eventId}.ics`;
		}

		const fullUrl = new URL(resourceUrl, this.config.serverUrl).toString();

		try {
			const headers: Record<string, string> = {
				Authorization: this.getAuthHeader(),
			};

			// Optimistic locking with ETag
			if (etag) {
				headers["If-Match"] = etag;
			}

			const response = await requestUrl({
				url: fullUrl,
				method: "DELETE",
				headers,
				throw: false,
			});

			if (response.status === 204 || response.status === 200) {
				console.log(`[AppleCaldavProvider] Deleted event: ${eventId}`);
				return { success: true };
			} else if (response.status === 404) {
				// Already deleted - consider success
				return { success: true };
			} else if (response.status === 412) {
				return {
					success: false,
					error: "Conflict: The event was modified on the server",
					conflict: true,
				};
			} else if (response.status === 401) {
				return {
					success: false,
					error: "Authentication failed - check your App-Specific Password",
				};
			} else if (response.status === 403) {
				return {
					success: false,
					error: "Permission denied - you may not have write access to this calendar",
				};
			} else {
				return {
					success: false,
					error: `Delete failed: HTTP ${response.status}`,
				};
			}
		} catch (error) {
			const errorMessage =
				error instanceof Error ? error.message : String(error);
			console.error(
				"[AppleCaldavProvider] Failed to delete event:",
				error,
			);
			return { success: false, error: errorMessage };
		}
	}

	// =========================================================================
	// ICS Generation
	// =========================================================================

	/**
	 * Generate VCALENDAR string from IcsEvent
	 */
	private generateIcsString(event: IcsEvent): string {
		const lines: string[] = [];

		lines.push("BEGIN:VCALENDAR");
		lines.push("VERSION:2.0");
		lines.push("PRODID:-//Task Genius//Obsidian Plugin//EN");
		lines.push("CALSCALE:GREGORIAN");
		lines.push("METHOD:PUBLISH");
		lines.push("BEGIN:VEVENT");

		// Required fields
		lines.push(`UID:${event.uid}`);
		lines.push(`DTSTAMP:${formatDateForCaldav(new Date())}`);

		// Date handling
		if (event.allDay) {
			// VALUE=DATE format: YYYYMMDD
			const startDate = this.formatDateOnly(event.dtstart);
			lines.push(`DTSTART;VALUE=DATE:${startDate}`);

			if (event.dtend) {
				// For all-day events, end date is exclusive in ICS
				// Add one day to make it exclusive
				const endDate = new Date(event.dtend);
				endDate.setDate(endDate.getDate() + 1);
				lines.push(`DTEND;VALUE=DATE:${this.formatDateOnly(endDate)}`);
			}
		} else {
			// Standard DateTime in UTC
			lines.push(`DTSTART:${formatDateForCaldav(event.dtstart)}`);
			if (event.dtend) {
				lines.push(`DTEND:${formatDateForCaldav(event.dtend)}`);
			}
		}

		// Optional fields
		if (event.summary) {
			lines.push(`SUMMARY:${this.escapeIcsText(event.summary)}`);
		}
		if (event.description) {
			lines.push(`DESCRIPTION:${this.escapeIcsText(event.description)}`);
		}
		if (event.location) {
			lines.push(`LOCATION:${this.escapeIcsText(event.location)}`);
		}
		if (event.status) {
			lines.push(`STATUS:${event.status.toUpperCase()}`);
		}
		if (event.transp) {
			lines.push(`TRANSP:${event.transp.toUpperCase()}`);
		}
		if (event.priority !== undefined) {
			lines.push(`PRIORITY:${event.priority}`);
		}

		// Recurrence rule
		if (event.rrule) {
			// RRULE might already include the prefix
			const rrule = event.rrule.startsWith("RRULE:")
				? event.rrule.substring(6)
				: event.rrule;
			lines.push(`RRULE:${rrule}`);
		}

		// Categories
		if (event.categories && event.categories.length > 0) {
			lines.push(`CATEGORIES:${event.categories.join(",")}`);
		}

		// Organizer
		if (event.organizer?.email) {
			const orgName = event.organizer.name
				? `;CN=${this.escapeIcsText(event.organizer.name)}`
				: "";
			lines.push(`ORGANIZER${orgName}:mailto:${event.organizer.email}`);
		}

		// Attendees
		if (event.attendees && event.attendees.length > 0) {
			for (const attendee of event.attendees) {
				if (attendee.email) {
					let attendeeLine = "ATTENDEE";
					if (attendee.name) {
						attendeeLine += `;CN=${this.escapeIcsText(attendee.name)}`;
					}
					if (attendee.role) {
						attendeeLine += `;ROLE=${attendee.role}`;
					}
					if (attendee.status) {
						attendeeLine += `;PARTSTAT=${attendee.status}`;
					}
					attendeeLine += `:mailto:${attendee.email}`;
					lines.push(attendeeLine);
				}
			}
		}

		// Created/Modified timestamps
		if (event.created) {
			lines.push(`CREATED:${formatDateForCaldav(event.created)}`);
		}
		if (event.lastModified) {
			lines.push(
				`LAST-MODIFIED:${formatDateForCaldav(event.lastModified)}`,
			);
		}

		lines.push("END:VEVENT");
		lines.push("END:VCALENDAR");

		// Join with CRLF as per ICS spec
		return lines.join("\r\n");
	}

	/**
	 * Format date as YYYYMMDD for all-day events
	 */
	private formatDateOnly(date: Date): string {
		const year = date.getFullYear();
		const month = String(date.getMonth() + 1).padStart(2, "0");
		const day = String(date.getDate()).padStart(2, "0");
		return `${year}${month}${day}`;
	}

	/**
	 * Escape special characters for ICS text fields
	 * Per RFC 5545, these characters need escaping: backslash, semicolon, comma, newline
	 */
	private escapeIcsText(text: string): string {
		return text
			.replace(/\\/g, "\\\\")
			.replace(/;/g, "\\;")
			.replace(/,/g, "\\,")
			.replace(/\r?\n/g, "\\n");
	}

	/**
	 * Generate a UUID for new events
	 */
	private generateUUID(): string {
		// Use crypto.randomUUID if available, otherwise fallback
		if (typeof crypto !== "undefined" && crypto.randomUUID) {
			return crypto.randomUUID();
		}

		// Fallback UUID v4 generation
		return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(
			/[xy]/g,
			function (c) {
				const r = (Math.random() * 16) | 0;
				const v = c === "x" ? r : (r & 0x3) | 0x8;
				return v.toString(16);
			},
		);
	}

	// =========================================================================
	// CalDAV Request Helpers
	// =========================================================================

	/**
	 * Generate Basic Auth header
	 */
	private getAuthHeader(): string {
		const credentials = `${this.config.username}:${this.config.appSpecificPassword}`;
		return `Basic ${btoa(credentials)}`;
	}

	/**
	 * Make a PROPFIND request
	 */
	private async makePropfindRequest(
		url: string,
		depth: number,
		body: string,
	): Promise<string> {
		const response = await requestUrl({
			url,
			method: "PROPFIND",
			headers: {
				Authorization: this.getAuthHeader(),
				"Content-Type": "application/xml; charset=utf-8",
				Depth: depth.toString(),
			},
			body,
			throw: false,
		});

		if (response.status === 401) {
			throw new ProviderError(
				"Authentication failed - check your App-Specific Password",
				"auth",
			);
		}

		if (response.status >= 400 && response.status !== 207) {
			throw new ProviderError(
				`CalDAV PROPFIND failed: ${response.status}`,
				"unknown",
			);
		}

		return response.text;
	}

	/**
	 * Build PROPFIND request body
	 */
	private buildPropfindBody(props: string[]): string {
		const propElements = props
			.map((prop) => {
				const [prefix, name] = prop.includes(":")
					? prop.split(":")
					: ["d", prop];
				return `<${prefix}:${name}/>`;
			})
			.join("\n\t\t");

		return `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav" xmlns:apple="http://apple.com/ns/ical/" xmlns:cs="http://calendarserver.org/ns/">
	<d:prop>
		${propElements}
	</d:prop>
</d:propfind>`;
	}

	// =========================================================================
	// XML Parsing Helpers
	// =========================================================================

	/**
	 * Extract href from a specific property in XML response
	 */
	private extractHref(xml: string, propertyName: string): string | null {
		// Look for the property container and extract the href
		const propertyRegex = new RegExp(
			`<[^>]*${propertyName}[^>]*>\\s*<[^>]*href[^>]*>([^<]+)<`,
			"i",
		);
		const match = xml.match(propertyRegex);
		return match ? match[1].trim() : null;
	}

	/**
	 * Parse multi-status response into structured objects
	 */
	private parseMultiStatusResponses(xml: string): Array<{
		href: string;
		isCalendar: boolean;
		displayName?: string;
		color?: string;
		description?: string;
		ctag?: string;
	}> {
		const results: Array<{
			href: string;
			isCalendar: boolean;
			displayName?: string;
			color?: string;
			description?: string;
			ctag?: string;
		}> = [];

		// Split by response elements
		const responseRegex = /<d:response[^>]*>([\s\S]*?)<\/d:response>/gi;
		let responseMatch;

		while ((responseMatch = responseRegex.exec(xml)) !== null) {
			const responseContent = responseMatch[1];

			// Extract href
			const hrefMatch = responseContent.match(
				/<d:href[^>]*>([^<]+)<\/d:href>/i,
			);
			if (!hrefMatch) continue;

			const href = hrefMatch[1].trim();

			// Check if it's a calendar (has calendar resourcetype)
			const isCalendar = /<c:calendar\s*\/>|<cal:calendar\s*\/>/i.test(
				responseContent,
			);

			// Extract display name
			const displayNameMatch = responseContent.match(
				/<d:displayname[^>]*>([^<]*)<\/d:displayname>/i,
			);

			// Extract calendar color (Apple specific)
			const colorMatch = responseContent.match(
				/<apple:calendar-color[^>]*>([^<]+)<\/apple:calendar-color>/i,
			);

			// Extract description
			const descriptionMatch = responseContent.match(
				/<c:calendar-description[^>]*>([^<]*)<\/c:calendar-description>/i,
			);

			// Extract ctag
			const ctagMatch = responseContent.match(
				/<cs:getctag[^>]*>([^<]+)<\/cs:getctag>/i,
			);

			results.push({
				href,
				isCalendar,
				displayName: displayNameMatch?.[1]?.trim(),
				color: this.normalizeAppleColor(colorMatch?.[1]?.trim()),
				description: descriptionMatch?.[1]?.trim(),
				ctag: ctagMatch?.[1]?.trim(),
			});
		}

		return results;
	}

	/**
	 * Extract calendar-data with metadata (href, etag) from REPORT response
	 */
	private extractCalendarDataWithMetadata(xml: string): Array<{
		href: string;
		etag?: string;
		icsContent: string;
	}> {
		const results: Array<{
			href: string;
			etag?: string;
			icsContent: string;
		}> = [];

		// Split by response elements
		const responseRegex = /<d:response[^>]*>([\s\S]*?)<\/d:response>/gi;
		let responseMatch;

		while ((responseMatch = responseRegex.exec(xml)) !== null) {
			const responseContent = responseMatch[1];

			// Extract href
			const hrefMatch = responseContent.match(
				/<d:href[^>]*>([^<]+)<\/d:href>/i,
			);
			if (!hrefMatch) continue;

			const href = hrefMatch[1].trim();

			// Extract etag
			const etagMatch = responseContent.match(
				/<d:getetag[^>]*>([^<]+)<\/d:getetag>/i,
			);
			const etag = etagMatch?.[1]?.trim();

			// Extract calendar-data
			const calDataMatch = responseContent.match(
				/<(?:c|cal):calendar-data[^>]*>([\s\S]*?)<\/(?:c|cal):calendar-data>/i,
			);

			if (calDataMatch) {
				let icsContent = calDataMatch[1];

				// Decode XML entities
				icsContent = icsContent
					.replace(/&lt;/g, "<")
					.replace(/&gt;/g, ">")
					.replace(/&amp;/g, "&")
					.replace(/&quot;/g, '"')
					.replace(/&apos;/g, "'")
					.trim();

				if (icsContent.startsWith("BEGIN:VCALENDAR")) {
					results.push({
						href,
						etag,
						icsContent,
					});
				}
			}
		}

		return results;
	}

	/**
	 * Normalize Apple calendar color format
	 * Apple uses #RRGGBBAA format, we convert to standard #RRGGBB
	 */
	private normalizeAppleColor(color?: string): string | undefined {
		if (!color) return undefined;

		// Remove any whitespace
		color = color.trim();

		// If it's 9 characters (#RRGGBBAA), strip the alpha
		if (color.length === 9 && color.startsWith("#")) {
			return color.substring(0, 7);
		}

		return color;
	}

	/**
	 * Extract calendar name from href path
	 */
	private extractCalendarNameFromHref(href: string): string {
		const parts = href.split("/").filter(Boolean);
		return parts[parts.length - 1] || "Calendar";
	}
}
