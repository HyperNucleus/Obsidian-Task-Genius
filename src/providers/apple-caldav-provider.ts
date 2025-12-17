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
 * CalDAV XML namespaces
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
				this.buildPropfindBody(["d:current-user-principal"])
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
				"auth"
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
			this.buildPropfindBody(["d:current-user-principal"])
		);

		const principalHref = this.extractHref(
			principalResponse,
			"current-user-principal"
		);

		if (!principalHref) {
			throw new ProviderError(
				"Could not discover user principal",
				"not_found"
			);
		}

		const principalUrl = new URL(
			principalHref,
			this.config.serverUrl
		).toString();

		// Then, get the calendar home set from the principal
		const homeSetResponse = await this.makePropfindRequest(
			principalUrl,
			0,
			this.buildPropfindBody(["c:calendar-home-set"])
		);

		const homeSetHref = this.extractHref(
			homeSetResponse,
			"calendar-home-set"
		);

		if (!homeSetHref) {
			throw new ProviderError(
				"Could not discover calendar home set",
				"not_found"
			);
		}

		return new URL(homeSetHref, this.config.serverUrl).toString();
	}

	/**
	 * List calendars in the calendar home set
	 */
	private async listCalendarsInHomeSet(
		homeSetUrl: string
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
			])
		);

		const calendars: CalendarListEntry[] = [];

		// Parse multi-status response
		const responses = this.parseMultiStatusResponses(response);

		for (const resp of responses) {
			// Check if this is a calendar collection
			if (!resp.isCalendar) continue;

			calendars.push({
				id: resp.href,
				name: resp.displayName || this.extractCalendarNameFromHref(resp.href),
				color: resp.color,
				primary: false, // CalDAV doesn't have a concept of primary calendar
				description: resp.description,
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
			const calendarHrefs =
				options.calendarIds?.length
					? options.calendarIds
					: this.config.calendarHrefs;

			if (calendarHrefs.length === 0) {
				console.warn(
					"[AppleCaldavProvider] No calendars configured"
				);
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
						options
					);
					allEvents.push(...events);
				} catch (error) {
					console.error(
						`[AppleCaldavProvider] Error fetching ${calHref}:`,
						error
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
		options: FetchEventsOptions
	): Promise<IcsEvent[]> {
		const calendarUrl = new URL(
			calendarHref,
			this.config.serverUrl
		).toString();

		// Build calendar-query REPORT
		const startStr = formatDateForCaldav(options.range.start);
		const endStr = formatDateForCaldav(options.range.end);

		const reportBody = `<?xml version="1.0" encoding="utf-8"?>
<c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
	<d:prop>
		<d:getetag/>
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
				"auth"
			);
		}

		if (response.status >= 400) {
			throw new ProviderError(
				`CalDAV REPORT failed: ${response.status}`,
				"unknown"
			);
		}

		// Parse the multi-status response and extract ICS data
		const icsBlocks = this.extractCalendarData(response.text);
		const events: IcsEvent[] = [];

		for (const icsContent of icsBlocks) {
			try {
				// Use existing IcsParser to parse the ICS content
				const parsed = IcsParser.parse(icsContent, this.config as any);
				events.push(...parsed.events);
			} catch (error) {
				console.warn(
					"[AppleCaldavProvider] Failed to parse ICS block:",
					error
				);
			}
		}

		return events;
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
		body: string
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
				"auth"
			);
		}

		if (response.status >= 400 && response.status !== 207) {
			throw new ProviderError(
				`CalDAV PROPFIND failed: ${response.status}`,
				"unknown"
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
			"i"
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
				/<d:href[^>]*>([^<]+)<\/d:href>/i
			);
			if (!hrefMatch) continue;

			const href = hrefMatch[1].trim();

			// Check if it's a calendar (has calendar resourcetype)
			const isCalendar =
				/<c:calendar\s*\/>|<cal:calendar\s*\/>/i.test(responseContent);

			// Extract display name
			const displayNameMatch = responseContent.match(
				/<d:displayname[^>]*>([^<]*)<\/d:displayname>/i
			);

			// Extract calendar color (Apple specific)
			const colorMatch = responseContent.match(
				/<apple:calendar-color[^>]*>([^<]+)<\/apple:calendar-color>/i
			);

			// Extract description
			const descriptionMatch = responseContent.match(
				/<c:calendar-description[^>]*>([^<]*)<\/c:calendar-description>/i
			);

			// Extract ctag
			const ctagMatch = responseContent.match(
				/<cs:getctag[^>]*>([^<]+)<\/cs:getctag>/i
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
	 * Extract calendar-data (ICS content) from REPORT response
	 */
	private extractCalendarData(xml: string): string[] {
		const results: string[] = [];

		// Match c:calendar-data or cal:calendar-data elements
		const regex =
			/<(?:c|cal):calendar-data[^>]*>([\s\S]*?)<\/(?:c|cal):calendar-data>/gi;
		let match;

		while ((match = regex.exec(xml)) !== null) {
			let icsContent = match[1];

			// Decode XML entities
			icsContent = icsContent
				.replace(/&lt;/g, "<")
				.replace(/&gt;/g, ">")
				.replace(/&amp;/g, "&")
				.replace(/&quot;/g, '"')
				.replace(/&apos;/g, "'");

			// Trim whitespace but preserve the ICS structure
			icsContent = icsContent.trim();

			if (icsContent.startsWith("BEGIN:VCALENDAR")) {
				results.push(icsContent);
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
