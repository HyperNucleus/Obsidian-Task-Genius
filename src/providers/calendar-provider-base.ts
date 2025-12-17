/**
 * Calendar Provider Base Class
 *
 * Abstract base class defining the common interface and behaviors for all
 * calendar providers (Google, Outlook, Apple CalDAV, URL ICS).
 *
 * Design principles:
 * - Unified interface for different calendar backends
 * - Built-in status management and error handling
 * - Event conversion to IcsEvent format
 * - Lifecycle management via Obsidian's Component
 *
 * @module calendar-provider-base
 */

import { Component, Notice } from "obsidian";
import { CalendarSource } from "../types/calendar-provider";
import { IcsEvent, IcsSyncStatus } from "../types/ics";
import { t } from "../translations/helper";

// ============================================================================
// Provider Types
// ============================================================================

/**
 * Represents a calendar entry from a provider's calendar list
 */
export interface CalendarListEntry {
	/** Unique identifier for this calendar */
	id: string;
	/** Display name */
	name: string;
	/** Calendar color (hex or provider-specific) */
	color?: string;
	/** Whether this is the primary/default calendar */
	primary: boolean;
	/** Whether user can write to this calendar */
	canWrite?: boolean;
	/** Calendar description */
	description?: string;
	/** Timezone of the calendar */
	timeZone?: string;
}

/**
 * Date range for event queries
 */
export interface DateRange {
	/** Start of the range (inclusive) */
	start: Date;
	/** End of the range (exclusive) */
	end: Date;
}

/**
 * Options for fetching events
 */
export interface FetchEventsOptions {
	/** Date range for events */
	range: DateRange;
	/** Specific calendar IDs to fetch (empty = all configured) */
	calendarIds?: string[];
	/** Maximum number of events to return */
	maxResults?: number;
	/** Whether to expand recurring events */
	expandRecurring?: boolean;
	/** Abort signal for cancellation */
	signal?: AbortSignal;
}

/**
 * Result of a write operation (create/update/delete)
 */
export interface WriteResult {
	success: boolean;
	/** The updated/created event (undefined for delete) */
	event?: IcsEvent;
	/** Error message if failed */
	error?: string;
	/** Whether a conflict was detected (remote was modified) */
	conflict?: boolean;
}

/**
 * Options for updating an event
 */
export interface UpdateEventOptions {
	/** The event with updated fields */
	event: IcsEvent;
	/** The original event (for ETag/conflict detection) */
	originalEvent?: IcsEvent;
	/** Specific calendar ID (if not embedded in event) */
	calendarId?: string;
}

/**
 * Result of a fetch operation
 */
export interface FetchResult<T> {
	success: boolean;
	data?: T;
	error?: Error;
	/** Whether the result came from cache */
	fromCache?: boolean;
}

/**
 * Callback type for status changes
 */
export type StatusChangeCallback = (status: IcsSyncStatus) => void;

// ============================================================================
// Error Types
// ============================================================================

/**
 * Categorized provider errors
 */
export type ProviderErrorType =
	| "auth" // Authentication/authorization failed
	| "network" // Network connectivity issue
	| "rate_limit" // API rate limit exceeded
	| "not_found" // Resource not found
	| "permission" // Permission denied
	| "parse" // Response parsing failed
	| "timeout" // Request timed out
	| "unknown"; // Unknown error

/**
 * Enhanced error with categorization
 */
export class ProviderError extends Error {
	constructor(
		message: string,
		public readonly type: ProviderErrorType,
		public readonly originalError?: Error,
		public readonly retryable: boolean = false,
	) {
		super(message);
		this.name = "ProviderError";
	}

	/**
	 * Create a ProviderError from an unknown error
	 */
	static from(error: unknown, context: string): ProviderError {
		if (error instanceof ProviderError) {
			return error;
		}

		const message = error instanceof Error ? error.message : String(error);
		const originalError = error instanceof Error ? error : undefined;

		// Categorize based on error message patterns
		let type: ProviderErrorType = "unknown";
		let retryable = false;

		const lowerMessage = message.toLowerCase();

		if (
			lowerMessage.includes("401") ||
			lowerMessage.includes("unauthorized") ||
			lowerMessage.includes("authentication") ||
			lowerMessage.includes("invalid_grant")
		) {
			type = "auth";
		} else if (
			lowerMessage.includes("403") ||
			lowerMessage.includes("forbidden") ||
			lowerMessage.includes("permission")
		) {
			type = "permission";
		} else if (
			lowerMessage.includes("404") ||
			lowerMessage.includes("not found")
		) {
			type = "not_found";
		} else if (
			lowerMessage.includes("429") ||
			lowerMessage.includes("rate limit") ||
			lowerMessage.includes("too many requests")
		) {
			type = "rate_limit";
			retryable = true;
		} else if (
			lowerMessage.includes("timeout") ||
			lowerMessage.includes("timed out")
		) {
			type = "timeout";
			retryable = true;
		} else if (
			lowerMessage.includes("network") ||
			lowerMessage.includes("connection") ||
			lowerMessage.includes("econnrefused") ||
			lowerMessage.includes("fetch failed")
		) {
			type = "network";
			retryable = true;
		} else if (
			lowerMessage.includes("parse") ||
			lowerMessage.includes("json") ||
			lowerMessage.includes("xml")
		) {
			type = "parse";
		}

		return new ProviderError(
			`${context}: ${message}`,
			type,
			originalError,
			retryable,
		);
	}
}

// ============================================================================
// Base Provider Class
// ============================================================================

/**
 * Abstract base class for calendar providers
 *
 * @template T - The specific CalendarSource configuration type
 */
export abstract class CalendarProviderBase<
	T extends CalendarSource,
> extends Component {
	/** Current configuration */
	protected config: T;

	/** Status change callback */
	protected onStatusChange?: StatusChangeCallback;

	/** Current sync status */
	protected currentStatus: IcsSyncStatus;

	/** Whether provider is currently syncing */
	protected isSyncing = false;

	constructor(config: T) {
		super();
		this.config = config;
		this.currentStatus = {
			sourceId: config.id,
			status: "idle",
		};
	}

	// =========================================================================
	// Abstract Methods (must be implemented by subclasses)
	// =========================================================================

	/**
	 * Establish connection and validate credentials
	 * @returns true if connection is successful and credentials are valid
	 */
	abstract connect(): Promise<boolean>;

	/**
	 * Disconnect and clean up resources
	 */
	abstract disconnect(): Promise<void>;

	/**
	 * Fetch events within the specified options
	 */
	abstract getEvents(options: FetchEventsOptions): Promise<IcsEvent[]>;

	/**
	 * List available calendars from the provider
	 */
	abstract listCalendars(): Promise<CalendarListEntry[]>;

	// =========================================================================
	// Write Methods (Optional - for providers supporting two-way sync)
	// =========================================================================

	/**
	 * Check if this provider supports write operations
	 * Override in subclasses that support writing
	 */
	supportsWrite(): boolean {
		return false;
	}

	/**
	 * Check if a specific calendar can be written to
	 * @param calendarId - The calendar ID to check
	 */
	canWriteToCalendar(calendarId?: string): boolean {
		return false;
	}

	/**
	 * Create a new event
	 * @param event - The event to create
	 * @param calendarId - The calendar to create the event in
	 * @returns WriteResult with the created event
	 */
	async createEvent(
		event: IcsEvent,
		calendarId?: string,
	): Promise<WriteResult> {
		return {
			success: false,
			error: "Write operations not supported by this provider",
		};
	}

	/**
	 * Update an existing event
	 * Uses PATCH semantics - only modified fields are sent
	 * @param options - Update options including the event and original for conflict detection
	 * @returns WriteResult with the updated event
	 */
	async updateEvent(options: UpdateEventOptions): Promise<WriteResult> {
		return {
			success: false,
			error: "Write operations not supported by this provider",
		};
	}

	/**
	 * Delete an event
	 * @param eventId - The event ID to delete
	 * @param calendarId - The calendar containing the event
	 * @param etag - Optional ETag for conflict detection
	 * @returns WriteResult indicating success/failure
	 */
	async deleteEvent(
		eventId: string,
		calendarId?: string,
		etag?: string,
	): Promise<WriteResult> {
		return {
			success: false,
			error: "Write operations not supported by this provider",
		};
	}

	// =========================================================================
	// Configuration
	// =========================================================================

	/**
	 * Get current configuration
	 */
	getConfig(): T {
		return this.config;
	}

	/**
	 * Update provider configuration
	 */
	updateConfig(newConfig: T): void {
		this.config = newConfig;
		this.currentStatus.sourceId = newConfig.id;
	}

	/**
	 * Get provider display name
	 */
	getDisplayName(): string {
		return this.config.name;
	}

	/**
	 * Get provider type
	 */
	getProviderType(): T["type"] {
		return this.config.type;
	}

	// =========================================================================
	// Status Management
	// =========================================================================

	/**
	 * Set status change listener
	 */
	setStatusListener(callback: StatusChangeCallback): void {
		this.onStatusChange = callback;
	}

	/**
	 * Get current sync status
	 */
	getStatus(): IcsSyncStatus {
		return { ...this.currentStatus };
	}

	/**
	 * Update and notify status
	 */
	protected updateStatus(
		updates: Partial<Omit<IcsSyncStatus, "sourceId">>,
	): void {
		this.currentStatus = {
			...this.currentStatus,
			...updates,
		};

		if (this.onStatusChange) {
			this.onStatusChange(this.currentStatus);
		}
	}

	/**
	 * Set syncing state
	 */
	protected setSyncing(syncing: boolean): void {
		this.isSyncing = syncing;
		this.updateStatus({
			status: syncing ? "syncing" : "idle",
		});
	}

	/**
	 * Check if currently syncing
	 */
	isBusy(): boolean {
		return this.isSyncing;
	}

	// =========================================================================
	// Error Handling
	// =========================================================================

	/**
	 * Handle and report an error
	 */
	protected handleError(error: unknown, context: string): ProviderError {
		const providerError = ProviderError.from(error, context);

		console.error(
			`[${this.config.type}:${this.config.name}] ${providerError.message}`,
			providerError.originalError,
		);

		this.updateStatus({
			status: "error",
			error: providerError.message,
			lastSync: Date.now(),
		});

		// Show user-friendly notice
		this.showErrorNotice(providerError);

		return providerError;
	}

	/**
	 * Show user-friendly error notification
	 */
	protected showErrorNotice(error: ProviderError): void {
		let message: string;

		switch (error.type) {
			case "auth":
				message = t(
					"Authentication failed. Please reconnect your calendar.",
				);
				break;
			case "network":
				message = t(
					"Network error. Please check your internet connection.",
				);
				break;
			case "rate_limit":
				message = t("Too many requests. Please try again later.");
				break;
			case "permission":
				message = t(
					"Permission denied. Please check calendar permissions.",
				);
				break;
			case "timeout":
				message = t("Request timed out. Please try again.");
				break;
			default:
				message = `${this.config.name}: ${error.message}`;
		}

		new Notice(message);
	}

	// =========================================================================
	// Lifecycle
	// =========================================================================

	/**
	 * Clean up on unload
	 */
	override onunload(): void {
		this.onStatusChange = undefined;
		super.onunload();
	}
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Create a default date range (current month)
 */
export function createDefaultDateRange(): DateRange {
	const now = new Date();
	const start = new Date(now.getFullYear(), now.getMonth(), 1);
	const end = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
	return { start, end };
}

/**
 * Create a date range for a specific number of days around today
 */
export function createDateRangeAroundToday(
	daysBefore: number,
	daysAfter: number,
): DateRange {
	const now = new Date();
	now.setHours(0, 0, 0, 0);

	const start = new Date(now);
	start.setDate(start.getDate() - daysBefore);

	const end = new Date(now);
	end.setDate(end.getDate() + daysAfter);
	end.setHours(23, 59, 59, 999);

	return { start, end };
}

/**
 * Format date for API requests (ISO 8601)
 */
export function formatDateForApi(date: Date): string {
	return date.toISOString();
}

/**
 * Format date for CalDAV requests (compact format: 20231225T120000Z)
 */
export function formatDateForCaldav(date: Date): string {
	return date.toISOString().replace(/[-:]/g, "").split(".")[0] + "Z";
}
