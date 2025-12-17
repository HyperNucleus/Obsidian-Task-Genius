/**
 * Calendar Provider Architecture Types
 *
 * This module defines the unified type system for multiple calendar sources,
 * including traditional ICS/iCal URLs and OAuth-based providers (Google, Outlook, Apple).
 *
 * Key design principles:
 * - Discriminated unions for type-safe provider handling
 * - Separation of configuration and authentication data
 * - Backward compatibility with existing IcsSource
 * - Secure token storage patterns
 *
 * @module calendar-provider
 */

import {
	IcsAuthConfig,
	IcsEventFilter,
	IcsHolidayConfig,
	IcsStatusMapping,
	IcsTextReplacement,
} from "./ics";

// ============================================================================
// Provider Type Enumeration
// ============================================================================

/**
 * Supported calendar provider types
 * - `url-ics`: Traditional ICS/iCal URL (webcal://, http://, https://)
 * - `google`: Google Calendar via OAuth 2.0 + PKCE
 * - `outlook`: Microsoft Outlook/365 via Microsoft Graph API + OAuth 2.0
 * - `apple-caldav`: Apple iCloud Calendar via CalDAV + App-Specific Password
 */
export type CalendarProviderType =
	| "url-ics"
	| "google"
	| "outlook"
	| "apple-caldav";

/**
 * Provider display metadata for UI rendering
 */
export const CalendarProviderMeta: Record<
	CalendarProviderType,
	{
		displayName: string;
		icon: string;
		description: string;
		authType: "none" | "basic" | "oauth2" | "caldav";
	}
> = {
	"url-ics": {
		displayName: "ICS/iCal URL",
		icon: "calendar",
		description: "Import from any ICS/iCal URL (webcal://, https://)",
		authType: "basic",
	},
	google: {
		displayName: "Google Calendar",
		icon: "mail", // Lucide doesn't have Google icon, use similar
		description: "Connect to Google Calendar via OAuth 2.0",
		authType: "oauth2",
	},
	outlook: {
		displayName: "Outlook / Microsoft 365",
		icon: "cloud",
		description: "Connect to Outlook Calendar via Microsoft Graph API",
		authType: "oauth2",
	},
	"apple-caldav": {
		displayName: "Apple iCloud Calendar",
		icon: "apple",
		description: "Connect to iCloud Calendar via CalDAV",
		authType: "caldav",
	},
};

// ============================================================================
// OAuth 2.0 Token Types
// ============================================================================

/**
 * OAuth 2.0 token data structure
 *
 * SECURITY NOTE: This data is stored in Obsidian's data.json file,
 * which is NOT encrypted. Users should be warned not to share their
 * data.json if OAuth connections are configured.
 */
export interface OAuthTokenData {
	/** The access token for API requests */
	accessToken: string;

	/** The refresh token for obtaining new access tokens */
	refreshToken: string;

	/** Unix timestamp (ms) when the access token expires */
	expiresAt: number;

	/** OAuth scope granted by the user */
	scope: string;

	/** Token type (typically "Bearer") */
	tokenType: string;

	/** Unix timestamp (ms) when the tokens were issued */
	issuedAt: number;
}

/**
 * OAuth connection state for UI display
 */
export type OAuthConnectionState =
	| "disconnected"
	| "connecting"
	| "connected"
	| "expired"
	| "error";

/**
 * OAuth provider-specific endpoints configuration
 */
export interface OAuthEndpoints {
	authorizationUrl: string;
	tokenUrl: string;
	revokeUrl?: string;
	userInfoUrl?: string;
}

/**
 * PKCE (Proof Key for Code Exchange) data
 * Used during OAuth flow for public clients
 */
export interface PKCEData {
	codeVerifier: string;
	codeChallenge: string;
	state: string;
}

// ============================================================================
// Base Calendar Source Configuration
// ============================================================================

/**
 * Base configuration shared by all calendar source types
 */
export interface BaseCalendarSource {
	/** Unique identifier for this calendar source */
	id: string;

	/** User-defined display name */
	name: string;

	/** Provider type discriminator */
	type: CalendarProviderType;

	/** Whether this source is enabled for syncing */
	enabled: boolean;

	/** Custom color for events from this source (hex format) */
	color?: string;

	/** Sync interval in minutes (default: 60) */
	refreshInterval: number;

	/** Last successful sync timestamp (ms) */
	lastSynced?: number;

	/** How to display events: as badges on dates or full events */
	showType: "badge" | "event";

	/** Include all-day events */
	showAllDayEvents: boolean;

	/** Include timed events */
	showTimedEvents: boolean;

	/** Task status mapping rules */
	statusMapping?: IcsStatusMapping;

	/** Text replacement rules for event titles/descriptions */
	textReplacements?: IcsTextReplacement[];

	/** Holiday detection configuration */
	holidayConfig?: IcsHolidayConfig;

	/**
	 * Creation timestamp
	 * @default Date.now() - For backward compatibility with legacy IcsSource
	 */
	createdAt?: number;

	/**
	 * Last modification timestamp
	 * @default Date.now() - For backward compatibility with legacy IcsSource
	 */
	updatedAt?: number;
}

// ============================================================================
// Provider-Specific Configurations
// ============================================================================

/**
 * Configuration for traditional ICS/iCal URL sources
 * This maintains backward compatibility with the existing IcsSource interface
 */
export interface UrlIcsSourceConfig extends BaseCalendarSource {
	type: "url-ics";

	/** URL to the ICS file (webcal://, http://, https://) */
	url: string;

	/** Legacy authentication (Basic/Bearer/Custom Headers) */
	auth?: IcsAuthConfig;

	/** Event filtering rules */
	filters?: IcsEventFilter;
}

/**
 * Configuration for Google Calendar
 * Uses OAuth 2.0 with PKCE for authentication
 */
export interface GoogleCalendarSourceConfig extends BaseCalendarSource {
	type: "google";

	/** Connected Google account email (for display purposes) */
	accountEmail?: string;

	/** List of specific calendar IDs to sync (empty = all calendars) */
	calendarIds: string[];

	/**
	 * User-friendly labels for selected calendars
	 * Map: calendarId -> displayName
	 */
	calendarLabels?: Record<string, string>;

	/** OAuth tokens - managed by AuthManager */
	auth?: OAuthTokenData;

	/** Whether to include primary calendar automatically */
	includePrimaryCalendar: boolean;

	/** Whether to include shared calendars */
	includeSharedCalendars: boolean;
}

/**
 * Configuration for Outlook/Microsoft 365 Calendar
 * Uses Microsoft Graph API with OAuth 2.0 + PKCE
 */
export interface OutlookCalendarSourceConfig extends BaseCalendarSource {
	type: "outlook";

	/** Connected Microsoft account email (for display purposes) */
	accountEmail?: string;

	/** List of specific calendar IDs to sync (empty = all calendars) */
	calendarIds: string[];

	/**
	 * User-friendly labels for selected calendars
	 * Map: calendarId -> displayName
	 */
	calendarLabels?: Record<string, string>;

	/** OAuth tokens - managed by AuthManager */
	auth?: OAuthTokenData;

	/**
	 * Azure AD tenant ID
	 * - "common" for multi-tenant (personal + work accounts)
	 * - "consumers" for personal accounts only
	 * - "organizations" for work/school accounts only
	 * - Specific tenant GUID for enterprise scenarios
	 */
	tenantId: string;

	/** Whether to include primary calendar automatically */
	includePrimaryCalendar: boolean;

	/** Whether to include shared calendars */
	includeSharedCalendars: boolean;
}

/**
 * Configuration for Apple iCloud Calendar via CalDAV
 * Uses App-Specific Password for authentication
 */
export interface AppleCaldavSourceConfig extends BaseCalendarSource {
	type: "apple-caldav";

	/** CalDAV server URL (default: https://caldav.icloud.com/) */
	serverUrl: string;

	/** Apple ID (email) */
	username: string;

	/**
	 * App-Specific Password generated from Apple ID settings
	 *
	 * SECURITY NOTE: This must be an App-Specific Password,
	 * NOT the Apple ID password. Users should generate this at:
	 * https://appleid.apple.com/account/manage
	 */
	appSpecificPassword?: string;

	/** List of calendar collection hrefs to sync */
	calendarHrefs: string[];

	/**
	 * User-friendly labels for selected calendars
	 * Map: calendarHref -> displayName
	 */
	calendarLabels?: Record<string, string>;

	/** Principal URL for CalDAV discovery */
	principalUrl?: string;
}

// ============================================================================
// Discriminated Union Type
// ============================================================================

/**
 * Discriminated union of all calendar source configurations
 * Use type guards below for type-safe handling
 */
export type CalendarSource =
	| UrlIcsSourceConfig
	| GoogleCalendarSourceConfig
	| OutlookCalendarSourceConfig
	| AppleCaldavSourceConfig;

// ============================================================================
// Type Guards
// ============================================================================

/**
 * Type guard: Check if source is a URL ICS source
 */
export function isUrlIcsSource(
	source: CalendarSource,
): source is UrlIcsSourceConfig {
	return source.type === "url-ics";
}

/**
 * Type guard: Check if source is a Google Calendar source
 */
export function isGoogleSource(
	source: CalendarSource,
): source is GoogleCalendarSourceConfig {
	return source.type === "google";
}

/**
 * Type guard: Check if source is an Outlook Calendar source
 */
export function isOutlookSource(
	source: CalendarSource,
): source is OutlookCalendarSourceConfig {
	return source.type === "outlook";
}

/**
 * Type guard: Check if source is an Apple CalDAV source
 */
export function isAppleSource(
	source: CalendarSource,
): source is AppleCaldavSourceConfig {
	return source.type === "apple-caldav";
}

/**
 * Type guard: Check if source uses OAuth authentication
 */
export function isOAuthSource(
	source: CalendarSource,
): source is GoogleCalendarSourceConfig | OutlookCalendarSourceConfig {
	return source.type === "google" || source.type === "outlook";
}

/**
 * Type guard: Check if source has valid OAuth tokens
 */
export function hasValidTokens(source: CalendarSource): source is (
	| GoogleCalendarSourceConfig
	| OutlookCalendarSourceConfig
) & {
	auth: OAuthTokenData;
} {
	if (!isOAuthSource(source) || !source.auth) {
		return false;
	}
	// Token is valid if it exists and hasn't expired
	// (with 5 minute buffer for clock skew)
	const bufferMs = 5 * 60 * 1000;
	return source.auth.expiresAt > Date.now() + bufferMs;
}

// ============================================================================
// Configuration Manager Types
// ============================================================================

/**
 * Calendar integration settings (stored in plugin settings)
 */
export interface CalendarIntegrationConfig {
	/** All configured calendar sources */
	sources: CalendarSource[];

	/** Global sync interval in minutes (default for new sources) */
	globalRefreshInterval: number;

	/** Maximum cache age in hours */
	maxCacheAge: number;

	/** Enable automatic background sync */
	enableBackgroundRefresh: boolean;

	/** Network request timeout in seconds */
	networkTimeout: number;

	/** Maximum events to load per source */
	maxEventsPerSource: number;

	/** Show calendar events in calendar views */
	showInCalendar: boolean;

	/** Show calendar events in task lists */
	showInTaskLists: boolean;

	/** Default color for events without custom color */
	defaultEventColor: string;
}

/**
 * Default configuration for CalendarIntegrationConfig
 */
export const DEFAULT_CALENDAR_INTEGRATION_CONFIG: CalendarIntegrationConfig = {
	sources: [],
	globalRefreshInterval: 60,
	maxCacheAge: 24,
	enableBackgroundRefresh: true,
	networkTimeout: 30,
	maxEventsPerSource: 1000,
	showInCalendar: true,
	showInTaskLists: true,
	defaultEventColor: "#3b82f6",
};

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new URL ICS source with default values
 */
export function createUrlIcsSource(
	partial: Partial<UrlIcsSourceConfig> & { name: string; url: string },
): UrlIcsSourceConfig {
	const now = Date.now();
	return {
		id: `ics-${now}-${Math.random().toString(36).substring(2, 11)}`,
		type: "url-ics",
		enabled: true,
		refreshInterval: 60,
		showType: "event",
		showAllDayEvents: true,
		showTimedEvents: true,
		createdAt: now,
		updatedAt: now,
		...partial,
	};
}

/**
 * Create a new Google Calendar source with default values
 */
export function createGoogleSource(
	partial: Partial<GoogleCalendarSourceConfig> & { name: string },
): GoogleCalendarSourceConfig {
	const now = Date.now();
	return {
		id: `google-${now}-${Math.random().toString(36).substring(2, 11)}`,
		type: "google",
		enabled: true,
		refreshInterval: 60,
		showType: "event",
		showAllDayEvents: true,
		showTimedEvents: true,
		calendarIds: [],
		includePrimaryCalendar: true,
		includeSharedCalendars: false,
		createdAt: now,
		updatedAt: now,
		...partial,
	};
}

/**
 * Create a new Outlook Calendar source with default values
 */
export function createOutlookSource(
	partial: Partial<OutlookCalendarSourceConfig> & { name: string },
): OutlookCalendarSourceConfig {
	const now = Date.now();
	return {
		id: `outlook-${now}-${Math.random().toString(36).substring(2, 11)}`,
		type: "outlook",
		enabled: true,
		refreshInterval: 60,
		showType: "event",
		showAllDayEvents: true,
		showTimedEvents: true,
		calendarIds: [],
		tenantId: "common",
		includePrimaryCalendar: true,
		includeSharedCalendars: false,
		createdAt: now,
		updatedAt: now,
		...partial,
	};
}

/**
 * Create a new Apple CalDAV source with default values
 */
export function createAppleSource(
	partial: Partial<AppleCaldavSourceConfig> & {
		name: string;
		username: string;
	},
): AppleCaldavSourceConfig {
	const now = Date.now();
	return {
		id: `apple-${now}-${Math.random().toString(36).substring(2, 11)}`,
		type: "apple-caldav",
		enabled: true,
		refreshInterval: 60,
		showType: "event",
		showAllDayEvents: true,
		showTimedEvents: true,
		serverUrl: "https://caldav.icloud.com/",
		calendarHrefs: [],
		createdAt: now,
		updatedAt: now,
		...partial,
	};
}

// ============================================================================
// Migration Utilities
// ============================================================================

/**
 * Legacy IcsSource type for backward compatibility
 * This represents the old format before multi-provider support
 */
export interface LegacyIcsSource {
	id: string;
	name: string;
	url: string;
	enabled: boolean;
	color?: string;
	refreshInterval: number;
	showType: "badge" | "event";
	showAllDayEvents: boolean;
	showTimedEvents: boolean;
	lastFetched?: number;
	auth?: IcsAuthConfig;
	filters?: IcsEventFilter;
	holidayConfig?: IcsHolidayConfig;
	statusMapping?: IcsStatusMapping;
	textReplacements?: IcsTextReplacement[];
	type?: CalendarProviderType;
}

/**
 * Union type that accepts both legacy and new calendar sources
 * Used for reading configuration that may contain old formats
 */
export type AnyCalendarSource = CalendarSource | LegacyIcsSource;

/**
 * Type guard: Check if source is a legacy IcsSource (lacks required type field)
 */
export function isLegacySource(
	source: AnyCalendarSource,
): source is LegacyIcsSource {
	return (
		!source.type ||
		(source.type === "url-ics" &&
			"url" in source &&
			!("createdAt" in source))
	);
}

/**
 * Normalize any calendar source to ensure it has all required fields
 * This handles both legacy IcsSource and new CalendarSource formats
 */
export function normalizeCalendarSource(
	source: AnyCalendarSource,
): CalendarSource {
	// If already a proper CalendarSource with type, return as-is (with defaults for optional fields)
	if (source.type && !isLegacySource(source)) {
		return source as CalendarSource;
	}

	// Migrate legacy source to UrlIcsSourceConfig
	const now = Date.now();
	const legacySource = source as LegacyIcsSource;
	return {
		...legacySource,
		type: "url-ics",
		createdAt: legacySource.lastFetched || now,
		updatedAt: now,
		lastSynced: legacySource.lastFetched,
	} as UrlIcsSourceConfig;
}

/**
 * Normalize an array of sources (handles mixed legacy and new formats)
 */
export function normalizeCalendarSources(
	sources: AnyCalendarSource[],
): CalendarSource[] {
	return sources.map(normalizeCalendarSource);
}

/**
 * Migrate legacy IcsSource to CalendarSource (UrlIcsSourceConfig)
 * This ensures backward compatibility with existing configurations
 * @deprecated Use normalizeCalendarSource instead
 */
export function migrateIcsSourceToCalendarSource(
	legacySource: LegacyIcsSource,
): UrlIcsSourceConfig {
	const now = Date.now();
	return {
		...legacySource,
		type: "url-ics",
		createdAt: legacySource.lastFetched || now,
		updatedAt: now,
		lastSynced: legacySource.lastFetched,
	};
}
