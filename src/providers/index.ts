/**
 * Calendar Provider Factory and Exports
 *
 * This module provides a unified factory for creating calendar providers
 * and exports all provider-related types and classes.
 *
 * @module providers
 */

import {
	CalendarSource,
	isGoogleSource,
	isOutlookSource,
	isAppleSource,
	isUrlIcsSource,
} from "../types/calendar-provider";
import { CalendarProviderBase } from "./calendar-provider-base";
import { GoogleCalendarProvider } from "./google-calendar-provider";
import { OutlookCalendarProvider } from "./outlook-calendar-provider";
import { AppleCaldavProvider } from "./apple-caldav-provider";
import { CalendarAuthManager } from "../managers/calendar-auth-manager";

// ============================================================================
// Re-exports
// ============================================================================

export {
	CalendarProviderBase,
	createDefaultDateRange,
	createDateRangeAroundToday,
	formatDateForApi,
	formatDateForCaldav,
	ProviderError,
} from "./calendar-provider-base";

export type {
	CalendarListEntry,
	DateRange,
	FetchEventsOptions,
	FetchResult,
	StatusChangeCallback,
	ProviderErrorType,
	WriteResult,
	UpdateEventOptions,
} from "./calendar-provider-base";

export { GoogleCalendarProvider } from "./google-calendar-provider";
export { OutlookCalendarProvider } from "./outlook-calendar-provider";
export { AppleCaldavProvider } from "./apple-caldav-provider";

// ============================================================================
// Provider Factory
// ============================================================================

/**
 * Factory options for creating providers
 */
export interface ProviderFactoryOptions {
	/** Authentication manager for OAuth providers */
	authManager: CalendarAuthManager;
}

/**
 * Factory for creating calendar provider instances
 */
export class ProviderFactory {
	private authManager: CalendarAuthManager;

	constructor(options: ProviderFactoryOptions) {
		this.authManager = options.authManager;
	}

	/**
	 * Create a provider instance based on the source configuration
	 *
	 * @param source - The calendar source configuration
	 * @returns The appropriate provider instance
	 * @throws Error if the source type is unknown
	 */
	createProvider(
		source: CalendarSource,
	): CalendarProviderBase<CalendarSource> {
		if (isGoogleSource(source)) {
			return new GoogleCalendarProvider(source, this.authManager);
		}

		if (isOutlookSource(source)) {
			return new OutlookCalendarProvider(source, this.authManager);
		}

		if (isAppleSource(source)) {
			return new AppleCaldavProvider(source);
		}

		if (isUrlIcsSource(source)) {
			// For URL ICS sources, we delegate to the existing IcsManager
			// This is handled separately in CalendarSourceManager
			throw new Error(
				"URL ICS sources should be handled by IcsManager directly",
			);
		}

		throw new Error(
			`Unknown calendar provider type: ${(source as any).type}`,
		);
	}

	/**
	 * Check if a provider type requires OAuth authentication
	 */
	static requiresOAuth(type: CalendarSource["type"]): boolean {
		return type === "google" || type === "outlook";
	}

	/**
	 * Check if a provider type requires manual credentials
	 */
	static requiresCredentials(type: CalendarSource["type"]): boolean {
		return type === "apple-caldav" || type === "url-ics";
	}

	/**
	 * Get the display name for a provider type
	 */
	static getProviderDisplayName(type: CalendarSource["type"]): string {
		switch (type) {
			case "google":
				return "Google Calendar";
			case "outlook":
				return "Outlook / Microsoft 365";
			case "apple-caldav":
				return "Apple iCloud Calendar";
			case "url-ics":
				return "ICS/iCal URL";
			default:
				return "Unknown";
		}
	}

	/**
	 * Get the icon name for a provider type (Lucide icons)
	 */
	static getProviderIcon(type: CalendarSource["type"]): string {
		switch (type) {
			case "google":
				return "mail"; // Google uses Gmail colors typically
			case "outlook":
				return "cloud";
			case "apple-caldav":
				return "apple";
			case "url-ics":
				return "calendar";
			default:
				return "calendar";
		}
	}
}

// ============================================================================
// Provider Manager
// ============================================================================

/**
 * Manages multiple calendar provider instances
 */
export class CalendarSourceManager {
	private factory: ProviderFactory;
	private providers: Map<string, CalendarProviderBase<CalendarSource>> =
		new Map();
	private authManager: CalendarAuthManager;

	constructor(authManager: CalendarAuthManager) {
		this.authManager = authManager;
		this.factory = new ProviderFactory({ authManager });
	}

	/**
	 * Get or create a provider for the given source
	 */
	getProvider(source: CalendarSource): CalendarProviderBase<CalendarSource> {
		// Check if we already have a provider for this source
		let provider = this.providers.get(source.id);

		if (provider) {
			// Update the provider's config in case it changed
			provider.updateConfig(source);
			return provider;
		}

		// Create a new provider
		provider = this.factory.createProvider(source);
		this.providers.set(source.id, provider);

		return provider;
	}

	/**
	 * Remove a provider
	 */
	async removeProvider(sourceId: string): Promise<void> {
		const provider = this.providers.get(sourceId);
		if (provider) {
			await provider.disconnect();
			provider.unload();
			this.providers.delete(sourceId);
		}
	}

	/**
	 * Get all active providers
	 */
	getAllProviders(): CalendarProviderBase<CalendarSource>[] {
		return Array.from(this.providers.values());
	}

	/**
	 * Disconnect and clean up all providers
	 */
	async disconnectAll(): Promise<void> {
		const disconnectPromises = Array.from(this.providers.values()).map(
			async (provider) => {
				try {
					await provider.disconnect();
					provider.unload();
				} catch (error) {
					console.error(
						`[CalendarSourceManager] Error disconnecting provider:`,
						error,
					);
				}
			},
		);

		await Promise.all(disconnectPromises);
		this.providers.clear();
	}

	/**
	 * Get the auth manager
	 */
	getAuthManager(): CalendarAuthManager {
		return this.authManager;
	}
}
