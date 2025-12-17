/**
 * Calendar Settings Component
 *
 * Unified settings interface for managing all calendar providers:
 * - URL ICS (traditional web calendars)
 * - Google Calendar (OAuth 2.0)
 * - Outlook/Microsoft 365 (OAuth 2.0)
 * - Apple iCloud (CalDAV)
 *
 * @module calendar-settings-tab
 */

import {
	App,
	Modal,
	Notice,
	Setting,
	setIcon,
	ButtonComponent,
	ExtraButtonComponent,
	TextComponent,
	ToggleComponent,
	DropdownComponent,
} from "obsidian";
import TaskProgressBarPlugin from "@/index";
import { t } from "@/translations/helper";
import {
	CalendarSource,
	CalendarProviderType,
	CalendarProviderMeta,
	isOAuthSource,
	isGoogleSource,
	isOutlookSource,
	isAppleSource,
	isUrlIcsSource,
	createUrlIcsSource,
	createGoogleSource,
	createOutlookSource,
	createAppleSource,
	GoogleCalendarSourceConfig,
	OutlookCalendarSourceConfig,
	AppleCaldavSourceConfig,
	UrlIcsSourceConfig,
	OAuthTokenData,
	normalizeCalendarSource,
	normalizeCalendarSources,
	AnyCalendarSource,
} from "@/types/calendar-provider";
import { IcsTextReplacement, IcsHolidayConfig } from "@/types/ics";
import { HolidayDetector } from "@/parsers/holiday-detector";
import {
	ProviderFactory,
	CalendarListEntry,
	CalendarSourceManager,
} from "@/providers";
import {
	CalendarAuthManager,
	AuthManagerEvent,
} from "@/managers/calendar-auth-manager";
import "@/styles/calendar-settings.scss";

// ============================================================================
// Calendar Settings Component
// ============================================================================

/**
 * Main settings component for calendar integration
 */
export class CalendarSettingsComponent {
	private plugin: TaskProgressBarPlugin;
	private containerEl: HTMLElement;
	private onBack?: () => void;
	private sourceManager: CalendarSourceManager | null = null;

	constructor(
		plugin: TaskProgressBarPlugin,
		containerEl: HTMLElement,
		onBack?: () => void,
	) {
		this.plugin = plugin;
		this.containerEl = containerEl;
		this.onBack = onBack;
	}

	/**
	 * Display the settings interface
	 */
	display(): void {
		this.containerEl.empty();
		this.containerEl.addClass("calendar-settings-container");

		// this.renderHeader();
		this.renderGlobalSettings();
		this.renderSourcesList();
		this.renderAddSourceButton();
	}

	/**
	 * Render the header with back button
	 */
	private renderHeader(): void {
		const headerSection = this.containerEl.createDiv(
			"settings-tab-section-header",
		);

		const headerContainer = this.containerEl.createDiv(
			"calendar-header-container",
		);
	}

	/**
	 * Render global settings section
	 */
	private renderGlobalSettings(): void {
		const globalContainer = this.containerEl.createDiv(
			"calendar-global-settings",
		);
		globalContainer.createEl("h3", { text: t("Global Settings") });

		const config = this.plugin.settings.icsIntegration;

		// Enable background refresh
		new Setting(globalContainer)
			.setName(t("Enable Background Refresh"))
			.setDesc(t("Automatically sync calendars in the background"))
			.addToggle((toggle) => {
				toggle
					.setValue(config.enableBackgroundRefresh)
					.onChange(async (value) => {
						config.enableBackgroundRefresh = value;
						await this.plugin.saveSettings();
					});
			});

		// Global refresh interval
		new Setting(globalContainer)
			.setName(t("Default Sync Interval"))
			.setDesc(t("How often to check for updates (minutes)"))
			.addText((text) => {
				text.setPlaceholder("60")
					.setValue(config.globalRefreshInterval.toString())
					.onChange(async (value) => {
						const interval = parseInt(value, 10);
						if (!isNaN(interval) && interval > 0) {
							config.globalRefreshInterval = interval;
							await this.plugin.saveSettings();
						}
					});
			});

		// Max cache age
		new Setting(globalContainer)
			.setName(t("Cache Duration"))
			.setDesc(t("How long to keep cached events (hours)"))
			.addText((text) => {
				text.setPlaceholder("24")
					.setValue(config.maxCacheAge.toString())
					.onChange(async (value) => {
						const hours = parseInt(value, 10);
						if (!isNaN(hours) && hours > 0) {
							config.maxCacheAge = hours;
							await this.plugin.saveSettings();
						}
					});
			});

		// Network timeout
		new Setting(globalContainer)
			.setName(t("Network Timeout"))
			.setDesc(t("Request timeout in seconds"))
			.addText((text) => {
				text.setPlaceholder("30")
					.setValue(config.networkTimeout.toString())
					.onChange(async (value) => {
						const timeout = parseInt(value, 10);
						if (!isNaN(timeout) && timeout > 0) {
							config.networkTimeout = timeout;
							await this.plugin.saveSettings();
						}
					});
			});

		// Max events per source
		new Setting(globalContainer)
			.setName(t("Max Events Per Source"))
			.setDesc(t("Maximum number of events to load from each calendar"))
			.addText((text) => {
				text.setPlaceholder("1000")
					.setValue(config.maxEventsPerSource.toString())
					.onChange(async (value) => {
						const max = parseInt(value, 10);
						if (!isNaN(max) && max > 0) {
							config.maxEventsPerSource = max;
							await this.plugin.saveSettings();
						}
					});
			});

		// Default event color
		new Setting(globalContainer)
			.setName(t("Default Event Color"))
			.setDesc(t("Color for events without a custom color"))
			.addColorPicker((color) => {
				color
					.setValue(config.defaultEventColor)
					.onChange(async (value) => {
						config.defaultEventColor = value;
						await this.plugin.saveSettings();
					});
			});
	}

	/**
	 * Render the list of configured sources
	 */
	private renderSourcesList(): void {
		const sourcesContainer = this.containerEl.createDiv(
			"calendar-sources-list",
		);
		sourcesContainer.createEl("h3", { text: t("Connected Calendars") });

		const rawSources = this.plugin.settings.icsIntegration.sources;

		if (rawSources.length === 0) {
			const emptyState = sourcesContainer.createDiv(
				"calendar-empty-state",
			);
			emptyState.createEl("p", {
				text: t(
					"No calendars connected. Add a calendar source to get started.",
				),
			});
			return;
		}

		// Normalize sources to ensure consistent format (handles legacy IcsSource)
		const sources = normalizeCalendarSources(rawSources);

		// Render each source as a card
		sources.forEach((source, index) => {
			this.renderSourceCard(sourcesContainer, source, index);
		});
	}

	/**
	 * Render a single source card
	 */
	private renderSourceCard(
		container: HTMLElement,
		source: CalendarSource,
		index: number,
	): void {
		const card = container.createDiv("calendar-source-card");

		// Left section: icon and info
		const leftSection = card.createDiv("source-left");

		// Type icon
		const iconDiv = leftSection.createDiv("source-icon");
		const meta = CalendarProviderMeta[source.type];
		setIcon(iconDiv, meta.icon);
		iconDiv.style.backgroundColor = source.color || "#3b82f6";

		// Source info
		const infoDiv = leftSection.createDiv("source-info");
		infoDiv.createEl("div", {
			cls: "source-name",
			text: source.name,
		});

		const typeRow = infoDiv.createDiv("source-type-row");
		typeRow.createEl("span", {
			cls: "source-type",
			text: meta.displayName,
		});

		// Status badge
		const statusBadge = typeRow.createEl("span", {
			cls: `source-status ${source.enabled ? "enabled" : "disabled"}`,
		});
		statusBadge.setText(source.enabled ? t("Active") : t("Disabled"));

		// Last sync info (if available)
		if (source.lastSynced) {
			const lastSyncDate = new Date(source.lastSynced);
			infoDiv.createEl("div", {
				cls: "source-last-sync",
				text: `${t("Last sync")}: ${lastSyncDate.toLocaleString()}`,
			});
		}

		// Right section: actions
		const actionsDiv = card.createDiv("source-actions");

		// Enable/disable toggle
		new ToggleComponent(actionsDiv)
			.setValue(source.enabled)
			.setTooltip(source.enabled ? t("Disable") : t("Enable"))
			.onChange(async (value) => {
				source.enabled = value;
				await this.plugin.saveSettings();
				this.display();
			});

		// Sync button
		new ExtraButtonComponent(actionsDiv)
			.setIcon("refresh-cw")
			.setTooltip(t("Sync Now"))
			.onClick(async () => {
				const btn = actionsDiv.querySelector(
					".clickable-icon:nth-child(2)",
				);
				btn?.addClass("spinning");
				try {
					// Use appropriate sync method based on source type
					if (isGoogleSource(source)) {
						// Use GoogleCalendarProvider for Google sources
						await this.syncGoogleSource(source);
					} else if (isOutlookSource(source)) {
						// Use OutlookCalendarProvider for Outlook sources
						await this.syncOutlookSource(source);
					} else if (isUrlIcsSource(source)) {
						// Use IcsManager for URL-based sources
						const icsManager = this.plugin.getIcsManager();
						if (icsManager) {
							const result = await icsManager.syncSource(
								source.id,
							);
							if (result.success) {
								new Notice(t("Sync completed successfully"));
							} else {
								new Notice(
									t("Sync failed") + `: ${result.error}`,
								);
							}
						}
					} else {
						new Notice(
							t("Sync not supported for this source type"),
						);
					}
				} catch (error) {
					console.error("[CalendarSettings] Sync error:", error);
					new Notice(
						t("Sync failed") +
							`: ${error instanceof Error ? error.message : String(error)}`,
					);
				} finally {
					btn?.removeClass("spinning");
					this.display();
				}
			});

		// Edit button
		new ExtraButtonComponent(actionsDiv)
			.setIcon("pencil")
			.setTooltip(t("Edit"))
			.onClick(() => {
				new CalendarSourceModal(
					this.plugin.app,
					this.plugin,
					async (updatedSource) => {
						this.plugin.settings.icsIntegration.sources[index] =
							updatedSource;
						await this.plugin.saveSettings();
						this.display();
					},
					source,
				).open();
			});

		// Delete button
		new ExtraButtonComponent(actionsDiv)
			.setIcon("trash-2")
			.setTooltip(t("Delete"))
			.onClick(async () => {
				if (
					confirm(
						t(
							"Are you sure you want to remove this calendar source?",
						),
					)
				) {
					// Disconnect OAuth if applicable
					if (isOAuthSource(source) && source.auth) {
						try {
							const authManager = this.plugin.getAuthManager?.();
							if (authManager) {
								await authManager.revokeTokens(
									source.type as "google" | "outlook",
									source.auth,
								);
							}
						} catch (e) {
							console.warn("Token revocation failed:", e);
						}
					}

					this.plugin.settings.icsIntegration.sources.splice(
						index,
						1,
					);
					await this.plugin.saveSettings();
					this.display();
				}
			});
	}

	/**
	 * Render the add source button
	 */
	private renderAddSourceButton(): void {
		const addContainer = this.containerEl.createDiv(
			"calendar-add-source-container",
		);

		const addButton = addContainer.createEl("button", {
			cls: "calendar-add-button",
		});
		setIcon(addButton.createSpan(), "plus");
		addButton.createSpan({ text: t("Add Calendar Source") });

		addButton.onclick = () => {
			new CalendarSourceModal(
				this.plugin.app,
				this.plugin,
				async (source) => {
					this.plugin.settings.icsIntegration.sources.push(source);
					await this.plugin.saveSettings();
					this.display();
				},
			).open();
		};
	}

	/**
	 * Sync a Google Calendar source using GoogleCalendarProvider
	 */
	private async syncGoogleSource(
		source: GoogleCalendarSourceConfig,
	): Promise<void> {
		const authManager = this.plugin.getAuthManager?.();
		if (!authManager) {
			throw new Error("Authentication manager not available");
		}

		if (!source.auth?.accessToken) {
			throw new Error("Not authenticated with Google Calendar");
		}

		// Import the provider dynamically to avoid circular deps
		const { GoogleCalendarProvider } =
			await import("@/providers/google-calendar-provider");

		// Create provider instance
		const provider = new GoogleCalendarProvider(source, authManager);

		// Fetch events for the configured date range (default: last 30 days to next 90 days)
		const now = new Date();
		const start = new Date(now);
		start.setDate(start.getDate() - 30);
		const end = new Date(now);
		end.setDate(end.getDate() + 90);

		const events = await provider.getEvents({
			range: { start, end },
			expandRecurring: true,
		});

		// Update cache in IcsManager
		const icsManager = this.plugin.getIcsManager();
		if (icsManager && events.length > 0) {
			// Store events in the IcsManager cache
			icsManager.updateCacheForSource(source.id, events);
		}

		new Notice(t("Sync completed") + `: ${events.length} ` + t("events"));
	}

	/**
	 * Sync an Outlook Calendar source using OutlookCalendarProvider
	 */
	private async syncOutlookSource(
		source: OutlookCalendarSourceConfig,
	): Promise<void> {
		const authManager = this.plugin.getAuthManager?.();
		if (!authManager) {
			throw new Error("Authentication manager not available");
		}

		if (!source.auth?.accessToken) {
			throw new Error("Not authenticated with Outlook Calendar");
		}

		// Import the provider dynamically to avoid circular deps
		const { OutlookCalendarProvider } =
			await import("@/providers/outlook-calendar-provider");

		// Create provider instance
		const provider = new OutlookCalendarProvider(source, authManager);

		// Fetch events for the configured date range (default: last 30 days to next 90 days)
		const now = new Date();
		const start = new Date(now);
		start.setDate(start.getDate() - 30);
		const end = new Date(now);
		end.setDate(end.getDate() + 90);

		const events = await provider.getEvents({
			range: { start, end },
			expandRecurring: true,
		});

		// Update cache in IcsManager
		const icsManager = this.plugin.getIcsManager();
		if (icsManager && events.length > 0) {
			// Store events in the IcsManager cache
			icsManager.updateCacheForSource(source.id, events);
		}

		new Notice(t("Sync completed") + `: ${events.length} ` + t("events"));
	}
}

// ============================================================================
// Calendar Source Modal
// ============================================================================

/**
 * Modal for adding/editing calendar sources
 */
class CalendarSourceModal extends Modal {
	private plugin: TaskProgressBarPlugin;
	private source: Partial<CalendarSource>;
	private onSave: (source: CalendarSource) => void;
	private isEditing: boolean;
	private currentView: "type-select" | "configure" = "type-select";
	private availableCalendars: CalendarListEntry[] = [];
	private isLoadingCalendars = false;
	private authEventHandler: ((event: AuthManagerEvent) => void) | null = null;

	constructor(
		app: App,
		plugin: TaskProgressBarPlugin,
		onSave: (source: CalendarSource) => void,
		existingSource?: CalendarSource,
	) {
		super(app);
		this.plugin = plugin;
		this.onSave = onSave;
		this.isEditing = !!existingSource;

		if (existingSource) {
			// Deep copy existing source
			this.source = JSON.parse(JSON.stringify(existingSource));
			this.currentView = "configure";
		} else {
			// Initialize with basic defaults
			this.source = {
				id: this.generateId(),
				enabled: true,
				refreshInterval: 60,
				showType: "event",
				showAllDayEvents: true,
				showTimedEvents: true,
				createdAt: Date.now(),
				updatedAt: Date.now(),
			};
		}

		this.modalEl.addClass("calendar-source-modal");
	}

	onOpen(): void {
		this.render();
	}

	onClose(): void {
		// Clean up auth event handler
		if (this.authEventHandler) {
			const authManager = this.plugin.getAuthManager?.();
			if (authManager) {
				authManager.off(this.authEventHandler);
			}
			this.authEventHandler = null;
		}
	}

	/**
	 * Render the modal content
	 */
	private render(): void {
		const { contentEl } = this;
		contentEl.empty();

		if (this.currentView === "type-select") {
			this.renderTypeSelection();
		} else {
			this.renderConfiguration();
		}
	}

	/**
	 * Render the type selection view
	 */
	private renderTypeSelection(): void {
		const { contentEl } = this;

		contentEl.createEl("h2", { text: t("Add Calendar Source") });
		contentEl.createEl("p", {
			cls: "modal-description",
			text: t("Select the type of calendar you want to connect:"),
		});

		const grid = contentEl.createDiv("calendar-type-grid");

		const providerTypes: CalendarProviderType[] = [
			"url-ics",
			"google",
			"outlook",
			"apple-caldav",
		];

		for (const type of providerTypes) {
			const meta = CalendarProviderMeta[type];
			const card = grid.createDiv("type-card");

			const iconDiv = card.createDiv("type-icon");
			setIcon(iconDiv, meta.icon);

			card.createDiv("type-name").setText(meta.displayName);
			card.createDiv("type-desc").setText(meta.description);

			card.onclick = () => {
				this.selectType(type);
			};
		}
	}

	/**
	 * Handle type selection
	 */
	private selectType(type: CalendarProviderType): void {
		// Initialize source with type-specific defaults
		switch (type) {
			case "url-ics":
				Object.assign(this.source, {
					type: "url-ics",
					url: "",
					name: t("Web Calendar"),
				});
				break;
			case "google":
				Object.assign(this.source, {
					type: "google",
					name: t("Google Calendar"),
					calendarIds: [],
					includePrimaryCalendar: true,
					includeSharedCalendars: false,
				});
				break;
			case "outlook":
				Object.assign(this.source, {
					type: "outlook",
					name: t("Outlook Calendar"),
					calendarIds: [],
					tenantId: "consumers", // Personal accounts only
					includePrimaryCalendar: true,
					includeSharedCalendars: false,
				});
				break;
			case "apple-caldav":
				Object.assign(this.source, {
					type: "apple-caldav",
					name: t("iCloud Calendar"),
					serverUrl: "https://caldav.icloud.com/",
					username: "",
					calendarHrefs: [],
				});
				break;
		}

		this.currentView = "configure";
		this.render();
	}

	/**
	 * Render the configuration view
	 */
	private renderConfiguration(): void {
		const { contentEl } = this;

		// Header with back button for new sources
		const header = contentEl.createDiv("modal-header");

		if (!this.isEditing) {
			const backBtn = new ButtonComponent(header)
				.setIcon("arrow-left")
				.setTooltip(t("Back"))
				.onClick(() => {
					this.currentView = "type-select";
					this.render();
				});
			backBtn.buttonEl.addClass("modal-back-btn");
		}

		header.createEl("h2", {
			text: this.isEditing
				? t("Edit Calendar Source")
				: t("Configure Calendar"),
		});

		// Common settings
		this.renderCommonSettings(contentEl);

		// Type-specific settings
		const type = this.source.type;
		if (type === "url-ics") {
			this.renderIcsSettings(contentEl);
		} else if (type === "google" || type === "outlook") {
			this.renderOAuthSettings(contentEl);
		} else if (type === "apple-caldav") {
			this.renderAppleSettings(contentEl);
		}

		// Display settings (common to all)
		this.renderDisplaySettings(contentEl);

		// Advanced settings (common to all calendar sources)
		this.renderAdvancedSettings(contentEl);

		// Action buttons
		this.renderActionButtons(contentEl);
	}

	/**
	 * Render common settings (name, color)
	 */
	private renderCommonSettings(container: HTMLElement): void {
		const section = container.createDiv("settings-section");
		section.createEl("h3", { text: t("Basic Settings") });

		// Name
		new Setting(section)
			.setName(t("Name"))
			.setDesc(t("Display name for this calendar"))
			.addText((text) => {
				text.setPlaceholder(t("My Calendar"))
					.setValue(this.source.name || "")
					.onChange((value) => {
						this.source.name = value;
					});
			});

		// Color
		new Setting(section)
			.setName(t("Color"))
			.setDesc(t("Color for events from this calendar"))
			.addColorPicker((color) => {
				color
					.setValue(this.source.color || "#3b82f6")
					.onChange((value) => {
						this.source.color = value;
					});
			});

		// Sync interval
		new Setting(section)
			.setName(t("Sync Interval"))
			.setDesc(t("How often to sync this calendar (minutes)"))
			.addText((text) => {
				text.setPlaceholder("60")
					.setValue((this.source.refreshInterval || 60).toString())
					.onChange((value) => {
						const interval = parseInt(value, 10);
						if (!isNaN(interval) && interval > 0) {
							this.source.refreshInterval = interval;
						}
					});
			});
	}

	/**
	 * Render ICS URL settings
	 */
	private renderIcsSettings(container: HTMLElement): void {
		const section = container.createDiv("settings-section");
		section.createEl("h3", { text: t("Calendar URL") });

		const icsSource = this.source as Partial<UrlIcsSourceConfig>;

		new Setting(section)
			.setName(t("URL"))
			.setDesc(
				t(
					"URL to the ICS file (supports http://, https://, and webcal://)",
				),
			)
			.addText((text) => {
				text.setPlaceholder("https://example.com/calendar.ics")
					.setValue(icsSource.url || "")
					.onChange((value) => {
						icsSource.url = value;
					});
				text.inputEl.style.width = "100%";
			});

		// Authentication (optional)
		const authSection = section.createDiv("auth-section");
		authSection.createEl("h4", { text: t("Authentication (Optional)") });

		new Setting(authSection)
			.setName(t("Authentication Type"))
			.addDropdown((dropdown) => {
				dropdown
					.addOption("none", t("None"))
					.addOption("basic", t("Basic Auth"))
					.addOption("bearer", t("Bearer Token"))
					.setValue(icsSource.auth?.type || "none")
					.onChange((value) => {
						if (value === "none") {
							icsSource.auth = undefined;
						} else {
							icsSource.auth = {
								type: value as any,
								...icsSource.auth,
							};
						}
						this.render();
					});
			});

		if (icsSource.auth?.type === "basic") {
			new Setting(authSection).setName(t("Username")).addText((text) => {
				text.setValue(icsSource.auth?.username || "").onChange(
					(value) => {
						if (icsSource.auth) {
							icsSource.auth.username = value;
						}
					},
				);
			});

			new Setting(authSection).setName(t("Password")).addText((text) => {
				text.inputEl.type = "password";
				text.setValue(icsSource.auth?.password || "").onChange(
					(value) => {
						if (icsSource.auth) {
							icsSource.auth.password = value;
						}
					},
				);
			});
		} else if (icsSource.auth?.type === "bearer") {
			new Setting(authSection).setName(t("Token")).addText((text) => {
				text.setValue(icsSource.auth?.token || "").onChange((value) => {
					if (icsSource.auth) {
						icsSource.auth.token = value;
					}
				});
			});
		}
	}

	/**
	 * Display text replacement rules configuration
	 * Applies to ALL calendar source types
	 */
	private displayTextReplacements(contentEl: HTMLElement): void {
		const textReplacementsContainer = contentEl.createDiv();
		textReplacementsContainer.createEl("h4", {
			text: t("Text Replacements"),
		});
		textReplacementsContainer.createEl("p", {
			text: t(
				"Configure rules to modify event text using regular expressions",
			),
			cls: "setting-item-description",
		});

		// Initialize textReplacements if not exists
		if (!this.source.textReplacements) {
			this.source.textReplacements = [];
		}

		// Container for replacement rules
		const rulesContainer = textReplacementsContainer.createDiv(
			"text-replacements-list",
		);

		const refreshRulesList = () => {
			rulesContainer.empty();

			if (this.source.textReplacements!.length === 0) {
				const emptyState = rulesContainer.createDiv(
					"text-replacements-empty",
				);
				emptyState.createEl("p", {
					text: t("No text replacement rules configured"),
					cls: "setting-item-description",
				});
			} else {
				this.source.textReplacements!.forEach((rule, index) => {
					const ruleContainer = rulesContainer.createDiv(
						"text-replacement-rule",
					);

					// Rule header
					const ruleHeader = ruleContainer.createDiv(
						"text-replacement-header",
					);
					ruleHeader.createEl("strong", {
						text: rule.name || `Rule ${index + 1}`,
					});

					ruleHeader.createEl("span", {
						cls: `text-replacement-status ${
							rule.enabled ? "enabled" : "disabled"
						}`,
						text: rule.enabled ? t("Enabled") : t("Disabled"),
					});

					// Rule details
					const ruleDetails = ruleContainer.createDiv(
						"text-replacement-details",
					);
					ruleDetails.createEl("div", {
						text: `${t("Target")}: ${rule.target}`,
					});
					ruleDetails.createEl("div", {
						text: `${t("Pattern")}: ${rule.pattern}`,
						cls: "text-replacement-pattern",
					});
					ruleDetails.createEl("div", {
						text: `${t("Replacement")}: ${rule.replacement}`,
						cls: "text-replacement-replacement",
					});

					// Rule actions
					const ruleActions = ruleContainer.createDiv(
						"text-replacement-actions",
					);

					const editButton = ruleActions.createEl("button", {
						text: t("Edit"),
						cls: "mod-cta",
					});
					editButton.onclick = () => {
						new TextReplacementModal(
							this.app,
							(updatedRule) => {
								this.source.textReplacements![index] =
									updatedRule;
								refreshRulesList();
							},
							rule,
						).open();
					};

					const toggleButton = ruleActions.createEl("button", {
						text: rule.enabled ? t("Disable") : t("Enable"),
					});
					toggleButton.onclick = () => {
						this.source.textReplacements![index].enabled =
							!rule.enabled;
						refreshRulesList();
					};

					const deleteButton = ruleActions.createEl("button", {
						text: t("Delete"),
						cls: "mod-warning",
					});
					deleteButton.onclick = () => {
						if (
							confirm(
								t(
									"Are you sure you want to delete this text replacement rule?",
								),
							)
						) {
							this.source.textReplacements!.splice(index, 1);
							refreshRulesList();
						}
					};
				});
			}
		};

		refreshRulesList();

		// Add rule button
		const addRuleContainer = textReplacementsContainer.createDiv(
			"text-replacement-add",
		);
		const addButton = addRuleContainer.createEl("button", {
			text: "+ " + t("Add Text Replacement Rule"),
		});
		addButton.onclick = () => {
			new TextReplacementModal(this.app, (newRule) => {
				this.source.textReplacements!.push(newRule);
				refreshRulesList();
			}).open();
		};
	}

	/**
	 * Display holiday configuration section
	 * Applies to ALL calendar source types
	 */
	private displayHolidayConfiguration(contentEl: HTMLElement): void {
		const holidayContainer = contentEl.createDiv();
		holidayContainer.createEl("h4", { text: t("Holiday Configuration") });
		holidayContainer.createEl("p", {
			text: t("Configure how holiday events are detected and displayed"),
			cls: "setting-item-description",
		});

		// Initialize holiday config if not exists
		if (!this.source.holidayConfig) {
			this.source.holidayConfig = HolidayDetector.getDefaultConfig();
		}

		// Enable holiday detection
		new Setting(holidayContainer)
			.setName(t("Enable Holiday Detection"))
			.setDesc(t("Automatically detect and group holiday events"))
			.addToggle((toggle) => {
				toggle
					.setValue(this.source.holidayConfig!.enabled)
					.onChange((value) => {
						this.source.holidayConfig!.enabled = value;
						this.refreshHolidaySettings(holidayContainer);
					});
			});

		this.refreshHolidaySettings(holidayContainer);
	}

	/**
	 * Display status mapping configuration section
	 * Applies to ALL calendar source types
	 */
	private displayStatusMappingConfiguration(contentEl: HTMLElement): void {
		const statusContainer = contentEl.createDiv();
		statusContainer.createEl("h4", { text: t("Status Mapping") });
		statusContainer.createEl("p", {
			text: t(
				"Configure how calendar events are mapped to task statuses",
			),
			cls: "setting-item-description",
		});

		// Initialize status mapping if not exists
		if (!this.source.statusMapping) {
			this.source.statusMapping = {
				enabled: false,
				timingRules: {
					pastEvents: "x",
					currentEvents: "/",
					futureEvents: " ",
				},
				overrideIcsStatus: true,
			};
		}

		// Enable status mapping
		new Setting(statusContainer)
			.setName(t("Enable Status Mapping"))
			.setDesc(
				t(
					"Automatically map calendar events to specific task statuses",
				),
			)
			.addToggle((toggle) => {
				toggle
					.setValue(this.source.statusMapping!.enabled)
					.onChange((value) => {
						this.source.statusMapping!.enabled = value;
						this.refreshStatusMappingSettings(statusContainer);
					});
			});

		this.refreshStatusMappingSettings(statusContainer);
	}

	/**
	 * Refresh holiday settings display
	 * Applies to ALL calendar source types
	 */
	private refreshHolidaySettings(container: HTMLElement): void {
		// Remove existing holiday settings
		const existingSettings = container.querySelectorAll(".holiday-setting");
		existingSettings.forEach((setting) => setting.remove());

		if (!this.source.holidayConfig?.enabled) {
			return;
		}

		// Grouping strategy
		new Setting(container)
			.setName(t("Grouping Strategy"))
			.setDesc(t("How to handle consecutive holiday events"))
			.setClass("holiday-setting")
			.addDropdown((dropdown) => {
				dropdown
					.addOption("none", t("Show All Events"))
					.addOption("first-only", t("Show First Day Only"))
					.addOption("summary", t("Show Summary"))
					.addOption("range", t("Show First and Last"))
					.setValue(this.source.holidayConfig!.groupingStrategy)
					.onChange((value) => {
						this.source.holidayConfig!.groupingStrategy =
							value as any;
					});
			});

		// Max gap days
		new Setting(container)
			.setName(t("Maximum Gap Days"))
			.setDesc(
				t("Maximum days between events to consider them consecutive"),
			)
			.setClass("holiday-setting")
			.addText((text) => {
				text.setPlaceholder("1")
					.setValue(this.source.holidayConfig!.maxGapDays.toString())
					.onChange((value) => {
						const gap = parseInt(value, 10);
						if (!isNaN(gap) && gap >= 0) {
							this.source.holidayConfig!.maxGapDays = gap;
						}
					});
			});

		// Show in forecast
		new Setting(container)
			.setName(t("Show in Forecast"))
			.setDesc(t("Whether to show holiday events in forecast view"))
			.setClass("holiday-setting")
			.addToggle((toggle) => {
				toggle
					.setValue(this.source.holidayConfig!.showInForecast)
					.onChange((value) => {
						this.source.holidayConfig!.showInForecast = value;
					});
			});

		// Show in calendar
		new Setting(container)
			.setName(t("Show in Calendar"))
			.setDesc(t("Whether to show holiday events in calendar view"))
			.setClass("holiday-setting")
			.addToggle((toggle) => {
				toggle
					.setValue(this.source.holidayConfig!.showInCalendar)
					.onChange((value) => {
						this.source.holidayConfig!.showInCalendar = value;
					});
			});

		// Detection patterns
		const patternsContainer = container.createDiv("holiday-setting");
		patternsContainer.createEl("h4", { text: t("Detection Patterns") });

		// Summary patterns
		new Setting(patternsContainer)
			.setName(t("Summary Patterns"))
			.setDesc(
				t("Regex patterns to match in event titles (one per line)"),
			)
			.addTextArea((text) => {
				text.setValue(
					(
						this.source.holidayConfig!.detectionPatterns.summary ||
						[]
					).join("\n"),
				).onChange((value) => {
					this.source.holidayConfig!.detectionPatterns.summary = value
						.split("\n")
						.map((line) => line.trim())
						.filter((line) => line.length > 0);
				});
			});

		// Keywords
		new Setting(patternsContainer)
			.setName(t("Keywords"))
			.setDesc(t("Keywords to detect in event text (one per line)"))
			.addTextArea((text) => {
				text.setValue(
					(
						this.source.holidayConfig!.detectionPatterns.keywords ||
						[]
					).join("\n"),
				).onChange((value) => {
					this.source.holidayConfig!.detectionPatterns.keywords =
						value
							.split("\n")
							.map((line) => line.trim())
							.filter((line) => line.length > 0);
				});
			});

		// Categories
		new Setting(patternsContainer)
			.setName(t("Categories"))
			.setDesc(
				t("Event categories that indicate holidays (one per line)"),
			)
			.addTextArea((text) => {
				text.setValue(
					(
						this.source.holidayConfig!.detectionPatterns
							.categories || []
					).join("\n"),
				).onChange((value) => {
					this.source.holidayConfig!.detectionPatterns.categories =
						value
							.split("\n")
							.map((line) => line.trim())
							.filter((line) => line.length > 0);
				});
			});

		// Group display format
		new Setting(container)
			.setName(t("Group Display Format"))
			.setDesc(
				t(
					"Format for grouped holiday display. Use {title}, {count}, {startDate}, {endDate}",
				),
			)
			.setClass("holiday-setting")
			.addText((text) => {
				text.setPlaceholder("{title} ({count} days)")
					.setValue(
						this.source.holidayConfig!.groupDisplayFormat || "",
					)
					.onChange((value) => {
						this.source.holidayConfig!.groupDisplayFormat =
							value || undefined;
					});
			});
	}

	/**
	 * Refresh status mapping settings display
	 * Applies to ALL calendar source types
	 */
	private refreshStatusMappingSettings(container: HTMLElement): void {
		// Remove existing status mapping settings
		const existingSettings = container.querySelectorAll(
			".status-mapping-setting",
		);
		existingSettings.forEach((setting) => setting.remove());

		if (!this.source.statusMapping?.enabled) {
			return;
		}

		// Override ICS status
		new Setting(container)
			.setName(t("Override Event Status"))
			.setDesc(t("Override original event status with mapped status"))
			.setClass("status-mapping-setting")
			.addToggle((toggle) => {
				toggle
					.setValue(this.source.statusMapping!.overrideIcsStatus)
					.onChange((value) => {
						this.source.statusMapping!.overrideIcsStatus = value;
					});
			});

		// Timing rules section
		const timingContainer = container.createDiv("status-mapping-setting");
		timingContainer.createEl("h4", { text: t("Timing Rules") });

		// Past events status
		new Setting(timingContainer)
			.setName(t("Past Events Status"))
			.setDesc(t("Status for events that have already ended"))
			.addDropdown((dropdown) => {
				dropdown
					.addOption(" ", t("Status Incomplete"))
					.addOption("x", t("Status Complete"))
					.addOption("-", t("Status Cancelled"))
					.addOption("/", t("Status In Progress"))
					.addOption("?", t("Status Question"))
					.setValue(this.source.statusMapping!.timingRules.pastEvents)
					.onChange((value) => {
						this.source.statusMapping!.timingRules.pastEvents =
							value as any;
					});
			});

		// Current events status
		new Setting(timingContainer)
			.setName(t("Current Events Status"))
			.setDesc(t("Status for events happening today"))
			.addDropdown((dropdown) => {
				dropdown
					.addOption(" ", t("Status Incomplete"))
					.addOption("x", t("Status Complete"))
					.addOption("-", t("Status Cancelled"))
					.addOption("/", t("Status In Progress"))
					.addOption("?", t("Status Question"))
					.setValue(
						this.source.statusMapping!.timingRules.currentEvents,
					)
					.onChange((value) => {
						this.source.statusMapping!.timingRules.currentEvents =
							value as any;
					});
			});

		// Future events status
		new Setting(timingContainer)
			.setName(t("Future Events Status"))
			.setDesc(t("Status for events in the future"))
			.addDropdown((dropdown) => {
				dropdown
					.addOption(" ", t("Status Incomplete"))
					.addOption("x", t("Status Complete"))
					.addOption("-", t("Status Cancelled"))
					.addOption("/", t("Status In Progress"))
					.addOption("?", t("Status Question"))
					.setValue(
						this.source.statusMapping!.timingRules.futureEvents,
					)
					.onChange((value) => {
						this.source.statusMapping!.timingRules.futureEvents =
							value as any;
					});
			});

		// Property rules section
		const propertyContainer = container.createDiv("status-mapping-setting");
		propertyContainer.createEl("h4", { text: t("Property Rules") });
		propertyContainer.createEl("p", {
			text: t(
				"Optional rules based on event properties (higher priority than timing rules)",
			),
			cls: "setting-item-description",
		});

		// Initialize property rules if not exists
		if (!this.source.statusMapping!.propertyRules) {
			this.source.statusMapping!.propertyRules = {};
		}

		// Holiday mapping
		new Setting(propertyContainer)
			.setName(t("Holiday Status"))
			.setDesc(t("Status for events detected as holidays"))
			.addDropdown((dropdown) => {
				dropdown
					.addOption("", t("Use timing rules"))
					.addOption(" ", t("Status Incomplete"))
					.addOption("x", t("Status Complete"))
					.addOption("-", t("Status Cancelled"))
					.addOption("/", t("Status In Progress"))
					.addOption("?", t("Status Question"))
					.setValue(
						this.source.statusMapping!.propertyRules!.holidayMapping
							?.holidayStatus || "",
					)
					.onChange((value) => {
						if (
							!this.source.statusMapping!.propertyRules!
								.holidayMapping
						) {
							this.source.statusMapping!.propertyRules!.holidayMapping =
								{
									holidayStatus: "-",
								};
						}
						if (value) {
							this.source.statusMapping!.propertyRules!.holidayMapping.holidayStatus =
								value as any;
						} else {
							delete this.source.statusMapping!.propertyRules!
								.holidayMapping;
						}
					});
			});

		// Category mapping
		new Setting(propertyContainer)
			.setName(t("Category Mapping"))
			.setDesc(
				t(
					"Map specific categories to statuses (format: category:status, one per line)",
				),
			)
			.addTextArea((text) => {
				const categoryMapping =
					this.source.statusMapping!.propertyRules!.categoryMapping ||
					{};
				const mappingText = Object.entries(categoryMapping)
					.map(([category, status]) => `${category}:${status}`)
					.join("\n");

				text.setValue(mappingText).onChange((value) => {
					const mapping: Record<string, any> = {};
					const lines = value
						.split("\n")
						.filter((line) => line.trim());

					for (const line of lines) {
						const [category, status] = line
							.split(":")
							.map((s) => s.trim());
						if (category && status) {
							mapping[category] = status;
						}
					}

					if (Object.keys(mapping).length > 0) {
						this.source.statusMapping!.propertyRules!.categoryMapping =
							mapping;
					} else {
						delete this.source.statusMapping!.propertyRules!
							.categoryMapping;
					}
				});
			});
	}

	/**
	 * Render OAuth settings (Google/Outlook)
	 */
	private renderOAuthSettings(container: HTMLElement): void {
		const section = container.createDiv("settings-section");
		const provider = this.source.type as "google" | "outlook";
		const oauthSource = this.source as
			| Partial<GoogleCalendarSourceConfig>
			| Partial<OutlookCalendarSourceConfig>;

		section.createEl("h3", {
			text: t("Account Connection"),
		});

		const authStatus = section.createDiv("oauth-status");

		if (oauthSource.auth?.accessToken) {
			// Connected state
			authStatus.addClass("connected");

			const connectedInfo = authStatus.createDiv("connected-info");
			setIcon(connectedInfo.createSpan("status-icon"), "check-circle");
			connectedInfo.createSpan({
				cls: "status-text",
				text: t("Connected"),
			});

			if (oauthSource.accountEmail) {
				connectedInfo.createSpan({
					cls: "account-email",
					text: oauthSource.accountEmail,
				});
			}

			new ButtonComponent(authStatus)
				.setButtonText(t("Disconnect"))
				.setWarning()
				.onClick(async () => {
					if (confirm(t("Are you sure you want to disconnect?"))) {
						oauthSource.auth = undefined;
						oauthSource.accountEmail = undefined;
						if (provider === "google") {
							(
								oauthSource as Partial<GoogleCalendarSourceConfig>
							).calendarIds = [];
						} else {
							(
								oauthSource as Partial<OutlookCalendarSourceConfig>
							).calendarIds = [];
						}
						this.availableCalendars = [];
						this.render();
					}
				});

			// Calendar selection
			this.renderCalendarSelector(section, provider);
		} else {
			// Disconnected state
			authStatus.addClass("disconnected");

			const disconnectedInfo = authStatus.createDiv("disconnected-info");
			setIcon(disconnectedInfo.createSpan("status-icon"), "alert-circle");
			disconnectedInfo.createSpan({
				cls: "status-text",
				text: t("Not connected"),
			});

			const connectBtn = new ButtonComponent(authStatus)
				.setButtonText(
					t(
						`Connect ${provider === "google" ? "Google" : "Microsoft"} Account`,
					),
				)
				.setCta()
				.onClick(() => this.startOAuthFlow(provider));

			// OAuth client ID configuration hint
			section.createEl("p", {
				cls: "oauth-hint",
				text: t(
					"Note: OAuth requires a configured client ID. Contact the plugin developer or configure your own OAuth app.",
				),
			});
		}

		// Outlook-specific: Tenant selection
		if (provider === "outlook") {
			const outlookSource =
				oauthSource as Partial<OutlookCalendarSourceConfig>;

			new Setting(section)
				.setName(t("Account Type"))
				.setDesc(t("Select the type of Microsoft account"))
				.addDropdown((dropdown) => {
					dropdown
						.addOption("common", t("Personal & Work (Common)"))
						.addOption("consumers", t("Personal Account Only"))
						.addOption(
							"organizations",
							t("Work/School Account Only"),
						)
						.setValue(outlookSource.tenantId || "common")
						.onChange((value) => {
							outlookSource.tenantId = value;
						});
				});
		}
	}

	/**
	 * Render Apple CalDAV settings
	 */
	private renderAppleSettings(container: HTMLElement): void {
		const section = container.createDiv("settings-section");
		section.createEl("h3", { text: t("iCloud Credentials") });

		const appleSource = this.source as Partial<AppleCaldavSourceConfig>;

		// Important notice about App-Specific Password
		const notice = section.createDiv("apple-notice");
		notice.createEl("strong", { text: t("Important: ") });
		notice.createSpan({
			text: t(
				"You must use an App-Specific Password, not your Apple ID password. Generate one at ",
			),
		});
		notice.createEl("a", {
			text: "appleid.apple.com",
			href: "https://appleid.apple.com/account/manage",
		});

		new Setting(section)
			.setName(t("Apple ID"))
			.setDesc(t("Your Apple ID email address"))
			.addText((text) => {
				text.setPlaceholder("your@email.com")
					.setValue(appleSource.username || "")
					.onChange((value) => {
						appleSource.username = value;
					});
			});

		new Setting(section)
			.setName(t("App-Specific Password"))
			.setDesc(t("Generated from Apple ID settings"))
			.addText((text) => {
				text.inputEl.type = "password";
				text.setPlaceholder("xxxx-xxxx-xxxx-xxxx")
					.setValue(appleSource.appSpecificPassword || "")
					.onChange((value) => {
						appleSource.appSpecificPassword = value;
					});
			});

		new Setting(section)
			.setName(t("CalDAV Server"))
			.setDesc(t("Usually https://caldav.icloud.com/"))
			.addText((text) => {
				text.setValue(
					appleSource.serverUrl || "https://caldav.icloud.com/",
				).onChange((value) => {
					appleSource.serverUrl = value;
				});
			});

		// Test connection and load calendars button
		new Setting(section)
			.setName(t("Calendars"))
			.setDesc(t("Connect to load available calendars"))
			.addButton((btn) => {
				btn.setButtonText(
					this.isLoadingCalendars
						? t("Loading...")
						: t("Load Calendars"),
				)
					.setDisabled(this.isLoadingCalendars)
					.onClick(async () => {
						await this.loadCalendarsForApple();
					});
			});

		// Calendar selector
		if (this.availableCalendars.length > 0) {
			this.renderCalendarSelector(section, "apple-caldav");
		}
	}

	/**
	 * Render calendar selector for OAuth and CalDAV providers
	 */
	private renderCalendarSelector(
		container: HTMLElement,
		provider: "google" | "outlook" | "apple-caldav",
	): void {
		const selector = container.createDiv("calendar-selector");
		selector.createEl("h4", { text: t("Select Calendars to Sync") });

		if (this.isLoadingCalendars) {
			selector
				.createDiv("loading-state")
				.setText(t("Loading calendars..."));
			return;
		}

		if (this.availableCalendars.length === 0) {
			const emptyState = selector.createDiv("empty-calendars");
			emptyState.setText(
				t("No calendars found. Click 'Load Calendars' to refresh."),
			);

			new ButtonComponent(selector)
				.setButtonText(t("Load Calendars"))
				.onClick(async () => {
					if (provider === "apple-caldav") {
						await this.loadCalendarsForApple();
					} else {
						await this.loadCalendarsForOAuth(provider);
					}
				});
			return;
		}

		// Get selected calendar IDs
		let selectedIds: Set<string>;
		if (provider === "apple-caldav") {
			const appleSource = this.source as Partial<AppleCaldavSourceConfig>;
			selectedIds = new Set(appleSource.calendarHrefs || []);
		} else {
			const oauthSource = this.source as
				| Partial<GoogleCalendarSourceConfig>
				| Partial<OutlookCalendarSourceConfig>;
			selectedIds = new Set(oauthSource.calendarIds || []);
		}

		// Render calendar checkboxes
		for (const cal of this.availableCalendars) {
			const calRow = selector.createDiv("calendar-row");

			const checkbox = calRow.createEl("input", { type: "checkbox" });
			checkbox.checked = selectedIds.has(cal.id);
			checkbox.onchange = () => {
				if (checkbox.checked) {
					selectedIds.add(cal.id);
				} else {
					selectedIds.delete(cal.id);
				}

				// Update source
				if (provider === "apple-caldav") {
					(
						this.source as Partial<AppleCaldavSourceConfig>
					).calendarHrefs = Array.from(selectedIds);
				} else {
					(
						this.source as
							| Partial<GoogleCalendarSourceConfig>
							| Partial<OutlookCalendarSourceConfig>
					).calendarIds = Array.from(selectedIds);
				}
			};

			const label = calRow.createEl("label");
			if (cal.color) {
				const colorDot = label.createSpan("calendar-color");
				colorDot.style.backgroundColor = cal.color;
			}
			label.createSpan({ text: cal.name });
			if (cal.primary) {
				label.createSpan({
					cls: "primary-badge",
					text: t("Primary"),
				});
			}
		}
	}

	/**
	 * Render display settings
	 */
	private renderDisplaySettings(container: HTMLElement): void {
		const section = container.createDiv("settings-section");
		section.createEl("h3", { text: t("Display Settings") });

		// Show type
		new Setting(section)
			.setName(t("Display Mode"))
			.setDesc(t("How to show events from this calendar"))
			.addDropdown((dropdown) => {
				dropdown
					.addOption("event", t("Full Events"))
					.addOption("badge", t("Badge Only"))
					.setValue(this.source.showType || "event")
					.onChange((value) => {
						this.source.showType = value as "event" | "badge";
					});
			});

		// Show all-day events
		new Setting(section)
			.setName(t("Show All-Day Events"))
			.addToggle((toggle) => {
				toggle
					.setValue(this.source.showAllDayEvents ?? true)
					.onChange((value) => {
						this.source.showAllDayEvents = value;
					});
			});

		// Show timed events
		new Setting(section)
			.setName(t("Show Timed Events"))
			.addToggle((toggle) => {
				toggle
					.setValue(this.source.showTimedEvents ?? true)
					.onChange((value) => {
						this.source.showTimedEvents = value;
					});
			});
	}

	/**
	 * Render advanced settings (Text Replacements, Holiday Config, Status Mapping)
	 * These settings apply to ALL calendar source types
	 */
	private renderAdvancedSettings(container: HTMLElement): void {
		const section = container.createDiv(
			"settings-section advanced-settings",
		);
		section.createEl("h3", { text: t("Advanced Settings") });
		section.createEl("p", {
			text: t(
				"Configure text processing, holiday detection, and status mapping for events from this calendar",
			),
			cls: "setting-item-description",
		});

		// Text Replacements
		this.displayTextReplacements(section);

		// Holiday Configuration
		this.displayHolidayConfiguration(section);

		// Status Mapping Configuration
		this.displayStatusMappingConfiguration(section);
	}

	/**
	 * Render action buttons
	 */
	private renderActionButtons(container: HTMLElement): void {
		const buttonContainer = container.createDiv("modal-button-container");

		new ButtonComponent(buttonContainer)
			.setButtonText(t("Save"))
			.setCta()
			.onClick(() => this.handleSave());

		new ButtonComponent(buttonContainer)
			.setButtonText(t("Cancel"))
			.onClick(() => this.close());
	}

	/**
	 * Start OAuth flow
	 */
	private async startOAuthFlow(
		provider: "google" | "outlook",
	): Promise<void> {
		const authManager = this.plugin.getAuthManager?.();
		if (!authManager) {
			new Notice(t("Authentication manager not available"));
			return;
		}

		// Set up event handler for auth success
		this.authEventHandler = (event: AuthManagerEvent) => {
			if (event.type === "auth-success" && event.provider === provider) {
				const oauthSource = this.source as
					| Partial<GoogleCalendarSourceConfig>
					| Partial<OutlookCalendarSourceConfig>;
				oauthSource.auth = event.tokens;
				oauthSource.accountEmail = event.email;

				// Load calendars after successful auth
				this.loadCalendarsForOAuth(provider).then(() => {
					this.render();
				});
			}
		};

		authManager.on(this.authEventHandler);

		// Start the flow
		const tenantId =
			provider === "outlook"
				? (this.source as Partial<OutlookCalendarSourceConfig>).tenantId
				: undefined;

		await authManager.startOAuthFlow(provider, { tenantId });
	}

	/**
	 * Load calendars for OAuth providers
	 */
	private async loadCalendarsForOAuth(
		provider: "google" | "outlook",
	): Promise<void> {
		const oauthSource = this.source as
			| Partial<GoogleCalendarSourceConfig>
			| Partial<OutlookCalendarSourceConfig>;

		if (!oauthSource.auth?.accessToken) {
			new Notice(t("Not connected"));
			return;
		}

		this.isLoadingCalendars = true;
		this.render();

		try {
			const authManager = this.plugin.getAuthManager?.();
			if (!authManager) throw new Error("Auth manager not available");

			// Create temporary provider to fetch calendars
			const tempSource = { ...this.source } as CalendarSource;
			const factoryOptions = { authManager };
			const factory = new ProviderFactory(factoryOptions);
			const tempProvider = factory.createProvider(tempSource);

			this.availableCalendars = await tempProvider.listCalendars();

			// Auto-select primary calendar if no calendars selected
			if (
				oauthSource.calendarIds?.length === 0 &&
				this.availableCalendars.length > 0
			) {
				const primary = this.availableCalendars.find((c) => c.primary);
				if (primary) {
					oauthSource.calendarIds = [primary.id];
				}
			}
		} catch (error) {
			console.error("Failed to load calendars:", error);
			new Notice(t("Failed to load calendars") + `: ${error.message}`);
		} finally {
			this.isLoadingCalendars = false;
			this.render();
		}
	}

	/**
	 * Load calendars for Apple CalDAV
	 */
	private async loadCalendarsForApple(): Promise<void> {
		const appleSource = this.source as Partial<AppleCaldavSourceConfig>;

		if (!appleSource.username || !appleSource.appSpecificPassword) {
			new Notice(
				t("Please enter your Apple ID and App-Specific Password"),
			);
			return;
		}

		this.isLoadingCalendars = true;
		this.render();

		try {
			// Create temporary provider
			const tempSource = {
				...appleSource,
				id: this.source.id,
				name: this.source.name || "Apple",
				type: "apple-caldav",
				enabled: true,
				refreshInterval: 60,
				showType: "event",
				showAllDayEvents: true,
				showTimedEvents: true,
				calendarHrefs: [],
				createdAt: Date.now(),
				updatedAt: Date.now(),
			} as AppleCaldavSourceConfig;

			const { AppleCaldavProvider } =
				await import("@/providers/apple-caldav-provider");
			const tempProvider = new AppleCaldavProvider(tempSource);

			this.availableCalendars = await tempProvider.listCalendars();

			if (this.availableCalendars.length === 0) {
				new Notice(
					t(
						"No calendars found. Check your credentials and try again.",
					),
				);
			}
		} catch (error) {
			console.error("Failed to load calendars:", error);
			new Notice(t("Failed to connect") + `: ${error.message}`);
		} finally {
			this.isLoadingCalendars = false;
			this.render();
		}
	}

	/**
	 * Handle save
	 */
	private handleSave(): void {
		// Validate
		if (!this.source.name?.trim()) {
			new Notice(t("Please enter a name for this calendar"));
			return;
		}

		if (this.source.type === "url-ics") {
			const icsSource = this.source as Partial<UrlIcsSourceConfig>;
			if (!icsSource.url?.trim()) {
				new Notice(t("Please enter a calendar URL"));
				return;
			}
		}

		if (this.source.type === "google" || this.source.type === "outlook") {
			const oauthSource = this.source as
				| Partial<GoogleCalendarSourceConfig>
				| Partial<OutlookCalendarSourceConfig>;
			if (!oauthSource.auth?.accessToken) {
				new Notice(t("Please connect your account first"));
				return;
			}
		}

		if (this.source.type === "apple-caldav") {
			const appleSource = this.source as Partial<AppleCaldavSourceConfig>;
			if (!appleSource.username || !appleSource.appSpecificPassword) {
				new Notice(t("Please enter your Apple ID credentials"));
				return;
			}
		}

		// Update timestamp
		this.source.updatedAt = Date.now();

		// Call save callback
		this.onSave(this.source as CalendarSource);
		this.close();
	}

	/**
	 * Generate unique ID
	 */
	private generateId(): string {
		return `cal-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
	}
}

// ============================================================================
// Text Replacement Modal
// ============================================================================

/**
 * Modal for adding/editing text replacement rules
 */
class TextReplacementModal extends Modal {
	private rule: IcsTextReplacement;
	private onSave: (rule: IcsTextReplacement) => void;
	private isEditing: boolean;

	constructor(
		app: App,
		onSave: (rule: IcsTextReplacement) => void,
		existingRule?: IcsTextReplacement,
	) {
		super(app);
		this.onSave = onSave;
		this.isEditing = !!existingRule;
		this.modalEl.addClass("ics-text-replacement-modal");
		if (existingRule) {
			this.rule = { ...existingRule };
		} else {
			this.rule = {
				id: this.generateId(),
				name: "",
				enabled: true,
				target: "summary",
				pattern: "",
				replacement: "",
				flags: "g",
			};
		}
	}

	onOpen(): void {
		const { contentEl } = this;
		contentEl.empty();

		contentEl.createEl("h2", {
			text: this.isEditing
				? t("Edit Text Replacement Rule")
				: t("Add Text Replacement Rule"),
		});

		// Rule name
		new Setting(contentEl)
			.setName(t("Rule Name"))
			.setDesc(t("Descriptive name for this replacement rule"))
			.addText((text) => {
				text.setPlaceholder(t("Remove Meeting Prefix"))
					.setValue(this.rule.name)
					.onChange((value) => {
						this.rule.name = value;
					});
			});

		// Enabled
		new Setting(contentEl)
			.setName(t("Enabled"))
			.setDesc(t("Whether this rule is active"))
			.addToggle((toggle) => {
				toggle.setValue(this.rule.enabled).onChange((value) => {
					this.rule.enabled = value;
				});
			});

		// Target field
		new Setting(contentEl)
			.setName(t("Target Field"))
			.setDesc(t("Which field to apply the replacement to"))
			.addDropdown((dropdown) => {
				dropdown
					.addOption("summary", t("Summary/Title"))
					.addOption("description", t("Description"))
					.addOption("location", t("Location"))
					.addOption("all", t("All Fields"))
					.setValue(this.rule.target)
					.onChange((value) => {
						this.rule.target = value as
							| "summary"
							| "description"
							| "location"
							| "all";
					});
			});

		// Store references to update test output
		let testInput: TextComponent;
		let testOutput: HTMLElement;

		// Define the update function
		const updateTestOutput = (input: string) => {
			if (!testOutput) return;

			try {
				if (this.rule.pattern && input) {
					const regex = new RegExp(
						this.rule.pattern,
						this.rule.flags || "g",
					);
					const result = input.replace(regex, this.rule.replacement);
					const resultSpan = testOutput.querySelector(
						".test-result",
					) as HTMLElement;
					if (resultSpan) {
						resultSpan.textContent = result;
						resultSpan.style.color =
							result !== input ? "#4caf50" : "#666";
					}
				} else {
					const resultSpan = testOutput.querySelector(
						".test-result",
					) as HTMLElement;
					if (resultSpan) {
						resultSpan.textContent = input || "";
						resultSpan.style.color = "#666";
					}
				}
			} catch (error) {
				const resultSpan = testOutput.querySelector(
					".test-result",
				) as HTMLElement;
				if (resultSpan) {
					resultSpan.textContent = "Invalid regex pattern";
					resultSpan.style.color = "#f44336";
				}
			}
		};

		// Pattern
		new Setting(contentEl)
			.setName(t("Pattern (Regular Expression)"))
			.setDesc(
				t(
					"Regular expression pattern to match. Use parentheses for capture groups.",
				),
			)
			.addText((text) => {
				text.setPlaceholder("^Meeting: ")
					.setValue(this.rule.pattern)
					.onChange((value) => {
						this.rule.pattern = value;
						if (testInput && testInput.getValue()) {
							updateTestOutput(testInput.getValue());
						}
					});
			});

		// Replacement
		new Setting(contentEl)
			.setName(t("Replacement"))
			.setDesc(
				t(
					"Text to replace matches with. Use $1, $2, etc. for capture groups.",
				),
			)
			.addText((text) => {
				text.setPlaceholder("")
					.setValue(this.rule.replacement)
					.onChange((value) => {
						this.rule.replacement = value;
						if (testInput && testInput.getValue()) {
							updateTestOutput(testInput.getValue());
						}
					});
			});

		// Flags
		new Setting(contentEl)
			.setName(t("Regex Flags"))
			.setDesc(
				t(
					"Regular expression flags (e.g., 'g' for global, 'i' for case-insensitive)",
				),
			)
			.addText((text) => {
				text.setPlaceholder("g")
					.setValue(this.rule.flags || "")
					.onChange((value) => {
						this.rule.flags = value;
						if (testInput && testInput.getValue()) {
							updateTestOutput(testInput.getValue());
						}
					});
			});

		// Examples section
		const examplesContainer = contentEl.createDiv();
		examplesContainer.createEl("h3", { text: t("Examples") });

		const examplesList = examplesContainer.createEl("ul");

		// Remove prefix example
		const example1 = examplesList.createEl("li");
		example1.createEl("strong", { text: t("Remove prefix") + ": " });
		example1.createSpan({ text: "Pattern: " });
		example1.createEl("code", { text: "^Meeting: " });
		example1.createSpan({ text: ", Replacement: " });
		example1.createEl("code", { text: "" });

		// Replace room numbers example
		const example2 = examplesList.createEl("li");
		example2.createEl("strong", { text: t("Replace room numbers") + ": " });
		example2.createSpan({ text: "Pattern: " });
		example2.createEl("code", { text: "Room (\\d+)" });
		example2.createSpan({ text: ", Replacement: " });
		example2.createEl("code", { text: "Conference Room $1" });

		// Swap words example
		const example3 = examplesList.createEl("li");
		example3.createEl("strong", { text: t("Swap words") + ": " });
		example3.createSpan({ text: "Pattern: " });
		example3.createEl("code", { text: "(\\w+) with (\\w+)" });
		example3.createSpan({ text: ", Replacement: " });
		example3.createEl("code", { text: "$2 and $1" });

		// Test section
		const testContainer = contentEl.createDiv();
		testContainer.createEl("h3", { text: t("Test Rule") });

		// Create test output first
		testOutput = testContainer.createDiv("test-output");
		testOutput.createEl("strong", { text: t("Output: ") });
		testOutput.createEl("span", { cls: "test-result" });

		// Create test input
		new Setting(testContainer)
			.setName(t("Test Input"))
			.setDesc(t("Enter text to test the replacement rule"))
			.addText((text) => {
				testInput = text;
				text.setPlaceholder("Meeting: Weekly Standup").onChange(
					(value) => {
						updateTestOutput(value);
					},
				);
			});

		// Buttons
		const buttonContainer = contentEl.createDiv("modal-button-container");

		const saveButton = buttonContainer.createEl("button", {
			text: t("Save"),
			cls: "mod-cta",
		});
		saveButton.onclick = () => {
			if (this.validateRule()) {
				this.onSave(this.rule);
				this.close();
			}
		};

		const cancelButton = buttonContainer.createEl("button", {
			text: t("Cancel"),
		});
		cancelButton.onclick = () => {
			this.close();
		};
	}

	private validateRule(): boolean {
		if (!this.rule.name.trim()) {
			new Notice(t("Please enter a name for the rule"));
			return false;
		}

		if (!this.rule.pattern.trim()) {
			new Notice(t("Please enter a pattern"));
			return false;
		}

		// Test if the regex pattern is valid
		try {
			new RegExp(this.rule.pattern, this.rule.flags || "g");
		} catch (error) {
			new Notice(t("Invalid regular expression pattern"));
			return false;
		}

		return true;
	}

	private generateId(): string {
		return `rule-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
	}
}
