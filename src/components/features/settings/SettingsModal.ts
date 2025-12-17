import {
	App,
	Modal,
	setIcon,
	ButtonComponent,
	Platform,
	requireApiVersion,
	debounce,
} from "obsidian";
import TaskProgressBarPlugin from "@/index";
import { t } from "@/translations/helper";
import {
	renderAboutSettingsTab,
	renderBetaTestSettingsTab,
	renderHabitSettingsTab,
	renderInterfaceSettingsTab,
	renderProgressSettingsTab,
	renderTaskStatusSettingsTab,
	renderDatePrioritySettingsTab,
	renderTaskFilterSettingsTab,
	renderWorkflowSettingsTab,
	renderQuickCaptureSettingsTab,
	renderTaskHandlerSettingsTab,
	renderViewSettingsTab,
	renderProjectSettingsTab,
	renderRewardSettingsTab,
	renderTimelineSidebarSettingsTab,
	renderIndexSettingsTab,
	IcsSettingsComponent,
	CalendarSettingsComponent,
	renderDesktopIntegrationSettingsTab,
	renderCalendarViewSettingsTab,
} from "./index";
import { renderFileFilterSettingsTab } from "./tabs/FileFilterSettingsTab";
import { renderTimeParsingSettingsTab } from "./tabs/TimeParsingSettingsTab";
import { renderMcpIntegrationSettingsTab } from "./tabs/McpIntegrationSettingsTab";
import { renderTaskTimerSettingTab } from "./tabs/TaskTimerSettingsTab";
import { renderBasesSettingsTab } from "./tabs/BasesSettingsTab";
import { renderWorkspaceSettingsTab } from "./tabs/WorkspaceSettingTab";
import { IframeModal } from "@/components/ui/modals/IframeModal";

interface SearchResultItem {
	id: string;
	tabId: string;
	tabName: string;
	name: string;
	desc: string;
	category: string;
}

interface SettingsTab {
	id: string;
	name: string;
	icon: string;
	category: string;
}

interface SettingsCategory {
	id: string;
	name: string;
	tabs: SettingsTab[];
}

export class SettingsModal extends Modal {
	plugin: TaskProgressBarPlugin;
	private currentTab: string | null = null;
	private debouncedApplySettings: () => void;
	private debouncedApplyNotifications: () => void;
	private sidebarEl: HTMLElement;
	private modalContentEl: HTMLElement;
	private searchInput: HTMLInputElement;
	private mobileTitleEl: HTMLElement | null = null;
	private searchIndex: SearchResultItem[] | null = null;
	private mobileSearchResultsEl: HTMLElement | null = null;
	private debouncedSearch: () => void;

	private tabs: SettingsTab[] = [
		// Core Settings
		{
			id: "index",
			name: t("Index & Sources"),
			icon: "database",
			category: "core",
		},
		{
			id: "view-settings",
			name: t("Views"),
			icon: "layout",
			category: "core",
		},
		{
			id: "interface",
			name: t("Interface"),
			icon: "layout-dashboard",
			category: "core",
		},
		{
			id: "file-filter",
			name: t("File Filter"),
			icon: "folder-x",
			category: "core",
		},

		// Display & Progress
		{
			id: "progress-bar",
			name: t("Progress Display"),
			icon: "trending-up",
			category: "display",
		},
		{
			id: "task-status",
			name: t("Checkbox Status"),
			icon: "checkbox-glyph",
			category: "display",
		},

		// Task Management
		{
			id: "task-handler",
			name: t("Task Handler"),
			icon: "list-checks",
			category: "management",
		},
		{
			id: "task-filter",
			name: t("Task Filter"),
			icon: "filter",
			category: "management",
		},
		{
			id: "project",
			name: t("Projects"),
			icon: "folder-open",
			category: "management",
		},

		// Workflow & Automation
		{
			id: "workflow",
			name: t("Workflows"),
			icon: "git-branch",
			category: "workflow",
		},
		{
			id: "date-priority",
			name: t("Dates & Priority"),
			icon: "calendar-clock",
			category: "workflow",
		},
		{
			id: "quick-capture",
			name: t("Quick Capture"),
			icon: "zap",
			category: "workflow",
		},
		{
			id: "task-timer",
			name: t("Task Timer"),
			icon: "timer",
			category: "workflow",
		},
		{
			id: "time-parsing",
			name: t("Time Parsing"),
			icon: "clock",
			category: "workflow",
		},
		{
			id: "timeline-sidebar",
			name: t("Timeline Sidebar"),
			icon: "clock",
			category: "workflow",
		},

		// Gamification
		{
			id: "reward",
			name: t("Rewards"),
			icon: "gift",
			category: "gamification",
		},
		{
			id: "habit",
			name: t("Habits"),
			icon: "repeat",
			category: "gamification",
		},

		// Integration & Advanced
		{
			id: "calendar-views",
			name: t("Calendars"),
			icon: "calendar-range",
			category: "display",
		},
		{
			id: "ics-integration",
			name: t("Calendar Sync"),
			icon: "calendar-plus",
			category: "integration",
		},
		{
			id: "desktop-integration",
			name: t("Desktop Integration"),
			icon: "monitor",
			category: "integration",
		},
		{
			id: "mcp-integration",
			name: t("MCP Integration"),
			icon: "network",
			category: "integration",
		},
		{
			id: "bases-support",
			name: t("Bases Support"),
			icon: "layout",
			category: "integration",
		},
		{
			id: "workspaces",
			name: t("Workspaces"),
			icon: "layers",
			category: "integration",
		},
		{
			id: "beta-test",
			name: t("Beta Features"),
			icon: "flask-conical",
			category: "advanced",
		},
		{ id: "about", name: t("About"), icon: "info", category: "info" },
	];

	private categories: { [key: string]: string } = {
		core: t("Core Settings"),
		display: t("Display & Progress"),
		management: t("Task Management"),
		workflow: t("Workflow & Automation"),
		gamification: t("Gamification"),
		integration: t("Integration"),
		advanced: t("Advanced"),
		info: t("Information"),
	};

	constructor(app: App, plugin: TaskProgressBarPlugin) {
		super(app);
		this.plugin = plugin;

		// Initialize debounced functions
		this.debouncedApplySettings = debounce(
			async () => {
				await plugin.saveSettings();

				if (plugin.dataflowOrchestrator) {
					await plugin.dataflowOrchestrator.updateSettings(
						plugin.settings,
					);
				}

				await plugin.notificationManager?.reloadSettings();
				await plugin.triggerViewUpdate();
			},
			100,
			true,
		);

		this.debouncedApplyNotifications = debounce(
			async () => {
				await plugin.saveSettings();
				await plugin.notificationManager?.reloadSettings();
			},
			100,
			true,
		);

		this.debouncedSearch = debounce(() => this.handleSearch(), 300, false);
	}

	applySettingsUpdate() {
		this.debouncedApplySettings();
	}

	applyNotificationsUpdateLight() {
		this.debouncedApplyNotifications();
	}

	onOpen() {
		const { modalEl, contentEl } = this;
		modalEl.toggleClass(["mod-settings", "mod-sidebar-layout"], true);
		contentEl.empty();
		contentEl.addClass("vertical-tabs-container");

		// Store title element reference for mobile
		this.mobileTitleEl = modalEl.querySelector(
			".modal-title",
		) as HTMLElement;

		// On mobile, start with navigation view (no tab selected)
		// On desktop, default to first tab
		// Set currentTab BEFORE rendering sidebar so is-active is correct
		if (Platform.isMobile) {
			this.currentTab = null;
		} else {
			this.currentTab = "index";
		}

		// Create sidebar (vertical-tab-header)
		this.sidebarEl = contentEl.createDiv({ cls: "vertical-tab-header" });
		this.renderSidebar();

		// Create content area (vertical-tab-content-container)
		this.modalContentEl = contentEl.createDiv({
			cls: "vertical-tab-content-container",
		});

		// Render content and update mobile view
		if (Platform.isMobile) {
			this.updateMobileView();
		} else {
			this.renderContent();
		}

		// Build search index in background
		setTimeout(() => this.buildSearchIndex(), 500);
	}

	onClose() {
		this.modalContentEl?.empty();
		if (this.mobileSearchResultsEl) {
			this.mobileSearchResultsEl.remove();
			this.mobileSearchResultsEl = null;
		}
	}

	private renderSidebar() {
		this.sidebarEl.empty();

		// Search container
		const searchContainer = this.sidebarEl.createDiv({
			cls: "vertical-tab-header-search",
		});
		this.searchInput = searchContainer.createEl("input", {
			type: "text",
			placeholder: t("Search settings..."),
		});
		this.searchInput.addEventListener("input", () =>
			this.debouncedSearch(),
		);

		// Navigation groups
		this.renderNavigation(this.sidebarEl);
	}

	private renderNavigation(container: HTMLElement) {
		// Group tabs by category
		const groupedTabs: { [key: string]: SettingsTab[] } = {};
		this.tabs.forEach((tab) => {
			// Skip MCP tab on non-desktop platforms
			if (tab.id === "mcp-integration" && !Platform.isDesktopApp) {
				return;
			}
			// Skip bases if API version doesn't support it
			if (tab.id === "bases-support" && !requireApiVersion("1.9.10")) {
				return;
			}

			if (!groupedTabs[tab.category]) {
				groupedTabs[tab.category] = [];
			}
			groupedTabs[tab.category].push(tab);
		});

		// Render categories in order
		const categoryOrder = [
			"core",
			"display",
			"management",
			"workflow",
			"gamification",
			"integration",
			"advanced",
			"info",
		];

		categoryOrder.forEach((categoryId) => {
			const tabs = groupedTabs[categoryId];
			if (!tabs || tabs.length === 0) return;

			const groupEl = container.createDiv({
				cls: "vertical-tab-header-group",
			});
			groupEl.createDiv({
				cls: "vertical-tab-header-group-title",
				text: this.categories[categoryId],
			});

			const itemsContainer = groupEl.createDiv({
				cls: "vertical-tab-header-group-items",
			});

			tabs.forEach((tab) => {
				const item = itemsContainer.createDiv({
					cls: `vertical-tab-nav-item tappable ${this.currentTab === tab.id ? "is-active" : ""}`,
				});
				item.setAttribute("data-tab-id", tab.id);

				const icon = item.createDiv({
					cls: "vertical-tab-nav-item-icon",
				});
				setIcon(icon, tab.icon);

				item.createDiv({
					cls: "vertical-tab-nav-item-title",
					text: tab.name,
				});

				const chevron = item.createDiv({
					cls: "vertical-tab-nav-item-chevron",
				});
				setIcon(chevron, "chevron-right");

				item.addEventListener("click", () => {
					this.switchToTab(tab.id);
				});
			});
		});
	}

	private switchToTab(tabId: string) {
		this.currentTab = tabId;

		// Update active state in sidebar
		const items = this.sidebarEl.querySelectorAll(".vertical-tab-nav-item");
		items.forEach((item) => {
			if (item.getAttribute("data-tab-id") === tabId) {
				item.addClass("is-active");
			} else {
				item.removeClass("is-active");
			}
		});

		// Re-render content
		this.renderContent();

		// Update mobile view
		if (Platform.isMobile) {
			this.updateMobileView();
		}
	}

	private goBackToNavigation() {
		this.currentTab = null;

		// Clear active state in sidebar
		const items = this.sidebarEl.querySelectorAll(".vertical-tab-nav-item");
		items.forEach((item) => {
			item.removeClass("is-active");
		});

		// Clear content
		this.modalContentEl.empty();

		// Update mobile view
		this.updateMobileView();
	}

	private updateMobileView() {
		if (!Platform.isMobile) return;

		const isShowingContent = this.currentTab !== null;

		// Toggle visibility: show sidebar or content
		this.sidebarEl.toggleClass("is-mobile-hidden", isShowingContent);
		this.modalContentEl.toggleClass("is-mobile-hidden", !isShowingContent);

		// Update modal title
		if (this.mobileTitleEl) {
			this.mobileTitleEl.empty();

			if (isShowingContent) {
				// Show back button and tab name
				const backBtn = this.mobileTitleEl.createDiv({
					cls: "clickable-icon modal-setting-back-button mod-raised",
				});
				setIcon(backBtn, "arrow-left");
				backBtn.addEventListener("click", () => {
					this.goBackToNavigation();
				});

				const tab = this.tabs.find((t) => t.id === this.currentTab);
				this.mobileTitleEl.createSpan({
					text: tab?.name || "Settings",
				});
			} else {
				// Show plugin name only
				this.mobileTitleEl.createSpan({ text: "Task Genius" });
			}
		}
	}

	private renderContent() {
		this.modalContentEl.empty();

		const tab = this.tabs.find((t) => t.id === this.currentTab);
		if (!tab) return;

		// Content wrapper
		const contentEl = this.modalContentEl.createDiv({
			cls: "vertical-tab-content",
		});

		// Header
		const header = contentEl.createDiv({
			cls: "vertical-tab-content-header",
		});

		// Title (left side)
		const titleEl = header.createEl("h2", {
			cls: "vertical-tab-content-header-title",
		});
		const titleIcon = titleEl.createSpan({
			cls: "vertical-tab-content-header-icon",
		});
		setIcon(titleIcon, tab.icon);
		titleEl.createSpan({ text: tab.name });

		// Actions (right side)
		const actions = header.createDiv({
			cls: "vertical-tab-content-header-actions",
		});
		const howToBtn = new ButtonComponent(actions);
		howToBtn.setClass("how-to-button");
		howToBtn.onClick(() => {
			const url = this.getHowToUseUrl(tab.id);
			try {
				new IframeModal(
					this.app,
					url,
					`${t("How to use")} — ${tab.name}`,
				).open();
			} catch {
				window.open(url);
			}
		});

		const howToIcon = howToBtn.buttonEl.createSpan();
		setIcon(howToIcon, "book");
		howToBtn.buttonEl.createSpan({ text: t("How to use") });

		// Description (full width, below title and actions)
		const desc = this.getTabDescription(tab.id);
		if (desc) {
			header.createEl("p", {
				cls: "vertical-tab-content-header-desc",
				text: desc,
			});
		}

		// Body
		const body = contentEl.createDiv({
			cls: "vertical-tab-content-body",
		});
		this.renderTabContent(tab.id, body);
	}

	private renderTabContent(tabId: string, container: HTMLElement) {
		switch (tabId) {
			case "progress-bar":
				renderProgressSettingsTab(this as any, container);
				break;
			case "task-status":
				renderTaskStatusSettingsTab(this as any, container);
				break;
			case "date-priority":
				renderDatePrioritySettingsTab(this as any, container);
				break;
			case "task-filter":
				renderTaskFilterSettingsTab(this as any, container);
				break;
			case "file-filter":
				renderFileFilterSettingsTab(this as any, container);
				break;
			case "workflow":
				renderWorkflowSettingsTab(this as any, container);
				break;
			case "quick-capture":
				renderQuickCaptureSettingsTab(this as any, container);
				break;
			case "task-timer":
				renderTaskTimerSettingTab(this as any, container);
				break;
			case "time-parsing":
				renderTimeParsingSettingsTab(this as any, container);
				break;
			case "timeline-sidebar":
				renderTimelineSidebarSettingsTab(this as any, container);
				break;
			case "task-handler":
				renderTaskHandlerSettingsTab(this as any, container);
				break;
			case "view-settings":
				renderViewSettingsTab(this as any, container);
				break;
			case "interface":
				renderInterfaceSettingsTab(this as any, container);
				break;
			case "index":
				renderIndexSettingsTab(this as any, container);
				break;
			case "project":
				renderProjectSettingsTab(this as any, container);
				break;
			case "calendar-views":
				renderCalendarViewSettingsTab(this as any, container);
				break;
			case "ics-integration":
				// Use new CalendarSettingsComponent with Google/Outlook OAuth support
				const calendarComponent = new CalendarSettingsComponent(
					this.plugin,
					container,
					() => this.switchToTab("progress-bar"),
				);
				calendarComponent.display();
				break;
			case "desktop-integration":
				renderDesktopIntegrationSettingsTab(this as any, container);
				break;
			case "mcp-integration":
				if (Platform.isDesktopApp) {
					renderMcpIntegrationSettingsTab(
						this as any,
						container,
						this.plugin,
						() => this.applySettingsUpdate(),
					);
				}
				break;
			case "bases-support":
				if (requireApiVersion("1.9.10")) {
					renderBasesSettingsTab(this as any, container);
				}
				break;
			case "workspaces":
				renderWorkspaceSettingsTab(this as any, container);
				break;
			case "reward":
				renderRewardSettingsTab(this as any, container);
				break;
			case "habit":
				renderHabitSettingsTab(this as any, container);
				break;
			case "beta-test":
				renderBetaTestSettingsTab(this as any, container);
				break;
			case "about":
				renderAboutSettingsTab(this as any, container);
				break;
			default:
				container.createDiv({
					cls: "vertical-tab-content-empty",
					text: t("Select a setting category from the sidebar."),
				});
		}
	}

	private getTabDescription(tabId: string): string {
		const descriptions: { [key: string]: string } = {
			"progress-bar": t(
				"Configure how progress bars are displayed for tasks and parent items.",
			),
			"task-status": t("Customize checkbox states and their appearance."),
			"date-priority": t("Configure date formats and priority handling."),
			"task-filter": t(
				"Set up rules to filter which tasks are displayed.",
			),
			"file-filter": t(
				"Configure which files and folders to include or exclude.",
			),
			workflow: t(
				"Define workflows with stages for managing task lifecycles.",
			),
			"quick-capture": t("Configure quick task capture settings."),
			"task-timer": t("Set up task timing and tracking options."),
			"time-parsing": t("Configure how time expressions are parsed."),
			"timeline-sidebar": t("Customize the timeline sidebar view."),
			"task-handler": t("Configure task gutter and handling behaviors."),
			"view-settings": t("Manage custom views and their configurations."),
			interface: t("Customize the user interface appearance."),
			index: t("Configure task indexing and data sources."),
			project: t("Set up project detection and management."),
			"calendar-views": t("Configure calendar view settings."),
			"ics-integration": t(
				"Connect external calendars to display events alongside your tasks. Supports ICS/iCal URLs, Google Calendar, Outlook, and Apple iCloud.",
			),
			"desktop-integration": t(
				"Configure desktop notifications and tray.",
			),
			"mcp-integration": t(
				"Configure Model Context Protocol integration.",
			),
			"bases-support": t("Configure Bases plugin integration."),
			workspaces: t("Manage workspace configurations."),
			reward: t("Configure the rewards and points system."),
			habit: t("Set up habit tracking."),
			"beta-test": t("Enable experimental features."),
			about: t("Information about this plugin."),
		};
		return descriptions[tabId] || "";
	}

	private getHowToUseUrl(tabId: string): string {
		const base = "https://taskgenius.md/docs";
		const urlMap: { [key: string]: string } = {
			index: `${base}/task-view/indexer`,
			"view-settings": `${base}/task-view`,
			interface: `${base}/interface`,
			"file-filter": `${base}/file-filter`,
			"progress-bar": `${base}/progress-bars`,
			"task-status": `${base}/task-status`,
			"task-handler": `${base}/task-gutter`,
			"task-filter": `${base}/filtering`,
			project: `${base}/project`,
			"date-priority": `${base}/date-priority`,
			"quick-capture": `${base}/quick-capture`,
			"task-timer": `${base}/task-timer`,
			"time-parsing": `${base}/time-parsing`,
			workflow: `${base}/workflows`,
			"timeline-sidebar": `${base}/task-view/timeline-sidebar-view`,
			reward: `${base}/reward`,
			habit: `${base}/habit`,
			"calendar-views": `${base}/task-view/calendar`,
			"ics-integration": `${base}/ics-support`,
			"mcp-integration": `${base}/mcp-integration`,
			"bases-support": `${base}/bases-support`,
			"desktop-integration": `${base}/bases-support`,
			workspaces: `${base}/workspaces`,
		};
		return urlMap[tabId] || `${base}/getting-started`;
	}

	private handleSearch() {
		const query = this.searchInput.value.toLowerCase().trim();
		const items = this.sidebarEl.querySelectorAll(".vertical-tab-nav-item");

		// Ensure search index is built before searching
		if (!this.searchIndex) {
			this.buildSearchIndex();
		}

		// Build a set of tabIds that have matching settings
		const tabsWithMatchingSettings = new Set<string>();
		if (query && this.searchIndex) {
			this.searchIndex.forEach((item) => {
				if (
					item.name.toLowerCase().includes(query) ||
					item.desc.toLowerCase().includes(query)
				) {
					tabsWithMatchingSettings.add(item.tabId);
				}
			});
		}

		// Filter sidebar items - show if tab name matches OR has matching settings
		items.forEach((item: HTMLElement) => {
			const label =
				item
					.querySelector(".vertical-tab-nav-item-title")
					?.textContent?.toLowerCase() || "";
			const tabId = item.getAttribute("data-tab-id") || "";
			const desc = this.getTabDescription(tabId).toLowerCase();

			// Match by tab name/desc OR by having matching settings inside
			const matchesByTabInfo =
				label.includes(query) ||
				desc.includes(query) ||
				tabId.includes(query);
			const hasMatchingSettings = tabsWithMatchingSettings.has(tabId);
			const matches = matchesByTabInfo || hasMatchingSettings;

			if (query === "" || matches) {
				item.removeClass("is-hidden");
				if (matches && query !== "") {
					item.addClass("is-search-match");
				} else {
					item.removeClass("is-search-match");
				}
			} else {
				item.addClass("is-hidden");
				item.removeClass("is-search-match");
			}
		});

		// Show/hide category headers based on whether they have visible items
		const groups = this.sidebarEl.querySelectorAll(
			".vertical-tab-header-group",
		);
		groups.forEach((group: HTMLElement) => {
			const visibleItems = group.querySelectorAll(
				".vertical-tab-nav-item:not(.is-hidden)",
			);
			if (visibleItems.length === 0) {
				group.style.display = "none";
			} else {
				group.style.display = "";
			}
		});

		// Handle deep search results display
		if (!query) {
			// No query: restore default view
			if (Platform.isMobile) {
				if (this.mobileSearchResultsEl) {
					this.mobileSearchResultsEl.hide();
				}
			} else {
				// Desktop: restore current tab content
				this.renderContent();
			}
			return;
		}

		// Perform deep search on settings items
		if (this.searchIndex && this.searchIndex.length > 0) {
			const results = this.searchIndex.filter(
				(item) =>
					item.name.toLowerCase().includes(query) ||
					item.desc.toLowerCase().includes(query) ||
					item.tabName.toLowerCase().includes(query),
			);
			this.renderSearchResults(results);
		}
	}

	/**
	 * Open modal with specific tab
	 */
	public openTab(tabId: string) {
		this.currentTab = tabId;
		if (this.modalContentEl) {
			this.renderContent();
			// Update sidebar active state
			const items = this.sidebarEl.querySelectorAll(
				".vertical-tab-nav-item",
			);
			items.forEach((item) => {
				if (item.getAttribute("data-tab-id") === tabId) {
					item.addClass("is-active");
				} else {
					item.removeClass("is-active");
				}
			});

			// Update mobile view
			if (Platform.isMobile) {
				this.updateMobileView();
			}
		}
	}

	/**
	 * Build search index by rendering all tabs to extract setting items
	 */
	private buildSearchIndex() {
		if (this.searchIndex) return;
		this.searchIndex = [];

		for (const tab of this.tabs) {
			// Skip tabs that are not available on current platform
			if (tab.id === "mcp-integration" && !Platform.isDesktopApp) {
				continue;
			}
			if (tab.id === "bases-support" && !requireApiVersion("1.9.10")) {
				continue;
			}

			// Create temporary container
			const tabContainer = document.createElement("div");

			// Render tab content to extract settings
			try {
				this.renderTabContent(tab.id, tabContainer);
			} catch (e) {
				console.warn(`Failed to index tab ${tab.id}`, e);
				continue;
			}

			// Extract all setting items
			const settingItems = tabContainer.querySelectorAll(".setting-item");
			settingItems.forEach((item, index) => {
				const nameEl = item.querySelector(".setting-item-name");
				const descEl = item.querySelector(".setting-item-description");
				const name = nameEl?.textContent || "";
				const desc = descEl?.textContent || "";

				if (name) {
					this.searchIndex!.push({
						id: `${tab.id}-${index}-${name}`,
						tabId: tab.id,
						tabName: tab.name,
						name: name,
						desc: desc,
						category: this.categories[tab.category] || tab.category,
					});
				}
			});

			// Cleanup
			tabContainer.remove();
		}
	}

	/**
	 * Render search results in right panel (desktop) or bottom sheet (mobile)
	 */
	private renderSearchResults(results: SearchResultItem[]) {
		let container: HTMLElement;

		if (Platform.isMobile) {
			// Mobile: Bottom sheet
			if (!this.mobileSearchResultsEl) {
				this.mobileSearchResultsEl = this.modalEl.createDiv({
					cls: "tg-settings-search-results tg-mobile-search-sheet",
				});
				this.mobileSearchResultsEl.setCssStyles({
					position: "fixed",
					bottom: "0",
					left: "0",
					right: "0",
					maxHeight: "50vh",
					overflowY: "auto",
					background: "var(--background-primary)",
					borderTop: "1px solid var(--background-modifier-border)",
					zIndex: "100",
					paddingBottom: "env(safe-area-inset-bottom)",
					boxShadow: "0 -4px 16px rgba(0, 0, 0, 0.15)",
					borderRadius: "12px 12px 0 0",
				});
			}
			container = this.mobileSearchResultsEl;
			container.show();
		} else {
			// Desktop: Replace right panel content
			this.modalContentEl.empty();
			const wrapper = this.modalContentEl.createDiv({
				cls: "tg-settings-search-results-wrapper",
			});
			wrapper.setCssStyles({
				padding: "20px",
				height: "100%",
				overflowY: "auto",
			});

			const header = wrapper.createDiv({
				cls: "tg-settings-search-header",
			});
			header.setCssStyles({
				marginBottom: "16px",
				display: "flex",
				alignItems: "center",
				justifyContent: "space-between",
			});

			header
				.createEl("h3", {
					text: t("Search Results"),
				})
				.setCssStyles({
					margin: "0",
					fontWeight: "600",
				});

			header
				.createSpan({
					text: `${results.length} ${t("results")}`,
					cls: "tg-search-count",
				})
				.setCssStyles({
					color: "var(--text-muted)",
					fontSize: "var(--font-ui-small)",
				});

			// Reuse search results styles but override position
			container = wrapper.createDiv({
				cls: "tg-settings-search-results",
			});
			container.setCssStyles({
				position: "static",
				boxShadow: "none",
				border: "none",
				maxHeight: "none",
				background: "transparent",
			});
		}

		container.empty();

		if (results.length === 0) {
			const noResult = container.createDiv({
				cls: "tg-settings-search-no-result",
			});
			noResult.createSpan({ text: t("No settings found") });
			return;
		}

		results.forEach((result) => {
			const itemEl = container.createDiv({
				cls: "tg-settings-search-result",
			});

			const metaEl = itemEl.createDiv({
				cls: "tg-settings-search-result-meta",
			});
			metaEl.createSpan({
				cls: "tg-settings-search-result-category",
				text: result.category,
			});
			const tabLabel = metaEl.createSpan({ text: result.tabName });
			tabLabel.setCssStyles({
				fontSize: "0.8em",
				color: "var(--text-muted)",
				marginLeft: "8px",
			});

			itemEl.createDiv({
				cls: "tg-settings-search-result-name",
				text: result.name,
			});

			if (result.desc) {
				itemEl.createDiv({
					cls: "tg-settings-search-result-desc",
					text: result.desc,
				});
			}

			itemEl.addEventListener("click", () => {
				this.highlightSetting(result.tabId, result.name);
				// Hide search results on mobile after click
				if (Platform.isMobile && this.mobileSearchResultsEl) {
					this.mobileSearchResultsEl.hide();
				}
				// Clear search input and restore sidebar
				this.searchInput.value = "";
				this.resetSidebarFilter();
			});
		});
	}

	/**
	 * Reset sidebar filter to show all items
	 */
	private resetSidebarFilter() {
		const items = this.sidebarEl.querySelectorAll(".vertical-tab-nav-item");
		items.forEach((item: HTMLElement) => {
			item.removeClass("is-hidden");
			item.removeClass("is-search-match");
		});

		const groups = this.sidebarEl.querySelectorAll(
			".vertical-tab-header-group",
		);
		groups.forEach((group: HTMLElement) => {
			group.style.display = "";
		});
	}

	/**
	 * Navigate to tab and highlight specific setting
	 */
	private highlightSetting(tabId: string, settingName: string) {
		this.openTab(tabId);

		// Wait for DOM update then find and highlight element
		setTimeout(() => {
			const settingItems =
				this.modalContentEl.querySelectorAll(".setting-item");
			for (let i = 0; i < settingItems.length; i++) {
				const item = settingItems[i] as HTMLElement;
				const nameEl = item.querySelector(".setting-item-name");
				if (nameEl && nameEl.textContent === settingName) {
					item.scrollIntoView({
						behavior: "smooth",
						block: "center",
					});
					item.addClass("tg-settings-search-highlight");
					// Remove highlight after animation
					setTimeout(
						() => item.removeClass("tg-settings-search-highlight"),
						2000,
					);
					break;
				}
			}
		}, 150);
	}
}
