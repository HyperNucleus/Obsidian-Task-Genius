import {
	Setting,
	Notice,
	setIcon,
	ButtonComponent,
	ExtraButtonComponent,
} from "obsidian";
import { TaskProgressBarSettingTab } from "@/setting";
import { CustomCalendarViewConfig } from "@/common/setting-definition";
import { t } from "@/translations/helper";
import { CalendarViewConfigModal } from "@/components/ui/modals/CalendarViewConfigModal";
import Sortable from "sortablejs";
import "@/styles/calendar-view-settings.scss";

/**
 * Built-in calendar view templates that users can use as base
 */
const BUILTIN_VIEW_TEMPLATES = [
	{
		type: "month" as const,
		name: t("Month View"),
		icon: "calendar",
		description: t("Display tasks in a monthly calendar grid"),
	},
	{
		type: "week" as const,
		name: t("Week View"),
		icon: "calendar-range",
		description: t("Display tasks in a weekly time grid with hourly slots"),
	},
	{
		type: "day" as const,
		name: t("Day View"),
		icon: "calendar-days",
		description: t(
			"Display tasks for a single day with detailed time slots",
		),
	},
	{
		type: "agenda" as const,
		name: t("Agenda View"),
		icon: "list",
		description: t(
			"Display upcoming tasks in a list format grouped by day",
		),
	},
	{
		type: "year" as const,
		name: t("Year View"),
		icon: "calendar-clock",
		description: t("Display a full year with 12 mini-month calendars"),
	},
];

/**
 * Render the Calendar View Settings Tab
 */
export function renderCalendarViewSettingsTab(
	settingTab: TaskProgressBarSettingTab,
	containerEl: HTMLElement,
) {
	// Header
	new Setting(containerEl)
		.setName(t("Calendar View Configuration"))
		.setDesc(
			t(
				"Create custom calendar views based on Month, Week, or Day views. Each custom view can have its own display settings.",
			),
		)
		.setHeading();

	// Built-in Templates Section
	new Setting(containerEl)
		.setName(t("Built-in View Templates"))
		.setDesc(
			t(
				"Use these templates as a starting point to create your own custom calendar views.",
			),
		)
		.setHeading();

	const templatesContainer = containerEl.createDiv({
		cls: "calendar-templates-container",
	});

	BUILTIN_VIEW_TEMPLATES.forEach((template) => {
		const templateCard = templatesContainer.createDiv({
			cls: "calendar-template-card",
		});

		// Icon
		const iconEl = templateCard.createDiv({
			cls: "calendar-template-icon",
		});
		setIcon(iconEl, template.icon);

		// Info
		const infoEl = templateCard.createDiv({
			cls: "calendar-template-info",
		});
		infoEl.createDiv({
			cls: "calendar-template-name",
			text: template.name,
		});
		infoEl.createDiv({
			cls: "calendar-template-description",
			text: template.description,
		});

		// Actions
		const actionsEl = templateCard.createDiv({
			cls: "calendar-template-actions",
		});

		const createBtn = new ExtraButtonComponent(actionsEl);
		createBtn.setTooltip(t("Create Custom View"));
		createBtn.setIcon("plus");
		createBtn.onClick(() => {
			openCreateViewModal(settingTab, template.type, () => {
				renderCustomViewsList(settingTab, customViewsContainer);
			});
		});
	});

	// Custom Views Section
	new Setting(containerEl)
		.setName(t("Custom Calendar Views"))
		.setDesc(
			t(
				"Your custom calendar views. Drag to reorder, toggle visibility, or edit settings.",
			),
		)
		.setHeading();

	const customViewsContainer = containerEl.createDiv({
		cls: "custom-calendar-views-container",
	});

	// Render the list
	renderCustomViewsList(settingTab, customViewsContainer);

	// Add New View Button
	const addBtnContainer = containerEl.createDiv({
		cls: "calendar-add-view-container",
	});

	new Setting(addBtnContainer).addButton((button) => {
		button
			.setButtonText(t("Create New Calendar View"))
			.setCta()
			.setIcon("plus")
			.onClick(() => {
				openCreateViewModal(settingTab, "month", () => {
					renderCustomViewsList(settingTab, customViewsContainer);
				});
			});
	});
}

/**
 * Render the list of custom calendar views
 */
function renderCustomViewsList(
	settingTab: TaskProgressBarSettingTab,
	container: HTMLElement,
) {
	container.empty();

	const customViews = settingTab.plugin.settings.customCalendarViews || [];

	if (customViews.length === 0) {
		const emptyState = container.createDiv({ cls: "calendar-views-empty" });
		const emptyIcon = emptyState.createDiv({
			cls: "calendar-views-empty-icon",
		});
		setIcon(emptyIcon, "calendar-plus");
		emptyState.createDiv({
			cls: "calendar-views-empty-text",
			text: t("No custom calendar views yet"),
		});
		emptyState.createDiv({
			cls: "calendar-views-empty-hint",
			text: t(
				"Create a custom view from the templates above to get started.",
			),
		});
		return;
	}

	const listContainer = container.createDiv({
		cls: "calendar-views-list sortable-calendar-views",
	});

	// Render each custom view
	customViews.forEach((view) => {
		createCustomViewItem(settingTab, listContainer, view, () => {
			renderCustomViewsList(settingTab, container);
		});
	});

	// Setup sortable
	Sortable.create(listContainer, {
		animation: 150,
		handle: ".calendar-view-drag-handle",
		ghostClass: "sortable-ghost",
		chosenClass: "sortable-chosen",
		dragClass: "sortable-drag",
		onEnd: async (evt) => {
			const views = settingTab.plugin.settings.customCalendarViews || [];
			const [movedItem] = views.splice(evt.oldIndex!, 1);
			views.splice(evt.newIndex!, 0, movedItem);

			// Update order values
			views.forEach((v, index) => {
				v.order = index;
				v.updatedAt = Date.now();
			});

			settingTab.plugin.settings.customCalendarViews = views;
			await settingTab.plugin.saveSettings();

			// Notify calendar component
			(settingTab.app.workspace as any).trigger(
				"task-genius:calendar-views-changed",
				{ reason: "order-changed" },
			);
		},
	});
}

/**
 * Create a single custom view item in the list
 */
function createCustomViewItem(
	settingTab: TaskProgressBarSettingTab,
	container: HTMLElement,
	view: CustomCalendarViewConfig,
	onUpdate: () => void,
) {
	const viewEl = container.createDiv({
		cls: `calendar-view-item ${!view.enabled ? "is-disabled" : ""}`,
		attr: { "data-view-id": view.id },
	});

	// Drag handle
	const dragHandle = viewEl.createDiv({ cls: "calendar-view-drag-handle" });
	setIcon(dragHandle, "grip-vertical");

	// Icon
	const iconEl = viewEl.createDiv({ cls: "calendar-view-icon" });
	setIcon(iconEl, view.icon);

	// Info
	const infoEl = viewEl.createDiv({ cls: "calendar-view-info" });
	infoEl.createDiv({ cls: "calendar-view-name", text: view.name });

	const metaEl = infoEl.createDiv({ cls: "calendar-view-meta" });
	const baseViewLabel = {
		month: t("Month"),
		week: t("Week"),
		day: t("Day"),
		agenda: t("Agenda"),
		year: t("Year"),
	}[view.baseViewType];
	metaEl.createSpan({
		cls: "calendar-view-base-type",
		text: t("Based on: ") + baseViewLabel,
	});

	// Config summary
	const configSummary = getConfigSummary(view);
	if (configSummary) {
		metaEl.createSpan({
			cls: "calendar-view-config-summary",
			text: configSummary,
		});
	}

	// Actions
	const actionsEl = viewEl.createDiv({ cls: "calendar-view-actions" });

	// Toggle enabled
	const toggleBtn = actionsEl.createEl("button", {
		cls: ["calendar-view-action-btn", "clickable-icon"],
		attr: { "aria-label": view.enabled ? t("Disable") : t("Enable") },
	});
	setIcon(toggleBtn, view.enabled ? "eye" : "eye-off");
	toggleBtn.onclick = async () => {
		view.enabled = !view.enabled;
		view.updatedAt = Date.now();
		await settingTab.plugin.saveSettings();
		onUpdate();
		(settingTab.app.workspace as any).trigger(
			"task-genius:calendar-views-changed",
			{ reason: "visibility-changed", viewId: view.id },
		);
	};

	// Edit
	const editBtn = actionsEl.createEl("button", {
		cls: ["calendar-view-action-btn", "clickable-icon"],
		attr: { "aria-label": t("Edit") },
	});
	setIcon(editBtn, "pencil");
	editBtn.onclick = () => {
		openEditViewModal(settingTab, view, onUpdate);
	};

	// Duplicate
	const duplicateBtn = actionsEl.createEl("button", {
		cls: ["calendar-view-action-btn", "clickable-icon"],
		attr: { "aria-label": t("Duplicate") },
	});
	setIcon(duplicateBtn, "copy");
	duplicateBtn.onclick = async () => {
		const newView: CustomCalendarViewConfig = {
			...JSON.parse(JSON.stringify(view)),
			id: `custom-calendar-${Date.now()}`,
			name: view.name + " " + t("(Copy)"),
			createdAt: Date.now(),
			updatedAt: Date.now(),
		};

		const views = settingTab.plugin.settings.customCalendarViews || [];
		views.push(newView);
		settingTab.plugin.settings.customCalendarViews = views;
		await settingTab.plugin.saveSettings();

		new Notice(t("View duplicated: ") + newView.name);
		onUpdate();

		(settingTab.app.workspace as any).trigger(
			"task-genius:calendar-views-changed",
			{ reason: "view-added", viewId: newView.id },
		);
	};

	// Delete
	const deleteBtn = actionsEl.createEl("button", {
		cls: [
			"calendar-view-action-btn",
			"calendar-view-action-delete",
			"clickable-icon",
		],
		attr: { "aria-label": t("Delete") },
	});
	setIcon(deleteBtn, "trash");
	deleteBtn.onclick = async () => {
		const views = settingTab.plugin.settings.customCalendarViews || [];
		const index = views.findIndex((v) => v.id === view.id);
		if (index !== -1) {
			views.splice(index, 1);
			settingTab.plugin.settings.customCalendarViews = views;
			await settingTab.plugin.saveSettings();

			new Notice(t("View deleted: ") + view.name);
			onUpdate();

			(settingTab.app.workspace as any).trigger(
				"task-genius:calendar-views-changed",
				{ reason: "view-deleted", viewId: view.id },
			);
		}
	};
}

/**
 * Get a summary of the view configuration for display
 */
function getConfigSummary(view: CustomCalendarViewConfig): string {
	const parts: string[] = [];
	const config = view.calendarConfig;

	if (config.dayFilter?.type === "hideWeekends") {
		parts.push(t("No weekends"));
	} else if (config.dayFilter?.type === "hideWeekdays") {
		parts.push(t("Weekends only"));
	} else if (
		config.dayFilter?.type === "customDays" &&
		config.dayFilter.hiddenDays?.length
	) {
		parts.push(t("Custom days"));
	}

	if (config.timeFilter?.enabled) {
		parts.push(
			`${config.timeFilter.startHour}:00-${config.timeFilter.endHour}:00`,
		);
	}

	if (config.firstDayOfWeek === 1) {
		parts.push(t("Mon first"));
	} else if (config.firstDayOfWeek === 6) {
		parts.push(t("Sat first"));
	}

	return parts.join(" · ");
}

/**
 * Open the modal to create a new custom view
 */
function openCreateViewModal(
	settingTab: TaskProgressBarSettingTab,
	baseViewType: "month" | "week" | "day" | "agenda" | "year",
	onSave: () => void,
) {
	const newView: CustomCalendarViewConfig = {
		id: `custom-calendar-${Date.now()}`,
		name: t("New Calendar View"),
		icon: "calendar",
		baseViewType,
		enabled: true,
		order: (settingTab.plugin.settings.customCalendarViews || []).length,
		calendarConfig: {
			firstDayOfWeek: undefined,
			showWeekNumbers: false,
			showEventCounts: true,
			dayFilter: { type: "none" },
			timeFilter: {
				enabled: false,
				type: "workingHours",
				startHour: 9,
				endHour: 18,
			},
		},
		createdAt: Date.now(),
		updatedAt: Date.now(),
	};

	new CalendarViewConfigModal(
		settingTab.app,
		settingTab.plugin,
		newView,
		async (savedView) => {
			const views = settingTab.plugin.settings.customCalendarViews || [];
			views.push(savedView);
			settingTab.plugin.settings.customCalendarViews = views;
			await settingTab.plugin.saveSettings();

			new Notice(t("Calendar view created: ") + savedView.name);
			onSave();

			(settingTab.app.workspace as any).trigger(
				"task-genius:calendar-views-changed",
				{ reason: "view-added", viewId: savedView.id },
			);
		},
	).open();
}

/**
 * Open the modal to edit an existing custom view
 */
function openEditViewModal(
	settingTab: TaskProgressBarSettingTab,
	view: CustomCalendarViewConfig,
	onSave: () => void,
) {
	new CalendarViewConfigModal(
		settingTab.app,
		settingTab.plugin,
		{ ...view },
		async (savedView) => {
			const views = settingTab.plugin.settings.customCalendarViews || [];
			const index = views.findIndex((v) => v.id === savedView.id);
			if (index !== -1) {
				savedView.updatedAt = Date.now();
				views[index] = savedView;
				settingTab.plugin.settings.customCalendarViews = views;
				await settingTab.plugin.saveSettings();

				new Notice(t("Calendar view updated: ") + savedView.name);
				onSave();

				(settingTab.app.workspace as any).trigger(
					"task-genius:calendar-views-changed",
					{ reason: "view-updated", viewId: savedView.id },
				);
			}
		},
	).open();
}
