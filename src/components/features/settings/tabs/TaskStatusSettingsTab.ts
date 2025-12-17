import { Modal, setIcon, Setting } from "obsidian";
import { t } from "@/translations/helper";
import { allStatusCollections } from "@/common/task-status";
import { TaskProgressBarSettingTab } from "@/setting";
import { getTasksAPI } from "@/utils";
import {
	DEFAULT_SETTINGS,
	TaskStatusConfig,
	StatusCycle,
} from "@/common/setting-definition";
import * as taskStatusModule from "@/common/task-status";
import Sortable from "sortablejs";
import { ListConfigModal } from "@/components/ui/modals/ListConfigModal";

export function renderTaskStatusSettingsTab(
	settingTab: TaskProgressBarSettingTab,
	containerEl: HTMLElement,
) {
	new Setting(containerEl)
		.setName(t("Task Status Configuration"))
		.setDesc(
			t("Define how task statuses are recognized, displayed, and cycled"),
		)
		.setHeading();

	// Check if Tasks plugin is installed and show compatibility warning
	const tasksAPI = getTasksAPI(settingTab.plugin);
	if (tasksAPI) {
		const warningBanner = containerEl.createDiv({
			cls: "tasks-compatibility-warning",
		});

		warningBanner.createEl("div", {
			cls: "tasks-warning-icon",
			text: "⚠️",
		});

		const warningContent = warningBanner.createDiv({
			cls: "tasks-warning-content",
		});

		warningContent.createEl("div", {
			cls: "tasks-warning-title",
			text: t("Tasks Plugin Detected"),
		});

		const warningText = warningContent.createEl("div", {
			cls: "tasks-warning-text",
		});

		warningText.createEl("span", {
			text: t(
				"Both plugins manage task statuses and dates, which may cause conflicts. To avoid issues: 1) Choose ONE plugin for status cycling, 2) Disable overlapping date features. See the ",
			),
		});

		const compatibilityLink = warningText.createEl("a", {
			text: t("compatibility guide"),
			href: "https://taskgenius.md/docs/compatibility",
		});
		compatibilityLink.setAttribute("target", "_blank");
		compatibilityLink.setAttribute("rel", "noopener noreferrer");

		warningText.createEl("span", {
			text: t(" for setup instructions."),
		});
	}

	// Parent Task Automation Section
	new Setting(containerEl)
		.setName(t("Parent Task Automation"))
		.setDesc(
			t("Automatically update parent tasks based on subtask completion"),
		)
		.setHeading();

	new Setting(containerEl)
		.setName(t("Auto-complete parent tasks"))
		.setDesc(
			t(
				"When ALL subtasks are completed, automatically mark the parent task as completed.",
			),
		)
		.addToggle((toggle) =>
			toggle
				.setValue(settingTab.plugin.settings.autoCompleteParent)
				.onChange(async (value) => {
					settingTab.plugin.settings.autoCompleteParent = value;
					settingTab.applySettingsUpdate();
				}),
		);

	new Setting(containerEl)
		.setName(t("Mark parent as 'In Progress' when partially complete"))
		.setDesc(
			t(
				"When SOME (but not all) subtasks are done, mark the parent as 'In Progress'. Requires 'Auto-complete parent tasks' to be enabled.",
			),
		)
		.addToggle((toggle) =>
			toggle
				.setValue(
					settingTab.plugin.settings
						.markParentInProgressWhenPartiallyComplete,
				)
				.onChange(async (value) => {
					settingTab.plugin.settings.markParentInProgressWhenPartiallyComplete =
						value;
					settingTab.applySettingsUpdate();
				}),
		);

	// Core Status Categories Section - with explanation
	new Setting(containerEl)
		.setName(t("Core Status Categories"))
		.setDesc(
			createFragment((frag) => {
				frag.appendText(
					t(
						"Task Genius uses 5 core categories to calculate progress and statistics. Each category can have multiple symbols.",
					),
				);
				frag.createEl("br");
				frag.createEl("br");
				const learnMore = frag.createEl("a", {
					text: t("Learn why 5 categories →"),
					href: "https://taskgenius.md/docs/task-status#status-categories",
				});
				learnMore.setAttribute("target", "_blank");
				learnMore.setAttribute("rel", "noopener noreferrer");
			}),
		)
		.setHeading()
		.addDropdown((dropdown) => {
			dropdown.addOption("custom", "Custom");
			for (const statusCollection of allStatusCollections) {
				dropdown.addOption(statusCollection, statusCollection);
			}

			// Set default value to custom
			dropdown.setValue("custom");

			dropdown.onChange(async (value) => {
				if (value === "custom") {
					return;
				}

				// Confirm before applying the theme
				const modal = new Modal(settingTab.app);
				modal.titleEl.setText(`Apply ${value} Theme?`);

				const content = modal.contentEl.createDiv();
				content.setText(
					`This will override your current checkbox status settings with the ${value} theme. Do you want to continue?`,
				);

				const buttonContainer = modal.contentEl.createDiv({
					cls: "tg-modal-button-container modal-button-container",
				});

				const cancelButton = buttonContainer.createEl("button");
				cancelButton.setText(t("Cancel"));
				cancelButton.addEventListener("click", () => {
					dropdown.setValue("custom");
					modal.close();
				});

				const confirmButton = buttonContainer.createEl("button");
				confirmButton.setText(t("Apply Theme"));
				confirmButton.addClass("mod-cta");
				confirmButton.addEventListener("click", async () => {
					modal.close();

					// Apply the selected theme's task statuses
					try {
						// Get the function based on the selected theme
						const functionName =
							value.toLowerCase() + "SupportedStatuses";

						// Use type assertion for the dynamic function access
						const getStatuses = (taskStatusModule as any)[
							functionName
						];

						if (typeof getStatuses === "function") {
							const statuses = getStatuses();

							// Update cycle and marks
							const cycle =
								settingTab.plugin.settings.taskStatusCycle;
							const marks =
								settingTab.plugin.settings.taskStatusMarks;
							const excludeMarks =
								settingTab.plugin.settings
									.excludeMarksFromCycle;

							// Clear existing cycle, marks and excludeMarks
							cycle.length = 0;
							Object.keys(marks).forEach(
								(key) => delete marks[key],
							);
							excludeMarks.length = 0;

							// Add new statuses to cycle and marks
							for (const [symbol, name, type] of statuses) {
								const realName = (name as string)
									.split("/")[0]
									.trim();
								// Add to cycle if not already included
								if (!cycle.includes(realName)) {
									cycle.push(realName);
								}

								// Add to marks
								marks[realName] = symbol;

								// Add to excludeMarks if not space or x
								if (symbol !== " " && symbol !== "x") {
									excludeMarks.push(realName);
								}
							}

							// Also update the main taskStatuses object based on the theme
							const statusMap: Record<string, string[]> = {
								completed: [],
								inProgress: [],
								abandoned: [],
								notStarted: [],
								planned: [],
							};
							for (const [symbol, _, type] of statuses) {
								if (type in statusMap) {
									statusMap[
										type as keyof typeof statusMap
									].push(symbol);
								}
							}
							// Corrected loop and assignment for TaskStatusConfig here too
							for (const type of Object.keys(statusMap) as Array<
								keyof TaskStatusConfig
							>) {
								if (
									type in
										settingTab.plugin.settings
											.taskStatuses &&
									statusMap[type] &&
									statusMap[type].length > 0
								) {
									settingTab.plugin.settings.taskStatuses[
										type
									] = statusMap[type].join("|");
								}
							}

							// Save settings and refresh the display
							settingTab.applySettingsUpdate();
							settingTab.display();
						}
					} catch (error) {
						console.error(
							"Failed to apply checkbox status theme:",
							error,
						);
					}
				});

				modal.open();
			});
		});

	/**
	 * Helper function to create a status symbol configuration button
	 * @param statusKey - The key in taskStatuses config (e.g., "completed")
	 * @param config - Configuration for the status button
	 */
	const createStatusConfigButton = (
		statusKey: keyof TaskStatusConfig,
		config: {
			title: string;
			description: string;
			placeholder: string;
			icon: string;
			// Special handling: Not Started needs to preserve spaces
			preserveEmpty?: boolean;
		},
	) => {
		// Create icon fragment
		const statusIcon = createFragment();
		statusIcon.createEl(
			"span",
			{
				cls: "tg-status-icon",
			},
			(el) => {
				setIcon(el, config.icon);
			},
		);

		statusIcon.createEl(
			"span",
			{
				cls: "tg-status-text",
			},
			(el) => {
				el.setText(t(config.title));
			},
		);

		// Create setting with button
		new Setting(containerEl)
			.setName(statusIcon)
			.setDesc(t(config.description))
			.addButton((button) => {
				const getStatusSymbols = () => {
					const value =
						settingTab.plugin.settings.taskStatuses[statusKey];
					const symbols = value.split("|");
					// Not Started needs to preserve spaces, others filter empty strings
					return config.preserveEmpty
						? symbols
						: symbols.filter((v) => v.length > 0);
				};

				const updateButtonText = () => {
					const symbols = getStatusSymbols();
					if (symbols.length === 0) {
						button.setButtonText(t("Configure Symbols"));
					} else {
						button.setButtonText(
							t("{{count}} symbol(s) configured", {
								interpolation: {
									count: symbols.length.toString(),
								},
							}),
						);
					}
				};

				updateButtonText();
				button.onClick(() => {
					new ListConfigModal(settingTab.plugin, {
						title: t("{{status}} Task Symbols", {
							interpolation: { status: t(config.title) },
						}),
						description: t(config.description),
						placeholder: config.placeholder,
						values: getStatusSymbols(),
						onSave: async (values) => {
							settingTab.plugin.settings.taskStatuses[statusKey] =
								values.join("|") ||
								DEFAULT_SETTINGS.taskStatuses[statusKey];
							await settingTab.applySettingsUpdate();
							updateButtonText();

							// Update Task Genius Icon Manager
							if (settingTab.plugin.taskGeniusIconManager) {
								settingTab.plugin.taskGeniusIconManager.update();
							}
						},
					}).open();
				});
			});
	};

	// Configure Completed status
	createStatusConfigButton("completed", {
		title: "Completed",
		description:
			"Finished tasks. Counts as 100% in progress bars. Common symbols: x, X",
		placeholder: "x",
		icon: "completed",
	});

	// Configure In Progress status
	createStatusConfigButton("inProgress", {
		title: "In Progress",
		description:
			"Tasks actively being worked on. Shows as 'active' in progress tracking. Common symbols: /, >",
		placeholder: "/",
		icon: "inProgress",
	});

	// Configure Planned status
	createStatusConfigButton("planned", {
		title: "Planned",
		description:
			"Scheduled or waiting tasks. Often excluded from progress counts. Common symbols: ?, >",
		placeholder: "?",
		icon: "planned",
	});

	// Configure Abandoned status
	createStatusConfigButton("abandoned", {
		title: "Abandoned",
		description:
			"Cancelled or irrelevant tasks. Excluded from active tracking. Common symbol: -",
		placeholder: "-",
		icon: "abandoned",
	});

	// Configure Not Started status (preserves empty spaces)
	createStatusConfigButton("notStarted", {
		title: "Not Started",
		description:
			"Pending tasks awaiting action. Default starting state. Common symbol: (space)",
		placeholder: " ",
		icon: "notStarted",
		preserveEmpty: true,
	});

	new Setting(containerEl)
		.setName(t("Count unknown symbols as"))
		.setDesc(
			t(
				"How to treat symbols not defined above (e.g., [!], [r]). Default: Not Started.",
			),
		)
		.addDropdown((dropdown) => {
			dropdown.addOption("notStarted", "Not Started");
			dropdown.addOption("abandoned", "Abandoned");
			dropdown.addOption("planned", "Planned");
			dropdown.addOption("completed", "Completed");
			dropdown.addOption("inProgress", "In Progress");
			dropdown.setValue(
				settingTab.plugin.settings.countOtherStatusesAs || "notStarted",
			);
			dropdown.onChange((value) => {
				settingTab.plugin.settings.countOtherStatusesAs = value;
				settingTab.applySettingsUpdate();
			});
		});

	// Task Counting Settings
	new Setting(containerEl)
		.setName(t("Progress Calculation"))
		.setDesc(
			t("Control which tasks are included in progress bar calculations"),
		)
		.setHeading();

	new Setting(containerEl)
		.setName(t("Exclude markers from progress"))
		.setDesc(
			t(
				"Symbols to ignore in progress calculations. Separate with |. Example: ?|- (excludes planned and abandoned)",
			),
		)
		.addText((text) =>
			text
				.setPlaceholder("?|-")
				.setValue(settingTab.plugin.settings.excludeTaskMarks)
				.onChange(async (value) => {
					settingTab.plugin.settings.excludeTaskMarks = value;
					settingTab.applySettingsUpdate();
				}),
		);

	new Setting(containerEl)
		.setName(t("Use whitelist mode"))
		.setDesc(
			t(
				"Only count specific markers (ignore all others). Useful for strict progress tracking.",
			),
		)
		.addToggle((toggle) =>
			toggle
				.setValue(settingTab.plugin.settings.useOnlyCountMarks)
				.onChange(async (value) => {
					settingTab.plugin.settings.useOnlyCountMarks = value;
					settingTab.applySettingsUpdate();

					setTimeout(() => {
						settingTab.display();
					}, 200);
				}),
		);

	if (settingTab.plugin.settings.useOnlyCountMarks) {
		new Setting(containerEl)
			.setName(t("Whitelist markers"))
			.setDesc(
				t(
					"Only these symbols will be counted. Separate with |. Example: (space)|/|x",
				),
			)
			.addText((text) =>
				text
					.setPlaceholder(DEFAULT_SETTINGS.onlyCountTaskMarks)
					.setValue(settingTab.plugin.settings.onlyCountTaskMarks)
					.onChange(async (value) => {
						if (value.length === 0) {
							settingTab.plugin.settings.onlyCountTaskMarks =
								DEFAULT_SETTINGS.onlyCountTaskMarks;
						} else {
							settingTab.plugin.settings.onlyCountTaskMarks =
								value;
						}
						settingTab.applySettingsUpdate();
					}),
			);
	}

	// Status Cycling Section - with clear explanation of scope
	new Setting(containerEl)
		.setName(t("Status Cycling in Task Genius View"))
		.setDesc(
			createFragment((frag) => {
				frag.appendText(
					t(
						"Control click behavior in the Task Genius sidebar panel.",
					),
				);
				frag.createEl("br");
				frag.createEl("br");
				const scopeEl = frag.createEl("strong");
				scopeEl.setText(t("Scope: "));
				frag.appendText(
					t(
						"Clicking tasks in Task Genius View cycles status for both file-based tasks and inline checkboxes. Changes are saved directly to the source file.",
					),
				);
				frag.createEl("br");
				frag.createEl("br");
				const learnMore = frag.createEl("a", {
					text: t("How status cycling works →"),
					href: "https://taskgenius.md/docs/task-status#cycling-through-statuses",
				});
				learnMore.setAttribute("target", "_blank");
				learnMore.setAttribute("rel", "noopener noreferrer");
			}),
		)
		.setHeading();

	new Setting(containerEl)
		.setName(t("Enable click-to-cycle in View"))
		.setDesc(
			t(
				"Click checkboxes in Task Genius View to cycle through statuses (e.g., [ ] → [/] → [x]). Affects all task sources.",
			),
		)
		.addToggle((toggle) => {
			toggle
				.setValue(settingTab.plugin.settings.enableTaskStatusSwitcher)
				.onChange(async (value) => {
					settingTab.plugin.settings.enableTaskStatusSwitcher = value;
					settingTab.applySettingsUpdate();

					setTimeout(() => {
						settingTab.display();
					}, 200);
				});
		});

	if (settingTab.plugin.settings.enableTaskStatusSwitcher) {
		new Setting(containerEl)
			.setName(t("Show status indicator"))
			.setDesc(
				t(
					"Display a clickable status icon next to the checkbox for easier status identification.",
				),
			)
			.addToggle((toggle) =>
				toggle
					.setValue(
						settingTab.plugin.settings.enableIndicatorWithCheckbox,
					)
					.onChange(async (value) => {
						settingTab.plugin.settings.enableIndicatorWithCheckbox =
							value;
						settingTab.applySettingsUpdate();
					}),
			);
	}

	if (settingTab.plugin.settings.enableTaskStatusSwitcher) {
		new Setting(containerEl)
			.setName(t("Visual style"))
			.setDesc(
				t(
					"How checkboxes appear: Standard (default), Text Marks ([TODO]), or Custom Icons.",
				),
			)
			.addDropdown((dropdown) => {
				dropdown.addOption("default", t("Default checkboxes"));
				dropdown.addOption("textmarks", t("Custom text marks"));
				dropdown.addOption("icons", t("Task Genius icons"));

				// Determine current value based on existing settings
				let currentValue = "default";
				if (settingTab.plugin.settings.enableTaskGeniusIcons) {
					currentValue = "icons";
				} else if (settingTab.plugin.settings.enableCustomTaskMarks) {
					currentValue = "textmarks";
				}

				dropdown.setValue(currentValue);

				dropdown.onChange(async (value) => {
					// Reset all options first
					settingTab.plugin.settings.enableCustomTaskMarks = false;
					settingTab.plugin.settings.enableTaskGeniusIcons = false;

					// Set the selected option
					if (value === "textmarks") {
						settingTab.plugin.settings.enableCustomTaskMarks = true;
					} else if (value === "icons") {
						settingTab.plugin.settings.enableTaskGeniusIcons = true;
					}

					settingTab.applySettingsUpdate();

					// Update Task Genius Icon Manager
					if (settingTab.plugin.taskGeniusIconManager) {
						settingTab.plugin.taskGeniusIconManager.update();
					}

					// Refresh display to show/hide dependent options
					setTimeout(() => {
						settingTab.display();
					}, 200);
				});
			});

		// Show text mark source mode option only when custom text marks are enabled
		if (settingTab.plugin.settings.enableCustomTaskMarks) {
			new Setting(containerEl)
				.setName(t("Enable text mark in source mode"))
				.setDesc(
					t(
						"Make the text mark in source mode follow the checkbox status cycle when clicked.",
					),
				)
				.addToggle((toggle) => {
					toggle
						.setValue(
							settingTab.plugin.settings
								.enableTextMarkInSourceMode,
						)
						.onChange(async (value) => {
							settingTab.plugin.settings.enableTextMarkInSourceMode =
								value;
							settingTab.applySettingsUpdate();
						});
				});
		}
	}

	// Custom Status Workflows Section - Editor behavior
	new Setting(containerEl)
		.setName(t("Status Cycling in Editor"))
		.setDesc(
			createFragment((frag) => {
				frag.appendText(
					t(
						"Control click behavior when editing notes in the Editor.",
					),
				);
				frag.createEl("br");
				frag.createEl("br");
				const scopeEl = frag.createEl("strong");
				scopeEl.setText(t("Scope: "));
				frag.appendText(
					t(
						"Clicking inline checkboxes in Live Preview or Reading mode cycles through your defined status sequences.",
					),
				);
				frag.createEl("br");
				frag.createEl("br");
				frag.appendText(
					t(
						"Example: [ ] → [/] → [x] (Not Started → In Progress → Done)",
					),
				);
			}),
		)
		.setHeading();

	new Setting(containerEl)
		.setName(t("Enable custom status cycles in Editor"))
		.setDesc(
			t(
				"Use custom cycling sequences when clicking checkboxes in the editor. When disabled, clicking toggles between [ ] and [x] only.",
			),
		)
		.addToggle((toggle) => {
			toggle
				.setValue(settingTab.plugin.settings.enableCycleCompleteStatus)
				.onChange(async (value) => {
					settingTab.plugin.settings.enableCycleCompleteStatus =
						value;
					settingTab.applySettingsUpdate();

					setTimeout(() => {
						settingTab.display();
					}, 200);
				});
		});

	if (settingTab.plugin.settings.enableCycleCompleteStatus) {
		// Initialize statusCycles from legacy settings if needed
		if (
			!settingTab.plugin.settings.statusCycles ||
			settingTab.plugin.settings.statusCycles.length === 0
		) {
			settingTab.plugin.settings.statusCycles = [
				{
					id: `cycle-${Date.now()}`,
					name: t("Default Cycle"),
					description: t("Migrated from legacy settings"),
					priority: 0,
					cycle: [...settingTab.plugin.settings.taskStatusCycle],
					marks: { ...settingTab.plugin.settings.taskStatusMarks },
					enabled: true,
				},
			];
			settingTab.applySettingsUpdate();
		}

		new Setting(containerEl)
			.setName(t("Status Cycles"))
			.setDesc(
				t(
					"Create multiple workflow patterns. Each cycle defines a sequence of statuses to cycle through.",
				),
			)
			.addDropdown((dropdown) => {
				dropdown.addOption("custom", "Custom");
				for (const statusCollection of allStatusCollections) {
					dropdown.addOption(statusCollection, statusCollection);
				}

				// Set default value to custom
				dropdown.setValue("custom");

				dropdown.onChange(async (value) => {
					if (value === "custom") {
						return;
					}

					// Confirm before applying the theme
					const modal = new Modal(settingTab.app);
					modal.titleEl.setText(`Add ${value} Theme as New Cycle?`);

					const content = modal.contentEl.createDiv();
					content.setText(
						t(
							`This will add a new status cycle based on the ${value} theme.`,
						),
					);

					const buttonContainer = modal.contentEl.createDiv({
						cls: "tg-modal-button-container modal-button-container",
					});

					const cancelButton = buttonContainer.createEl("button");
					cancelButton.setText(t("Cancel"));
					cancelButton.addEventListener("click", () => {
						dropdown.setValue("custom");
						modal.close();
					});

					const confirmButton = buttonContainer.createEl("button");
					confirmButton.setText(t("Add Cycle"));
					confirmButton.addClass("mod-cta");
					confirmButton.addEventListener("click", async () => {
						modal.close();

						// Add a new cycle based on the selected theme
						try {
							// Get the function based on the selected theme
							const functionName =
								value.toLowerCase() + "SupportedStatuses";

							// Use type assertion for the dynamic function access
							const getStatuses = (taskStatusModule as any)[
								functionName
							];

							if (typeof getStatuses === "function") {
								const statuses = getStatuses();

								// Create new cycle arrays
								const newCycle: string[] = [];
								const newMarks: Record<string, string> = {};

								// Add new statuses to cycle and marks
								for (const [symbol, name, type] of statuses) {
									const realName = (name as string)
										.split("/")[0]
										.trim();
									// Add to cycle if not already included
									if (!newCycle.includes(realName)) {
										newCycle.push(realName);
									}

									// Add to marks
									newMarks[realName] = symbol;
								}

								// Create the new status cycle
								const newStatusCycle: StatusCycle = {
									id: `cycle-${Date.now()}`,
									name: value,
									description: t(`${value} theme workflow`),
									priority:
										settingTab.plugin.settings.statusCycles!
											.length,
									cycle: newCycle,
									marks: newMarks,
									enabled: true,
								};

								// Add to statusCycles array
								settingTab.plugin.settings.statusCycles!.push(
									newStatusCycle,
								);

								// Also update the main taskStatuses object based on the theme
								const statusMap: Record<string, string[]> = {
									completed: [],
									inProgress: [],
									abandoned: [],
									notStarted: [],
									planned: [],
								};
								for (const [symbol, _, type] of statuses) {
									if (type in statusMap) {
										statusMap[
											type as keyof typeof statusMap
										].push(symbol);
									}
								}
								// Corrected loop and assignment for TaskStatusConfig here too
								for (const type of Object.keys(
									statusMap,
								) as Array<keyof TaskStatusConfig>) {
									if (
										type in
											settingTab.plugin.settings
												.taskStatuses &&
										statusMap[type] &&
										statusMap[type].length > 0
									) {
										settingTab.plugin.settings.taskStatuses[
											type
										] = statusMap[type].join("|");
									}
								}

								// Save settings and refresh the display
								settingTab.applySettingsUpdate();
								settingTab.display();
							}
						} catch (error) {
							console.error(
								"Failed to apply checkbox status theme:",
								error,
							);
						}
					});

					modal.open();
				});
			});

		// Render the unified multi-cycle management interface
		renderMultiCycleManagement(settingTab, containerEl);
	}

	// Auto Date Manager Settings
	new Setting(containerEl)
		.setName(t("Automatic Date Management"))
		.setDesc(
			createFragment((frag) => {
				frag.appendText(
					t(
						"Auto-add dates when task status changes (e.g., completion date when marked done).",
					),
				);
				frag.createEl("br");
				const learnMore = frag.createEl("a", {
					text: t("Date management guide →"),
					href: "https://taskgenius.md/docs/append-date",
				});
				learnMore.setAttribute("target", "_blank");
				learnMore.setAttribute("rel", "noopener noreferrer");
			}),
		)
		.setHeading();

	new Setting(containerEl)
		.setName(t("Enable automatic dates"))
		.setDesc(
			t(
				"Automatically append date metadata to tasks when their status changes. Supports Tasks emoji format and Dataview format.",
			),
		)
		.addToggle((toggle) =>
			toggle
				.setValue(settingTab.plugin.settings.autoDateManager.enabled)
				.onChange(async (value) => {
					settingTab.plugin.settings.autoDateManager.enabled = value;
					settingTab.applySettingsUpdate();
					setTimeout(() => {
						settingTab.display();
					}, 200);
				}),
		);

	if (settingTab.plugin.settings.autoDateManager.enabled) {
		new Setting(containerEl)
			.setName(t("Completion dates"))
			.setDesc(
				t(
					"Add completion date (✅) when marked Done. Remove if status changes.",
				),
			)
			.addToggle((toggle) =>
				toggle
					.setValue(
						settingTab.plugin.settings.autoDateManager
							.manageCompletedDate,
					)
					.onChange(async (value) => {
						settingTab.plugin.settings.autoDateManager.manageCompletedDate =
							value;
						settingTab.applySettingsUpdate();
					}),
			);

		new Setting(containerEl)
			.setName(t("Start dates"))
			.setDesc(
				t(
					"Add start date (🛫) when marked In Progress. Remove if status changes.",
				),
			)
			.addToggle((toggle) =>
				toggle
					.setValue(
						settingTab.plugin.settings.autoDateManager
							.manageStartDate,
					)
					.onChange(async (value) => {
						settingTab.plugin.settings.autoDateManager.manageStartDate =
							value;
						settingTab.applySettingsUpdate();
					}),
			);

		new Setting(containerEl)
			.setName(t("Cancelled dates"))
			.setDesc(
				t(
					"Add cancelled date (❌) when marked Abandoned. Remove if status changes.",
				),
			)
			.addToggle((toggle) =>
				toggle
					.setValue(
						settingTab.plugin.settings.autoDateManager
							.manageCancelledDate,
					)
					.onChange(async (value) => {
						settingTab.plugin.settings.autoDateManager.manageCancelledDate =
							value;
						settingTab.applySettingsUpdate();
					}),
			);
	}
}

/**
 * Render the unified multi-cycle management interface
 * This replaces the old single-cycle UI and integrates all cycle management features
 */
function renderMultiCycleManagement(
	settingTab: TaskProgressBarSettingTab,
	containerEl: HTMLElement,
) {
	// Quick Templates section - buttons to add preset cycles
	new Setting(containerEl)
		.setName(t("Quick Add Templates"))
		.setDesc(t("Click to add a pre-configured workflow pattern"))
		.addButton((button) => {
			button
				.setButtonText(t("Simple"))
				.setTooltip(t("[ ] ↔ [x] — Basic toggle"))
				.onClick(() => {
					settingTab.plugin.settings.statusCycles!.push({
						id: `cycle-${Date.now()}`,
						name: t("Simple"),
						description: t("Basic TODO/DONE toggle"),
						priority:
							settingTab.plugin.settings.statusCycles!.length,
						cycle: ["TODO", "DONE"],
						marks: {
							TODO: " ",
							DONE: "x",
						},
						enabled: true,
					});
					settingTab.applySettingsUpdate();
					setTimeout(() => settingTab.display(), 200);
				});
		})
		.addButton((button) => {
			button
				.setButtonText(t("GTD"))
				.setTooltip(t("[ ] → [/] → [x] — Getting Things Done"))
				.onClick(() => {
					settingTab.plugin.settings.statusCycles!.push({
						id: `cycle-${Date.now()}`,
						name: t("GTD"),
						description: t("Getting Things Done workflow"),
						priority:
							settingTab.plugin.settings.statusCycles!.length,
						cycle: ["TODO", "IN PROGRESS", "DONE"],
						marks: {
							TODO: " ",
							"IN PROGRESS": "/",
							DONE: "x",
						},
						enabled: true,
					});
					settingTab.applySettingsUpdate();
					setTimeout(() => settingTab.display(), 200);
				});
		})
		.addButton((button) => {
			button
				.setButtonText(t("Kanban"))
				.setTooltip(t("[ ] → [?] → [/] → [x] — Full project flow"))
				.onClick(() => {
					settingTab.plugin.settings.statusCycles!.push({
						id: `cycle-${Date.now()}`,
						name: t("Kanban"),
						description: t("Full project workflow"),
						priority:
							settingTab.plugin.settings.statusCycles!.length,
						cycle: ["TODO", "PLANNED", "IN PROGRESS", "DONE"],
						marks: {
							TODO: " ",
							PLANNED: "?",
							"IN PROGRESS": "/",
							DONE: "x",
						},
						enabled: true,
					});
					settingTab.applySettingsUpdate();
					setTimeout(() => settingTab.display(), 200);
				});
		});

	// Container for all cycles with sortable support
	const cyclesContainer = containerEl.createDiv({
		cls: "status-cycles-container",
	});

	// Sort cycles by priority
	const sortedCycles = [...settingTab.plugin.settings.statusCycles!].sort(
		(a, b) => a.priority - b.priority,
	);

	// Render each cycle
	sortedCycles.forEach((cycle, index) => {
		const cycleCard = cyclesContainer.createDiv({
			cls: "status-cycle-card",
		});
		cycleCard.setAttribute("data-cycle-id", cycle.id);

		// Card header with collapse button, up/down buttons, title and controls
		const cardHeader = cycleCard.createDiv({
			cls: "status-cycle-header",
		});

		// Collapse button
		const collapseButton = cardHeader.createDiv({
			cls: "status-cycle-collapse-button",
		});
		setIcon(collapseButton, "chevron-down");
		collapseButton.setAttribute("title", t("Collapse/Expand"));
		collapseButton.addEventListener("click", () => {
			cycleCard.classList.toggle("collapsed");
			collapseButton.empty();
			setIcon(
				collapseButton,
				cycleCard.classList.contains("collapsed")
					? "chevron-right"
					: "chevron-down",
			);
		});

		// Up/Down buttons container
		const upDownButtons = cardHeader.createDiv({
			cls: "status-cycle-updown-buttons",
		});

		const upButton = upDownButtons.createDiv({
			cls: "status-cycle-button",
		});
		setIcon(upButton, "chevron-up");
		upButton.setAttribute("title", t("Move up"));
		if (index === 0) {
			upButton.classList.add("disabled");
		} else {
			upButton.addEventListener("click", () => {
				const cycles = settingTab.plugin.settings.statusCycles!;
				const currentIndex = cycles.findIndex((c) => c.id === cycle.id);
				if (currentIndex > 0) {
					// Swap priorities
					const temp = cycles[currentIndex - 1].priority;
					cycles[currentIndex - 1].priority =
						cycles[currentIndex].priority;
					cycles[currentIndex].priority = temp;
					settingTab.applySettingsUpdate();
					setTimeout(() => settingTab.display(), 200);
				}
			});
		}

		const downButton = upDownButtons.createDiv({
			cls: "status-cycle-button",
		});
		setIcon(downButton, "chevron-down");
		downButton.setAttribute("title", t("Move down"));
		if (index === sortedCycles.length - 1) {
			downButton.classList.add("disabled");
		} else {
			downButton.addEventListener("click", () => {
				const cycles = settingTab.plugin.settings.statusCycles!;
				const currentIndex = cycles.findIndex((c) => c.id === cycle.id);
				if (currentIndex < cycles.length - 1) {
					// Swap priorities
					const temp = cycles[currentIndex + 1].priority;
					cycles[currentIndex + 1].priority =
						cycles[currentIndex].priority;
					cycles[currentIndex].priority = temp;
					settingTab.applySettingsUpdate();
					setTimeout(() => settingTab.display(), 200);
				}
			});
		}

		// Content-editable title
		const titleElement = cardHeader.createDiv({
			cls: "status-cycle-title",
		});
		titleElement.setAttribute("contenteditable", "true");
		titleElement.textContent = cycle.name;
		titleElement.addEventListener("blur", () => {
			const newName = titleElement.textContent?.trim() || "Unnamed Cycle";
			if (newName !== cycle.name) {
				cycle.name = newName;
				settingTab.applySettingsUpdate();
			}
		});
		titleElement.addEventListener("keydown", (e) => {
			if (e.key === "Enter") {
				e.preventDefault();
				titleElement.blur();
			}
		});

		// Control buttons (toggle, copy, delete)
		const controlsContainer = cardHeader.createDiv({
			cls: "status-cycle-controls",
		});

		const headerSetting = new Setting(controlsContainer)
			.addToggle((toggle) => {
				toggle
					.setValue(cycle.enabled)
					.setTooltip(t("Enable/disable this cycle"))
					.onChange(async (value) => {
						cycle.enabled = value;
						settingTab.applySettingsUpdate();
						setTimeout(() => settingTab.display(), 200);
					});
			})
			.addExtraButton((button) => {
				button
					.setIcon("copy")
					.setTooltip(t("Copy this cycle"))
					.onClick(() => {
						// Create a deep copy of the cycle
						const copiedCycle: StatusCycle = {
							id: `cycle-${Date.now()}`,
							name: `${cycle.name} (Copy)`,
							description: cycle.description,
							priority:
								settingTab.plugin.settings.statusCycles!.length,
							cycle: [...cycle.cycle],
							marks: { ...cycle.marks },
							enabled: cycle.enabled,
							color: cycle.color,
							icon: cycle.icon,
						};
						settingTab.plugin.settings.statusCycles!.push(
							copiedCycle,
						);
						settingTab.applySettingsUpdate();
						setTimeout(() => settingTab.display(), 200);
					});
			})
			.addExtraButton((button) => {
				button
					.setIcon("trash")
					.setTooltip(t("Delete this cycle"))
					.onClick(() => {
						const cycleIndex =
							settingTab.plugin.settings.statusCycles!.findIndex(
								(c) => c.id === cycle.id,
							);
						if (cycleIndex !== -1) {
							settingTab.plugin.settings.statusCycles!.splice(
								cycleIndex,
								1,
							);
							settingTab.applySettingsUpdate();
							setTimeout(() => settingTab.display(), 200);
						}
					});
			});

		// Cycle details
		const cardBody = cycleCard.createDiv({
			cls: "status-cycle-body",
		});

		// Status list heading
		new Setting(cardBody)
			.setName(t("Status sequence"))
			.setDesc(t("Define the statuses in cycling order"))
			.setHeading();

		// Status list container
		const statusListContainer = cardBody.createDiv({
			cls: "status-list-container",
		});

		// Render each status in the cycle
		cycle.cycle.forEach((statusName, statusIndex) => {
			const statusRow = statusListContainer.createDiv({
				cls: "status-row",
			});
			statusRow.setAttribute("data-status-name", statusName);

			// Add drag handle for status
			const statusDragHandle = statusRow.createDiv({
				cls: "status-drag-handle",
			});
			setIcon(statusDragHandle, "grip-vertical");
			statusDragHandle.setAttribute("title", t("Drag to reorder"));

			const statusSetting = new Setting(statusRow);
			statusSetting
				.setName(`#${statusIndex + 1}`)
				.addText((text) => {
					text.setValue(statusName)
						.setPlaceholder(t("Status name"))
						.onChange((value) => {
							const oldName = cycle.cycle[statusIndex];
							cycle.cycle[statusIndex] = value;

							// Update marks
							if (cycle.marks[oldName]) {
								cycle.marks[value] = cycle.marks[oldName];
								delete cycle.marks[oldName];
							}

							settingTab.applySettingsUpdate();
						});
					text.inputEl.style.width = "150px";
				})
				.addText((text) => {
					text.setValue(cycle.marks[statusName] || " ")
						.setPlaceholder(t("Mark"))
						.onChange((value) => {
							cycle.marks[statusName] = value.charAt(0) || " ";
							settingTab.applySettingsUpdate();
						});
					text.inputEl.style.width = "50px";
					text.inputEl.maxLength = 1;
				})
				.addExtraButton((button) => {
					button
						.setIcon("trash")
						.setTooltip(t("Remove this status"))
						.onClick(() => {
							cycle.cycle.splice(statusIndex, 1);
							delete cycle.marks[statusName];
							settingTab.applySettingsUpdate();
							setTimeout(() => settingTab.display(), 200);
						});
				});
		});

		// Initialize Sortable.js for status reordering
		new Sortable(statusListContainer, {
			animation: 150,
			handle: ".status-drag-handle",
			draggable: ".status-row",
			ghostClass: "status-row-ghost",
			chosenClass: "status-row-chosen",
			dragClass: "status-row-drag",
			filter: ".setting-item", // Exclude the add button
			onEnd: (evt) => {
				if (evt.oldIndex !== undefined && evt.newIndex !== undefined) {
					// Reorder the status array
					const movedStatus = cycle.cycle.splice(evt.oldIndex, 1)[0];
					cycle.cycle.splice(evt.newIndex, 0, movedStatus);

					settingTab.applySettingsUpdate();
					setTimeout(() => settingTab.display(), 200);
				}
			},
		});

		// Add status button
		new Setting(statusListContainer).addButton((button) => {
			button.setButtonText(t("+ Add Status")).onClick(() => {
				const newStatus = `STATUS_${cycle.cycle.length + 1}`;
				cycle.cycle.push(newStatus);
				cycle.marks[newStatus] = " ";
				settingTab.applySettingsUpdate();
				setTimeout(() => settingTab.display(), 200);
			});
		});
	});

	// Add new custom cycle button
	new Setting(cyclesContainer).addButton((button) => {
		button
			.setButtonText(t("+ Add Custom Cycle"))
			.setCta()
			.onClick(() => {
				settingTab.plugin.settings.statusCycles!.push({
					id: `cycle-${Date.now()}`,
					name: t("Custom Cycle"),
					description: "",
					priority: settingTab.plugin.settings.statusCycles!.length,
					cycle: ["TODO", "DONE"],
					marks: {
						TODO: " ",
						DONE: "x",
					},
					enabled: true,
				});
				settingTab.applySettingsUpdate();
				setTimeout(() => settingTab.display(), 200);
			});
	});
}
