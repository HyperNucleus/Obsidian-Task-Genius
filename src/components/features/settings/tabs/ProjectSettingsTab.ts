/**
 * ProjectSettingsTab.ts - Project Auto-Detection Settings Page
 *
 * ## Overview
 * This module renders the "Project Auto-Detection" settings page, allowing users
 * to configure how tasks are automatically assigned to projects.
 * Core concept: Tasks can be automatically identified and assigned to projects
 * through multiple data sources without manual tagging.
 *
 * ## Design Principles
 * 1. Data Source Driven: Project detection is divided into three main sources
 *    (path, metadata, config file)
 * 2. Progressive Complexity: From simple to complex, users can enable only
 *    the features they need
 * 3. Fallback Strategy: When primary detection methods fail, provide default
 *    naming as a fallback
 *
 * ## Usage Examples
 * - Path mapping: `Projects/Work/meeting.md` → Automatically assigned to "Work" project
 * - Metadata detection: `project: MyProject` in frontmatter → Assigned to "MyProject"
 * - Config file: `project.md` exists in folder → Read its frontmatter as project config
 *
 * ## Important Notes
 * - Detection priority: Metadata > Config File > Path Mapping > Default Naming
 * - Performance consideration: Path mapping uses glob pattern matching,
 *   too many rules may affect performance
 *
 * @module ProjectSettingsTab
 * @author Task Genius
 * @since v9.x
 */

import { Setting } from "obsidian";
import { TaskProgressBarSettingTab } from "@/setting";
import { t } from "@/translations/helper";

/**
 * Default project configuration object.
 * Used when user first opens settings page or when configuration is missing.
 *
 * @constant DEFAULT_PROJECT_CONFIG
 *
 * @property {boolean} enableEnhancedProject - Main toggle controlling the entire
 *           project auto-detection feature
 * @property {Array} pathMappings - List of path-to-project mapping rules
 * @property {Object} metadataConfig - Metadata detection configuration
 * @property {Object} configFile - Config file detection configuration
 * @property {Array} metadataMappings - Metadata field mapping rules
 * @property {Object} defaultProjectNaming - Default project naming strategy
 */
const DEFAULT_PROJECT_CONFIG = {
	enableEnhancedProject: false,
	pathMappings: [],
	metadataConfig: {
		metadataKey: "project",
		enabled: false,
	},
	configFile: {
		fileName: "project.md",
		searchRecursively: true,
		enabled: false,
	},
	metadataMappings: [],
	defaultProjectNaming: {
		strategy: "filename" as const,
		stripExtension: true,
		enabled: false,
	},
};

/**
 * Ensures project configuration object exists and is complete.
 * If configuration is missing, fills it with default values.
 *
 * @param settingTab - Settings tab instance
 * @returns Complete project configuration object
 */
function ensureProjectConfig(settingTab: TaskProgressBarSettingTab) {
	if (!settingTab.plugin.settings.projectConfig) {
		settingTab.plugin.settings.projectConfig = {
			...DEFAULT_PROJECT_CONFIG,
		};
	}
	return settingTab.plugin.settings.projectConfig;
}

/**
 * Main entry function for rendering the project settings page.
 *
 * ## Page Structure
 * 1. Page title and general description
 * 2. Main toggle: Enable/disable entire project auto-detection feature
 * 3. Source 1: Path-based project mapping
 * 4. Source 2: Metadata-based project detection
 * 5. Source 3: Config file-based project detection
 * 6. Advanced: Custom detection methods
 * 7. Advanced: Metadata field mapping
 * 8. Fallback: Default project naming
 *
 * @param settingTab - Obsidian settings tab instance for accessing plugin
 *                     settings and save functionality
 * @param containerEl - DOM container element where all settings will be rendered
 */
export function renderProjectSettingsTab(
	settingTab: TaskProgressBarSettingTab,
	containerEl: HTMLElement,
) {
	// ============================================================
	// Section 1: Page Title and General Description
	// ============================================================
	// Set page title with new, more precise naming
	new Setting(containerEl)
		.setName(t("Project Auto-Detection"))
		.setDesc(
			t(
				"Configure how tasks are automatically assigned to projects. Detection priority: Task-level metadata > File metadata > Config file > Path mapping > Default naming.",
			),
		)
		.setHeading();

	// Ensure configuration object exists
	const projectConfig = ensureProjectConfig(settingTab);

	// ============================================================
	// Section 2: Main Feature Toggle
	// ============================================================
	/**
	 * Main toggle: Enable project auto-detection feature
	 *
	 * ## Functionality
	 * - When ON: Plugin will automatically assign projects to tasks based on
	 *   configured rules below
	 * - When OFF: All auto-detection features are disabled, tasks only
	 *   recognize projects through manual tagging
	 *
	 * ## Scope of Impact
	 * - ON: Shows all detailed configuration options
	 * - OFF: Hides all detailed configuration options to reduce UI complexity
	 */
	new Setting(containerEl)
		.setName(t("Enable project auto-detection"))
		.setDesc(
			t(
				"When enabled, tasks will be automatically assigned to projects based on file location, frontmatter metadata, or project configuration files.",
			),
		)
		.addToggle((toggle) => {
			toggle
				.setValue(projectConfig.enableEnhancedProject || false)
				.onChange(async (value) => {
					projectConfig.enableEnhancedProject = value;
					await settingTab.plugin.saveSettings();
					// Refresh page to show/hide detailed settings
					setTimeout(() => {
						settingTab.display();
					}, 200);
				});
		});

	// Only show detailed configuration when main toggle is enabled
	if (!projectConfig.enableEnhancedProject) {
		return;
	}

	// ============================================================
	// Section 3: Source 1 - Path-based Project Mapping
	// ============================================================
	/**
	 * Path mapping configuration section
	 *
	 * ## Functionality
	 * Automatically assigns tasks to projects based on file location in vault.
	 * Supports glob pattern matching for flexible path rule definition.
	 *
	 * ## Use Cases
	 * - Scenario 1: All files under `Projects/Work/*` assigned to "Work" project
	 * - Scenario 2: All files under `Areas/Health/*` assigned to "Health & Wellness"
	 *
	 * ## Configuration Examples
	 * | Path Pattern        | Project Name        |
	 * |---------------------|---------------------|
	 * | Projects/Work       | Work                |
	 * | Areas/Health/**     | Health & Wellness   |
	 * | Daily/**            | Daily Notes         |
	 */
	new Setting(containerEl)
		.setName(t("Source 1: Path-based Detection"))
		.setDesc(
			t(
				"Automatically assign projects based on file location. Create rules that map folder paths to project names.",
			),
		)
		.setHeading();

	// Path mappings list container
	const pathMappingsContainer = containerEl.createDiv({
		cls: "project-path-mappings-container",
	});

	/**
	 * Refreshes the path mappings list.
	 * Called after adding, deleting, or modifying mappings to re-render the list.
	 */
	const refreshPathMappings = () => {
		pathMappingsContainer.empty();

		// Ensure pathMappings array exists
		if (
			!projectConfig.pathMappings ||
			!Array.isArray(projectConfig.pathMappings)
		) {
			projectConfig.pathMappings = [];
		}

		const pathMappings = projectConfig.pathMappings;

		// Empty state message
		if (pathMappings.length === 0) {
			pathMappingsContainer.createDiv({
				cls: "no-mappings-message",
				text: t(
					"No path mappings yet. Add a mapping to auto-detect projects from file paths.",
				),
			});
		}

		// Render each mapping rule
		pathMappings.forEach((mapping, index) => {
			const mappingRow = pathMappingsContainer.createDiv({
				cls: "project-path-mapping-row",
			});

			new Setting(mappingRow)
				.setName(`${t("Rule")} ${index + 1}`)
				.addText((text) => {
					text.setPlaceholder(t("Path pattern (e.g., Projects/Work)"))
						.setValue(mapping.pathPattern)
						.onChange(async (value) => {
							projectConfig.pathMappings[index].pathPattern =
								value;
							await settingTab.plugin.saveSettings();
						});
				})
				.addText((text) => {
					text.setPlaceholder(t("Project name"))
						.setValue(mapping.projectName)
						.onChange(async (value) => {
							projectConfig.pathMappings[index].projectName =
								value;
							await settingTab.plugin.saveSettings();
						});
				})
				.addToggle((toggle) => {
					toggle
						.setTooltip(t("Enable this rule"))
						.setValue(mapping.enabled)
						.onChange(async (value) => {
							projectConfig.pathMappings[index].enabled = value;
							await settingTab.plugin.saveSettings();
						});
				})
				.addButton((button) => {
					button
						.setIcon("trash")
						.setTooltip(t("Delete this rule"))
						.onClick(async () => {
							projectConfig.pathMappings.splice(index, 1);
							await settingTab.plugin.saveSettings();
							refreshPathMappings();
						});
				});
		});

		// Add new mapping button
		new Setting(pathMappingsContainer).addButton((button) => {
			button
				.setButtonText(t("Add path rule"))
				.setCta()
				.onClick(async () => {
					projectConfig.pathMappings.push({
						pathPattern: "",
						projectName: "",
						enabled: true,
					});
					await settingTab.plugin.saveSettings();
					setTimeout(() => {
						refreshPathMappings();
					}, 100);
				});
		});
	};

	refreshPathMappings();

	// ============================================================
	// Section 4: Source 2 - Metadata-based Project Detection
	// ============================================================
	/**
	 * Metadata detection configuration section
	 *
	 * ## Functionality
	 * Reads project information from file frontmatter (YAML metadata section).
	 * This is the most direct way to tag projects, suitable for scenarios
	 * requiring precise control.
	 *
	 * ## Use Case
	 * Add frontmatter at the top of markdown file:
	 * ```yaml
	 * ---
	 * project: MyProject
	 * ---
	 * ```
	 * All tasks in this file will be automatically assigned to "MyProject".
	 *
	 * ## Priority Note
	 * Metadata detection has the highest priority and will override results
	 * from path mapping and config file detection.
	 */
	new Setting(containerEl)
		.setName(t("Source 2: Metadata-based Detection"))
		.setDesc(
			t(
				"Read project information directly from file frontmatter (YAML metadata). Has higher priority than path mapping and config file.",
			),
		)
		.setHeading();

	// Metadata detection toggle
	new Setting(containerEl)
		.setName(t("Enable metadata detection"))
		.setDesc(
			t(
				"Read project name from the frontmatter of each file. Example: 'project: MyProject' in YAML header.",
			),
		)
		.addToggle((toggle) => {
			toggle
				.setValue(projectConfig.metadataConfig?.enabled || false)
				.onChange(async (value) => {
					if (projectConfig.metadataConfig) {
						projectConfig.metadataConfig.enabled = value;
						await settingTab.plugin.saveSettings();
					}
				});
		});

	// Metadata key name configuration
	new Setting(containerEl)
		.setName(t("Metadata field name"))
		.setDesc(
			t(
				"The frontmatter key to read project from. Use 'project: true' to use filename, or 'project: \"Name\"' for explicit name.",
			),
		)
		.addText((text) => {
			text.setPlaceholder("project")
				.setValue(
					projectConfig.metadataConfig?.metadataKey || "project",
				)
				.onChange(async (value) => {
					if (projectConfig.metadataConfig) {
						projectConfig.metadataConfig.metadataKey =
							value || "project";
						await settingTab.plugin.saveSettings();
					}
				});
		});

	// ============================================================
	// Section 5: Source 3 - Config File-based Project Detection
	// ============================================================
	/**
	 * Config file detection configuration section
	 *
	 * ## Functionality
	 * Place a special configuration file (default: project.md) in a folder,
	 * and all files in that folder will be automatically assigned to the
	 * project defined in that config file.
	 *
	 * ## Use Case
	 * 1. Create `project.md` in `Projects/Work/` folder
	 * 2. Define in `project.md` frontmatter:
	 *    ```yaml
	 *    ---
	 *    project: Work Tasks
	 *    ---
	 *    ```
	 * 3. All tasks under `Projects/Work/` are automatically assigned to
	 *    "Work Tasks" project
	 *
	 * ## Design Philosophy
	 * This approach is especially suitable for users who organize projects
	 * by folder, eliminating the need to add metadata in each file.
	 */
	new Setting(containerEl)
		.setName(t("Source 3: Config File-based Detection"))
		.setDesc(
			t(
				"Place a configuration file (e.g., project.md) in a folder. All files in that folder will inherit the project defined in the config file's frontmatter.",
			),
		)
		.setHeading();

	// Config file detection toggle
	new Setting(containerEl)
		.setName(t("Enable config file detection"))
		.setDesc(
			t(
				"Look for project configuration files (e.g., project.md) in folders to determine project membership.",
			),
		)
		.addToggle((toggle) => {
			toggle
				.setValue(projectConfig.configFile?.enabled || false)
				.onChange(async (value) => {
					if (projectConfig.configFile) {
						projectConfig.configFile.enabled = value;
						await settingTab.plugin.saveSettings();
					}
				});
		});

	// Config file name
	new Setting(containerEl)
		.setName(t("Config file name"))
		.setDesc(
			t(
				"Name of the project configuration file to look for. The file should contain project settings in its frontmatter.",
			),
		)
		.addText((text) => {
			text.setPlaceholder("project.md")
				.setValue(projectConfig.configFile?.fileName || "project.md")
				.onChange(async (value) => {
					if (projectConfig.configFile) {
						projectConfig.configFile.fileName =
							value || "project.md";
						await settingTab.plugin.saveSettings();
					}
				});
		});

	// ============================================================
	// Section 6: Advanced - Custom Detection Methods
	// ============================================================
	/**
	 * Custom detection methods configuration section
	 *
	 * ## Functionality
	 * In addition to the three main data sources above, projects can also
	 * be detected through:
	 * - Metadata property: Read from any frontmatter field
	 * - Tags: Extract from specially formatted tags (e.g., #project/Work)
	 * - Links: Extract project info from file links
	 *
	 * ## Use Cases
	 * - Use `#project/Work` tag to mark task's project
	 * - Use `[[Projects/Work]]` link to associate task with project
	 *
	 * ## Important Notes
	 * This is an advanced feature that may conflict with primary detection
	 * methods. Enable only when needed.
	 */
	new Setting(containerEl)
		.setName(t("Advanced: Custom Detection Methods"))
		.setDesc(
			t(
				"Configure additional detection methods using tags (#project/name), links ([[Projects/Work]]), or custom metadata fields.",
			),
		)
		.setHeading();

	// Custom detection methods list container
	const detectionMethodsContainer = containerEl.createDiv({
		cls: "project-detection-methods-container",
	});

	/**
	 * Refreshes the custom detection methods list.
	 */
	const refreshDetectionMethods = () => {
		detectionMethodsContainer.empty();

		// Ensure detectionMethods array exists
		if (!projectConfig.metadataConfig?.detectionMethods) {
			if (projectConfig.metadataConfig) {
				projectConfig.metadataConfig.detectionMethods = [];
			}
		}

		const methods = projectConfig.metadataConfig?.detectionMethods || [];

		// Render each detection method
		methods.forEach((method, index) => {
			const methodDiv = detectionMethodsContainer.createDiv({
				cls: "project-detection-method",
			});

			new Setting(methodDiv)
				.setName(`${t("Method")} ${index + 1}`)
				.addDropdown((dropdown) => {
					dropdown
						.addOption("metadata", t("Metadata field"))
						.addOption("tag", t("Tag pattern"))
						.addOption("link", t("Link target"))
						.setValue(method.type)
						.onChange(async (value) => {
							method.type = value as "metadata" | "tag" | "link";
							await settingTab.plugin.saveSettings();
							refreshDetectionMethods();
						});
				})
				.addText((text) => {
					// Show different placeholder based on type
					const placeholder =
						method.type === "metadata"
							? t("Field name (e.g., project)")
							: method.type === "tag"
								? t("Tag prefix (e.g., project)")
								: t("Link filter (e.g., Projects/)");
					text.setPlaceholder(placeholder)
						.setValue(method.propertyKey)
						.onChange(async (value) => {
							method.propertyKey = value;
							await settingTab.plugin.saveSettings();
						});
				})
				.addToggle((toggle) => {
					toggle.setValue(method.enabled).onChange(async (value) => {
						method.enabled = value;
						await settingTab.plugin.saveSettings();
					});
				})
				.addButton((button) => {
					button
						.setIcon("trash")
						.setTooltip(t("Delete this method"))
						.onClick(async () => {
							methods.splice(index, 1);
							await settingTab.plugin.saveSettings();
							refreshDetectionMethods();
						});
				});

			// Show additional filter config for link type
			if (method.type === "link") {
				new Setting(methodDiv)
					.setName(t("Link path filter"))
					.setDesc(t("Only match links containing this path"))
					.addText((text) => {
						text.setPlaceholder("Projects/")
							.setValue(method.linkFilter || "")
							.onChange(async (value) => {
								method.linkFilter = value;
								await settingTab.plugin.saveSettings();
							});
					});
			}
		});

		// Add new method button
		new Setting(detectionMethodsContainer).addButton((button) => {
			button
				.setButtonText(t("Add detection method"))
				.setCta()
				.onClick(async () => {
					if (projectConfig.metadataConfig?.detectionMethods) {
						projectConfig.metadataConfig.detectionMethods.push({
							type: "metadata",
							propertyKey: "",
							enabled: false,
						});
					} else if (projectConfig.metadataConfig) {
						projectConfig.metadataConfig.detectionMethods = [
							{
								type: "metadata",
								propertyKey: "",
								enabled: false,
							},
						];
					}
					await settingTab.plugin.saveSettings();
					refreshDetectionMethods();
				});
		});
	};

	refreshDetectionMethods();

	// ============================================================
	// Section 7: Advanced - Metadata Field Mapping
	// ============================================================
	/**
	 * Metadata mapping configuration section
	 *
	 * ## Functionality
	 * Maps custom frontmatter fields to standard task properties.
	 * This allows users to use their preferred field names while the plugin
	 * internally uses standard properties.
	 *
	 * ## Use Cases
	 * - Map `proj` to `project` (shorthand support)
	 * - Map `ddl` to `dueDate` (custom naming)
	 * - Map `assignee` to `context` (semantic mapping)
	 *
	 * ## Available Target Fields
	 * project, context, priority, tags, startDate, scheduledDate,
	 * dueDate, completedDate, createdDate, recurrence
	 */
	new Setting(containerEl)
		.setName(t("Advanced: Metadata Field Mapping"))
		.setDesc(
			t(
				"Map custom frontmatter fields to standard Task Genius properties. Useful for custom naming, i18n, or integrating with existing vault structures.",
			),
		)
		.setHeading();

	// Metadata mappings list container
	const metadataMappingsContainer = containerEl.createDiv({
		cls: "project-metadata-mappings-container",
	});

	/**
	 * Refreshes the metadata mappings list.
	 */
	const refreshMetadataMappings = () => {
		metadataMappingsContainer.empty();

		// Ensure metadataMappings array exists
		if (
			!projectConfig.metadataMappings ||
			!Array.isArray(projectConfig.metadataMappings)
		) {
			projectConfig.metadataMappings = [];
		}

		const metadataMappings = projectConfig.metadataMappings;

		// Empty state message
		if (metadataMappings.length === 0) {
			metadataMappingsContainer.createDiv({
				cls: "no-mappings-message",
				text: t(
					"No field mappings yet. Add a mapping to use custom frontmatter field names.",
				),
			});
		}

		// Render each mapping rule
		metadataMappings.forEach((mapping, index) => {
			const mappingRow = metadataMappingsContainer.createDiv({
				cls: "project-metadata-mapping-row",
			});

			// Get already used target keys (avoid duplicate mappings)
			const usedTargetKeys = new Set(
				metadataMappings
					.filter((_, i) => i !== index)
					.map((m) => m.targetKey)
					.filter((key) => key && key.trim() !== ""),
			);

			// Available standard fields list
			const availableTargetKeys = [
				"project",
				"context",
				"priority",
				"tags",
				"startDate",
				"scheduledDate",
				"dueDate",
				"completedDate",
				"createdDate",
				"recurrence",
			].filter(
				(key) => !usedTargetKeys.has(key) || key === mapping.targetKey,
			);

			new Setting(mappingRow)
				.setName(`${t("Mapping")} ${index + 1}`)
				.addText((text) => {
					text.setPlaceholder(t("Your field name (e.g., proj)"))
						.setValue(mapping.sourceKey)
						.onChange(async (value) => {
							projectConfig.metadataMappings[index].sourceKey =
								value;
							await settingTab.plugin.saveSettings();
						});
				})
				.addDropdown((dropdown) => {
					dropdown.addOption("", t("→ Standard field"));
					availableTargetKeys.forEach((key) => {
						dropdown.addOption(key, key);
					});
					dropdown
						.setValue(mapping.targetKey)
						.onChange(async (value) => {
							projectConfig.metadataMappings[index].targetKey =
								value;
							await settingTab.plugin.saveSettings();
							refreshMetadataMappings();
						});
				})
				.addToggle((toggle) => {
					toggle
						.setTooltip(t("Enable this mapping"))
						.setValue(mapping.enabled)
						.onChange(async (value) => {
							projectConfig.metadataMappings[index].enabled =
								value;
							await settingTab.plugin.saveSettings();
						});
				})
				.addButton((button) => {
					button
						.setIcon("trash")
						.setTooltip(t("Delete this mapping"))
						.onClick(async () => {
							projectConfig.metadataMappings.splice(index, 1);
							await settingTab.plugin.saveSettings();
							refreshMetadataMappings();
						});
				});
		});

		// Add new mapping button
		new Setting(metadataMappingsContainer).addButton((button) => {
			button
				.setButtonText(t("Add field mapping"))
				.setCta()
				.onClick(async () => {
					projectConfig.metadataMappings.push({
						sourceKey: "",
						targetKey: "",
						enabled: true,
					});
					await settingTab.plugin.saveSettings();
					setTimeout(() => {
						refreshMetadataMappings();
					}, 100);
				});
		});
	};

	refreshMetadataMappings();

	// ============================================================
	// Section 8: Fallback - Default Project Naming
	// ============================================================
	/**
	 * Default project naming configuration section
	 *
	 * ## Functionality
	 * When all detection methods fail to determine a task's project,
	 * use this fallback strategy to automatically generate a project name.
	 *
	 * ## Available Strategies
	 * - filename: Use current filename as project name
	 * - foldername: Use current folder name as project name
	 * - metadata: Use specified frontmatter field value as project name
	 *
	 * ## Use Cases
	 * - Want each file to be its own project: choose filename
	 * - Want to organize projects by folder: choose foldername
	 * - Need more flexible control: choose metadata
	 */
	new Setting(containerEl)
		.setName(t("Fallback: Default Project Naming"))
		.setDesc(
			t(
				"When all detection methods fail, use this fallback strategy to automatically generate a project name from filename, folder name, or metadata.",
			),
		)
		.setHeading();

	// Ensure default naming config exists
	if (!projectConfig.defaultProjectNaming) {
		projectConfig.defaultProjectNaming = {
			strategy: "filename",
			stripExtension: true,
			enabled: false,
		};
	}

	// Default naming toggle
	new Setting(containerEl)
		.setName(t("Enable fallback naming"))
		.setDesc(
			t(
				"Generate a default project name when no project is detected. If disabled, tasks without detected projects will have no project assigned.",
			),
		)
		.addToggle((toggle) => {
			toggle
				.setValue(projectConfig.defaultProjectNaming?.enabled || false)
				.onChange(async (value) => {
					if (projectConfig.defaultProjectNaming) {
						projectConfig.defaultProjectNaming.enabled = value;
						await settingTab.plugin.saveSettings();
						setTimeout(() => {
							settingTab.display();
						}, 200);
					}
				});
		});

	// Naming strategy selection
	new Setting(containerEl)
		.setName(t("Naming strategy"))
		.setDesc(t("How to generate the default project name"))
		.addDropdown((dropdown) => {
			dropdown
				.addOption("filename", t("Use file name"))
				.addOption("foldername", t("Use folder name"))
				.addOption("metadata", t("Use metadata field"))
				.setValue(
					projectConfig.defaultProjectNaming?.strategy || "filename",
				)
				.onChange(async (value) => {
					if (projectConfig.defaultProjectNaming) {
						projectConfig.defaultProjectNaming.strategy = value as
							| "filename"
							| "foldername"
							| "metadata";
						await settingTab.plugin.saveSettings();
						setTimeout(() => {
							settingTab.display();
						}, 200);
					}
				});
		});

	// Show additional config based on strategy
	if (projectConfig.defaultProjectNaming?.strategy === "metadata") {
		// Metadata strategy: show field name config
		new Setting(containerEl)
			.setName(t("Metadata field for default name"))
			.setDesc(t("Frontmatter field to use as the default project name"))
			.addText((text) => {
				text.setPlaceholder(t("e.g., category"))
					.setValue(
						projectConfig.defaultProjectNaming?.metadataKey || "",
					)
					.onChange(async (value) => {
						if (projectConfig.defaultProjectNaming) {
							projectConfig.defaultProjectNaming.metadataKey =
								value;
							await settingTab.plugin.saveSettings();
						}
					});
			});
	}

	if (projectConfig.defaultProjectNaming?.strategy === "filename") {
		// Filename strategy: show extension handling config
		new Setting(containerEl)
			.setName(t("Remove file extension"))
			.setDesc(
				t(
					"Remove the file extension (.md) from the filename when using as project name",
				),
			)
			.addToggle((toggle) => {
				toggle
					.setValue(
						projectConfig.defaultProjectNaming?.stripExtension ??
							true,
					)
					.onChange(async (value) => {
						if (projectConfig.defaultProjectNaming) {
							projectConfig.defaultProjectNaming.stripExtension =
								value;
							await settingTab.plugin.saveSettings();
						}
					});
			});
	}
}
