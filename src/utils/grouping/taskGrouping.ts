/**
 * Task grouping utilities
 * Core logic for grouping tasks by different dimensions
 */

import { Task } from "@/types/task";
import { GroupByDimension, TaskGroup } from "@/types/groupBy";
import { t } from "@/translations/helper";

/**
 * Default status mark to display name mapping
 * Based on common task management conventions and Tasks plugin compatibility
 * Used as fallback when status mark is not found in user's taskStatusMarks configuration
 */
const DEFAULT_STATUS_NAMES: Record<string, string> = {
	// Standard statuses
	" ": "Todo",           // Incomplete/Not started
	"x": "Done",           // Completed (lowercase)
	"X": "Done",           // Completed (uppercase)
	"-": "Cancelled",      // Cancelled/Abandoned

	// Progress indicators
	"/": "In Progress",    // Half-done/In progress
	">": "Forwarded",      // Deferred/Forwarded
	"<": "Scheduled",      // Scheduled for later

	// Priority/importance markers
	"!": "Important",      // Important/High priority
	"*": "Star",           // Starred/Favorite

	// Question/tentative
	"?": "Question",       // Uncertain/Question

	// Extended status marks (from various task management systems)
	"i": "Info",           // Information
	"I": "Idea",           // Idea
	"S": "Started",        // Started
	"p": "Pro",            // Pro/Professional
	"c": "Choice",         // Choice
	"l": "Location",       // Location-based
	"b": "Bookmark",       // Bookmarked
	"f": "Fire",           // Urgent/Fire
	"k": "Key",            // Key task
	"w": "Win",            // Won/Achieved
	"u": "Up",             // Ongoing/Up
	"d": "Down",           // Deprecated/Down
	'"': "Quote",          // Quoted/Referenced
	"B": "Bug",            // Bug/Issue

	// Numeric progress indicators (0-5 scale)
	"0": "0/5",
	"1": "1/5",
	"2": "2/5",
	"3": "3/5",
	"4": "4/5",
	"5": "5/5",

	// Other
	"n": "Note",           // Note
};

/**
 * Format status key to readable text
 * Handles camelCase, snake_case, and kebab-case
 * @example formatStatusKey("IN_PROGRESS") => "In Progress"
 * @example formatStatusKey("inProgress") => "In Progress"
 */
function formatStatusKey(key: string): string {
	return key
		.replace(/([a-z])([A-Z])/g, "$1 $2") // camelCase -> camel Case
		.replace(/[-_]+/g, " ") // snake_case/kebab-case -> spaces
		.split(" ")
		.filter(Boolean)
		.map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
		.join(" ");
}

/**
 * Convert status mark to display name using task status marks mapping
 * Performs reverse lookup in taskStatusMarks to find the configured status name
 * Falls back to default status names, then formatting if no mapping is found
 *
 * Lookup priority:
 * 1. User-configured taskStatusMarks (highest priority)
 * 2. DEFAULT_STATUS_NAMES (common status marks)
 * 3. formatStatusKey (generic formatting fallback)
 *
 * @param mark - Status mark from task (e.g., "x", ">", "-", " ")
 * @param taskStatusMarks - Status marks mapping from plugin settings
 * @returns Display name (e.g., "Completed", "In Progress", "Abandoned")
 *
 * @example
 * // With taskStatusMarks = { "Completed": "x", "In Progress": ">", "Abandoned": "-" }
 * getStatusDisplayName("x", taskStatusMarks) => "Completed"
 * getStatusDisplayName(">", taskStatusMarks) => "In Progress"
 * getStatusDisplayName("/", taskStatusMarks) => "In Progress" (from DEFAULT_STATUS_NAMES)
 * getStatusDisplayName("unknown", taskStatusMarks) => "Unknown" (from formatStatusKey)
 */
function getStatusDisplayName(
	mark: string,
	taskStatusMarks?: Record<string, string>
): string {
	// Priority 1: User-configured taskStatusMarks
	if (taskStatusMarks) {
		for (const [statusName, statusMark] of Object.entries(taskStatusMarks)) {
			// Match both exact and case-insensitive
			if (statusMark === mark ||
			    (statusMark && statusMark.toLowerCase() === mark.toLowerCase())) {
				return statusName;
			}
		}
	}

	// Priority 2: Check default status names mapping
	if (mark in DEFAULT_STATUS_NAMES) {
		return DEFAULT_STATUS_NAMES[mark];
	}

	// Priority 3: Fallback formatting for unknown marks
	// Handle empty/whitespace marks
	if (!mark || mark.trim().length === 0) {
		return DEFAULT_STATUS_NAMES[" "] || "Todo";
	}

	// Use generic formatting for completely unknown marks
	return formatStatusKey(mark);
}

/**
 * Main grouping function - dispatches to specific grouping strategies
 */
export function groupTasksBy(
	tasks: Task[],
	dimension: GroupByDimension,
	taskStatusMarks?: Record<string, string>
): TaskGroup[] {
	switch (dimension) {
		case "none":
			return groupTasksNone(tasks);
		case "filePath":
			return groupTasksByFilePath(tasks);
		case "dueDate":
			return groupTasksByDueDate(tasks);
		case "priority":
			return groupTasksByPriority(tasks);
		case "project":
			return groupTasksByProject(tasks);
		case "tags":
			return groupTasksByTags(tasks);
		case "status":
			return groupTasksByStatus(tasks, taskStatusMarks);
		default:
			return groupTasksNone(tasks);
	}
}

/**
 * No grouping - return all tasks in a single group
 */
function groupTasksNone(tasks: Task[]): TaskGroup[] {
	if (tasks.length === 0) return [];

	return [
		{
			title: t("All Tasks"),
			key: "all",
			sortOrder: 0,
			tasks: tasks,
			isExpanded: true,
		},
	];
}

/**
 * Group tasks by file path (including filename)
 */
export function groupTasksByFilePath(tasks: Task[]): TaskGroup[] {
	const groupMap = new Map<string, Task[]>();

	tasks.forEach((task) => {
		const filePath =
			task.metadata.filePath || task.filePath || t("Unknown File");

		// Use full file path as grouping key
		if (!groupMap.has(filePath)) {
			groupMap.set(filePath, []);
		}
		groupMap.get(filePath)!.push(task);
	});

	// Convert to TaskGroup array and sort alphabetically
	const groups: TaskGroup[] = [];
	const sortedKeys = Array.from(groupMap.keys()).sort((a, b) => {
		// Sort by full path to keep files in same folder together
		return a.localeCompare(b);
	});

	sortedKeys.forEach((key, index) => {
		// Extract filename for display, but keep folder path for context
		const lastSlashIndex = Math.max(
			key.lastIndexOf("/"),
			key.lastIndexOf("\\")
		);

		let displayTitle: string;
		if (lastSlashIndex >= 0) {
			const folder = key.substring(0, lastSlashIndex);
			const filename = key.substring(lastSlashIndex + 1);

			// Remove file extension (only the last one, e.g., "file.backup.md" -> "file.backup")
			const lastDotIndex = filename.lastIndexOf(".");
			const fileBaseName = lastDotIndex > 0
				? filename.substring(0, lastDotIndex)
				: filename;

			// Show filename prominently, with folder path in subdued style
			displayTitle = folder
				? `${fileBaseName} (${folder})`
				: fileBaseName;
		} else {
			// File at root, remove extension if present
			const lastDotIndex = key.lastIndexOf(".");
			displayTitle = lastDotIndex > 0
				? key.substring(0, lastDotIndex)
				: key;
		}

		groups.push({
			title: displayTitle,
			key: `file-${key}`,
			sortOrder: index,
			tasks: groupMap.get(key)!,
			isExpanded: true,
		});
	});

	return groups;
}

/**
 * Extract base name from file path (without extension)
 * Helper function to avoid code duplication
 */
function extractFileBaseName(filePath: string): string {
	const lastSlashIndex = Math.max(
		filePath.lastIndexOf("/"),
		filePath.lastIndexOf("\\")
	);
	const fileName = filePath.substring(lastSlashIndex + 1);
	const lastDotIndex = fileName.lastIndexOf(".");
	return lastDotIndex > 0
		? fileName.substring(0, lastDotIndex)
		: fileName;
}

/**
 * Group tasks by file path with nested structure (folder -> file)
 * Creates a two-level hierarchy: folders at top level, files as children
 */
export function groupTasksByFilePathNested(tasks: Task[]): TaskGroup[] {
	// Map structure: folder path -> file paths -> tasks
	const folderMap = new Map<string, Map<string, Task[]>>();
	const rootFiles: Map<string, Task[]> = new Map(); // Files without folder

	// Step 1: Organize tasks by folder and file
	tasks.forEach((task) => {
		const filePath =
			task.metadata.filePath || task.filePath || t("Unknown File");

		const lastSlashIndex = Math.max(
			filePath.lastIndexOf("/"),
			filePath.lastIndexOf("\\")
		);

		if (lastSlashIndex >= 0) {
			// File is in a folder
			const folderPath = filePath.substring(0, lastSlashIndex);

			if (!folderMap.has(folderPath)) {
				folderMap.set(folderPath, new Map());
			}

			const fileMap = folderMap.get(folderPath)!;
			if (!fileMap.has(filePath)) {
				fileMap.set(filePath, []);
			}
			fileMap.get(filePath)!.push(task);
		} else {
			// File at root level
			if (!rootFiles.has(filePath)) {
				rootFiles.set(filePath, []);
			}
			rootFiles.get(filePath)!.push(task);
		}
	});

	// Step 2: Build nested TaskGroup structure
	const groups: TaskGroup[] = [];
	let sortOrder = 0;

	// Sort folders alphabetically
	const sortedFolders = Array.from(folderMap.keys()).sort((a, b) =>
		a.localeCompare(b)
	);

	// Create folder groups with file children
	sortedFolders.forEach((folderPath) => {
		const fileMap = folderMap.get(folderPath)!;
		const folderKey = `folder-${folderPath}`;

		// Create child groups for each file in the folder
		const children: TaskGroup[] = [];
		const sortedFiles = Array.from(fileMap.keys()).sort((a, b) =>
			a.localeCompare(b)
		);

		sortedFiles.forEach((filePath, fileIndex) => {
			children.push({
				title: extractFileBaseName(filePath),
				key: `file-${filePath}`,
				sortOrder: fileIndex,
				tasks: fileMap.get(filePath)!,
				isExpanded: false, // Default collapsed
				level: 1,
				parentKey: folderKey,
			});
		});

		// Calculate total tasks in this folder
		const folderTasks: Task[] = [];
		children.forEach((child) => folderTasks.push(...child.tasks));

		// Get folder display name (last segment of path)
		const folderSegments = folderPath.split(/[/\\]/);
		const folderDisplayName = folderSegments[folderSegments.length - 1] || folderPath;

		// Create folder group
		groups.push({
			title: folderDisplayName,
			key: folderKey,
			sortOrder: sortOrder++,
			tasks: folderTasks,
			isExpanded: false, // Default collapsed
			children: children,
			level: 0,
		});
	});

	// Add root-level files (if any) as a special group
	if (rootFiles.size > 0) {
		const rootChildren: TaskGroup[] = [];
		const sortedRootFiles = Array.from(rootFiles.keys()).sort((a, b) =>
			a.localeCompare(b)
		);

		sortedRootFiles.forEach((filePath, fileIndex) => {
			rootChildren.push({
				title: extractFileBaseName(filePath),
				key: `file-${filePath}`,
				sortOrder: fileIndex,
				tasks: rootFiles.get(filePath)!,
				isExpanded: false,
				level: 1,
				parentKey: "folder-root",
			});
		});

		const rootTasks: Task[] = [];
		rootChildren.forEach((child) => rootTasks.push(...child.tasks));

		// Add root folder group at the beginning
		groups.unshift({
			title: t("Root Files"),
			key: "folder-root",
			sortOrder: -1,
			tasks: rootTasks,
			isExpanded: false,
			children: rootChildren,
			level: 0,
		});
	}

	return groups;
}

/**
 * Group tasks by due date buckets
 */
export function groupTasksByDueDate(tasks: Task[]): TaskGroup[] {
	const today = new Date();
	today.setHours(0, 0, 0, 0);
	const todayTime = today.getTime();

	const tomorrow = new Date(today);
	tomorrow.setDate(tomorrow.getDate() + 1);
	const tomorrowTime = tomorrow.getTime();

	const nextWeek = new Date(today);
	nextWeek.setDate(nextWeek.getDate() + 7);
	const nextWeekTime = nextWeek.getTime();

	// Categorize tasks
	const pastDue: Task[] = [];
	const todayTasks: Task[] = [];
	const tomorrowTasks: Task[] = [];
	const thisWeekTasks: Task[] = [];
	const laterTasks: Task[] = [];
	const noDateTasks: Task[] = [];

	tasks.forEach((task) => {
		const dueDate = task.metadata.dueDate;
		if (!dueDate) {
			noDateTasks.push(task);
			return;
		}

		const date = new Date(dueDate);
		date.setHours(0, 0, 0, 0);
		const dateTime = date.getTime();

		if (dateTime < todayTime) {
			pastDue.push(task);
		} else if (dateTime === todayTime) {
			todayTasks.push(task);
		} else if (dateTime === tomorrowTime) {
			tomorrowTasks.push(task);
		} else if (dateTime <= nextWeekTime) {
			thisWeekTasks.push(task);
		} else {
			laterTasks.push(task);
		}
	});

	// Build groups in chronological order
	const groups: TaskGroup[] = [];
	let sortOrder = 0;

	if (pastDue.length > 0) {
		groups.push({
			title: t("Past Due"),
			key: "due-past",
			sortOrder: sortOrder++,
			tasks: pastDue,
			isExpanded: true,
		});
	}

	if (todayTasks.length > 0) {
		groups.push({
			title: t("Today"),
			key: "due-today",
			sortOrder: sortOrder++,
			tasks: todayTasks,
			isExpanded: true,
		});
	}

	if (tomorrowTasks.length > 0) {
		groups.push({
			title: t("Tomorrow"),
			key: "due-tomorrow",
			sortOrder: sortOrder++,
			tasks: tomorrowTasks,
			isExpanded: true,
		});
	}

	if (thisWeekTasks.length > 0) {
		groups.push({
			title: t("This Week"),
			key: "due-week",
			sortOrder: sortOrder++,
			tasks: thisWeekTasks,
			isExpanded: true,
		});
	}

	if (laterTasks.length > 0) {
		groups.push({
			title: t("Later"),
			key: "due-later",
			sortOrder: sortOrder++,
			tasks: laterTasks,
			isExpanded: true,
		});
	}

	if (noDateTasks.length > 0) {
		groups.push({
			title: t("No Due Date"),
			key: "due-none",
			sortOrder: sortOrder++,
			tasks: noDateTasks,
			isExpanded: true,
		});
	}

	return groups;
}

/**
 * Group tasks by priority level
 */
export function groupTasksByPriority(tasks: Task[]): TaskGroup[] {
	const highPriority: Task[] = [];
	const mediumPriority: Task[] = [];
	const lowPriority: Task[] = [];
	const noPriority: Task[] = [];

	tasks.forEach((task) => {
		const priority = task.metadata.priority || 0;
		if (priority === 3) {
			highPriority.push(task);
		} else if (priority === 2) {
			mediumPriority.push(task);
		} else if (priority === 1) {
			lowPriority.push(task);
		} else {
			noPriority.push(task);
		}
	});

	const groups: TaskGroup[] = [];
	let sortOrder = 0;

	if (highPriority.length > 0) {
		groups.push({
			title: t("High Priority"),
			key: "priority-high",
			sortOrder: sortOrder++,
			tasks: highPriority,
			isExpanded: true,
		});
	}

	if (mediumPriority.length > 0) {
		groups.push({
			title: t("Medium Priority"),
			key: "priority-medium",
			sortOrder: sortOrder++,
			tasks: mediumPriority,
			isExpanded: true,
		});
	}

	if (lowPriority.length > 0) {
		groups.push({
			title: t("Low Priority"),
			key: "priority-low",
			sortOrder: sortOrder++,
			tasks: lowPriority,
			isExpanded: true,
		});
	}

	if (noPriority.length > 0) {
		groups.push({
			title: t("No Priority"),
			key: "priority-none",
			sortOrder: sortOrder++,
			tasks: noPriority,
			isExpanded: true,
		});
	}

	return groups;
}

/**
 * Group tasks by project
 */
export function groupTasksByProject(tasks: Task[]): TaskGroup[] {
	const groupMap = new Map<string, Task[]>();

	tasks.forEach((task) => {
		const project = task.metadata.project || t("No Project");

		if (!groupMap.has(project)) {
			groupMap.set(project, []);
		}
		groupMap.get(project)!.push(task);
	});

	// Convert to TaskGroup array and sort alphabetically
	// Put "No Project" at the end
	const groups: TaskGroup[] = [];
	const sortedKeys = Array.from(groupMap.keys()).sort((a, b) => {
		if (a === t("No Project")) return 1;
		if (b === t("No Project")) return -1;
		return a.localeCompare(b);
	});

	sortedKeys.forEach((key, index) => {
		groups.push({
			title: key,
			key: `project-${key}`,
			sortOrder: index,
			tasks: groupMap.get(key)!,
			isExpanded: true,
		});
	});

	return groups;
}

/**
 * Group tasks by tags
 * Note: A task can appear in multiple groups if it has multiple tags
 */
export function groupTasksByTags(tasks: Task[]): TaskGroup[] {
	const groupMap = new Map<string, Task[]>();
	const noTagTasks: Task[] = [];

	tasks.forEach((task) => {
		const tags = task.metadata.tags || [];

		if (tags.length === 0) {
			noTagTasks.push(task);
		} else {
			tags.forEach((tag) => {
				// Remove leading '#' if present
				const cleanTag = tag.startsWith("#") ? tag.substring(1) : tag;

				if (!groupMap.has(cleanTag)) {
					groupMap.set(cleanTag, []);
				}
				groupMap.get(cleanTag)!.push(task);
			});
		}
	});

	// Convert to TaskGroup array and sort alphabetically
	const groups: TaskGroup[] = [];
	const sortedKeys = Array.from(groupMap.keys()).sort();

	sortedKeys.forEach((key, index) => {
		groups.push({
			title: `#${key}`,
			key: `tag-${key}`,
			sortOrder: index,
			tasks: groupMap.get(key)!,
			isExpanded: true,
		});
	});

	// Add "No Tags" group at the end
	if (noTagTasks.length > 0) {
		groups.push({
			title: t("No Tags"),
			key: "tag-none",
			sortOrder: groups.length,
			tasks: noTagTasks,
			isExpanded: true,
		});
	}

	return groups;
}

/**
 * Group tasks by status
 * Uses taskStatusMarks mapping to display user-configured status names
 *
 * @param tasks - Array of tasks to group
 * @param taskStatusMarks - Optional status marks mapping from plugin settings
 * @returns Array of task groups organized by status
 */
export function groupTasksByStatus(
	tasks: Task[],
	taskStatusMarks?: Record<string, string>
): TaskGroup[] {
	const groupMap = new Map<string, Task[]>();

	tasks.forEach((task) => {
		const status = task.status || "TODO";

		if (!groupMap.has(status)) {
			groupMap.set(status, []);
		}
		groupMap.get(status)!.push(task);
	});

	// Define status order
	const statusOrder = ["TODO", "IN_PROGRESS", "WAITING", "DONE", "CANCELLED"];

	// Convert to TaskGroup array and sort by defined order
	const groups: TaskGroup[] = [];
	const sortedKeys = Array.from(groupMap.keys()).sort((a, b) => {
		const indexA = statusOrder.indexOf(a);
		const indexB = statusOrder.indexOf(b);

		// If both are in the order array, sort by index
		if (indexA !== -1 && indexB !== -1) {
			return indexA - indexB;
		}
		// If only one is in the order array, prioritize it
		if (indexA !== -1) return -1;
		if (indexB !== -1) return 1;
		// Otherwise sort alphabetically
		return a.localeCompare(b);
	});

	sortedKeys.forEach((key, index) => {
		// Get display name using status marks mapping or fallback formatting
		const displayStatus = getStatusDisplayName(key, taskStatusMarks);

		groups.push({
			title: displayStatus,
			key: `status-${key}`,
			sortOrder: index,
			tasks: groupMap.get(key)!,
			isExpanded: true,
		});
	});

	return groups;
}

/**
 * Get display label for a grouping dimension
 */
export function getGroupByDimensionLabel(dimension: GroupByDimension): string {
	switch (dimension) {
		case "none":
			return t("None");
		case "filePath":
			return t("File Path");
		case "dueDate":
			return t("Due Date");
		case "priority":
			return t("Priority");
		case "project":
			return t("Project");
		case "tags":
			return t("Tags");
		case "status":
			return t("Status");
		default:
			return t("None");
	}
}
