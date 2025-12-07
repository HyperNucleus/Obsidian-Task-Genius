/**
 * Filter Metadata Extractor
 *
 * Extracts task metadata from filter state for auto-filling when creating new tasks.
 * This enables context-aware task creation based on active filters.
 */

import { moment } from "obsidian";
import type { RootFilterState, FilterGroup, Filter } from "./ViewTaskFilter";
import type { TaskMetadata } from "../../quick-capture/modals/BaseQuickCaptureModal";

/**
 * Extracts task metadata from the current filter state.
 *
 * Strategy:
 * - For `rootCondition === 'all'`: Merge metadata from all filter groups (union)
 * - For `rootCondition === 'any'`: Only extract metadata that is common across all groups (intersection)
 * - For `rootCondition === 'none'`: Return empty metadata (not suitable for pre-filling)
 *
 * @param filterState - The current root filter state
 * @returns TaskMetadata object with extracted values
 */
export function extractMetadataFromFilter(
	filterState: RootFilterState | null | undefined,
): TaskMetadata {
	const metadata: TaskMetadata = {};

	if (
		!filterState ||
		!filterState.filterGroups ||
		filterState.filterGroups.length === 0
	) {
		return metadata;
	}

	// 'none' condition is not suitable for pre-filling
	if (filterState.rootCondition === "none") {
		return metadata;
	}

	if (filterState.rootCondition === "all") {
		// Strategy: Union/Merge
		// For single-value fields, later groups override earlier ones (Last Write Wins)
		// For multi-value fields (tags), merge all occurrences
		filterState.filterGroups.forEach((group) => {
			const groupMeta = extractFromGroup(group);
			mergeMetadata(metadata, groupMeta);
		});
	} else if (filterState.rootCondition === "any") {
		// Strategy: Intersection
		// Only extract metadata that is common across ALL filter groups
		const allGroupsMeta = filterState.filterGroups.map(extractFromGroup);

		if (allGroupsMeta.length > 0) {
			const potential = allGroupsMeta[0];

			// Check single-value fields
			const singleFields: (keyof TaskMetadata)[] = [
				"priority",
				"project",
				"status",
				"context",
			];

			singleFields.forEach((field) => {
				const val = potential[field];
				if (val !== undefined) {
					const allMatch = allGroupsMeta.every(
						(g) => g[field] === val,
					);
					if (allMatch) {
						(metadata as Record<string, unknown>)[field] = val;
					}
				}
			});

			// Check date fields (compare timestamps)
			const dateFields: (keyof TaskMetadata)[] = [
				"startDate",
				"dueDate",
				"scheduledDate",
			];
			dateFields.forEach((field) => {
				const val = potential[field];
				if (val instanceof Date) {
					const allMatch = allGroupsMeta.every((g) => {
						const gVal = g[field];
						return (
							gVal instanceof Date &&
							gVal.getTime() === val.getTime()
						);
					});
					if (allMatch) {
						(metadata as Record<string, unknown>)[field] = val;
					}
				}
			});

			// Check tags (array intersection)
			if (potential.tags && potential.tags.length > 0) {
				const commonTags = potential.tags.filter((tag) => {
					return allGroupsMeta.every(
						(g) => g.tags && g.tags.includes(tag),
					);
				});
				if (commonTags.length > 0) {
					metadata.tags = commonTags;
				}
			}
		}
	}

	return metadata;
}

/**
 * Extracts metadata from a single filter group.
 * Only processes groups with `groupCondition === 'all'` for reliable extraction.
 */
function extractFromGroup(group: FilterGroup): TaskMetadata {
	const groupMeta: TaskMetadata = {};

	// Only extract from 'all' (AND) groups for reliable pre-filling
	if (group.groupCondition !== "all") {
		return groupMeta;
	}

	group.filters.forEach((filter) => {
		extractFromFilter(filter, groupMeta);
	});

	return groupMeta;
}

/**
 * Extracts metadata from a single filter condition.
 * Only extracts from conditions that provide definite values (e.g., 'is', 'contains').
 */
function extractFromFilter(filter: Filter, metadata: TaskMetadata): void {
	if (!filter.property || !filter.condition) {
		return;
	}

	const val = filter.value;
	// Ignore empty values
	if (val === undefined || val === "" || val === null) {
		return;
	}

	switch (filter.property) {
		case "priority":
			if (filter.condition === "is") {
				const parsed = parseInt(val, 10);
				if (!isNaN(parsed)) {
					metadata.priority = parsed;
				}
			}
			break;

		case "project":
			if (filter.condition === "is") {
				metadata.project = String(val).trim();
			}
			break;

		case "status":
			if (filter.condition === "is") {
				metadata.status = String(val).trim();
			}
			break;

		case "tags":
			if (filter.condition === "contains" || filter.condition === "is") {
				if (!metadata.tags) {
					metadata.tags = [];
				}
				const tagVal = String(val).trim();
				if (tagVal && !metadata.tags.includes(tagVal)) {
					metadata.tags.push(tagVal);
				}
			}
			break;

		case "dueDate":
			if (filter.condition === "is") {
				const date = parseFilterDate(val);
				if (date) {
					metadata.dueDate = date;
				}
			}
			break;

		case "startDate":
			if (filter.condition === "is") {
				const date = parseFilterDate(val);
				if (date) {
					metadata.startDate = date;
				}
			}
			break;

		case "scheduledDate":
			if (filter.condition === "is") {
				const date = parseFilterDate(val);
				if (date) {
					metadata.scheduledDate = date;
				}
			}
			break;
	}
}

/**
 * Merges source metadata into target metadata.
 * Single-value fields are overwritten; multi-value fields (tags) are merged.
 */
function mergeMetadata(target: TaskMetadata, source: TaskMetadata): void {
	// Single-value fields: overwrite
	if (source.priority !== undefined) target.priority = source.priority;
	if (source.project !== undefined) target.project = source.project;
	if (source.status !== undefined) target.status = source.status;
	if (source.context !== undefined) target.context = source.context;
	if (source.startDate !== undefined) target.startDate = source.startDate;
	if (source.dueDate !== undefined) target.dueDate = source.dueDate;
	if (source.scheduledDate !== undefined)
		target.scheduledDate = source.scheduledDate;

	// Multi-value fields: merge and deduplicate
	if (source.tags) {
		if (!target.tags) {
			target.tags = [];
		}
		source.tags.forEach((t) => {
			if (!target.tags!.includes(t)) {
				target.tags!.push(t);
			}
		});
	}
}

/**
 * Parses a date string from filter value.
 * Supports ISO format and YYYY-MM-DD.
 */
function parseFilterDate(dateStr: unknown): Date | undefined {
	if (typeof dateStr !== "string") {
		return undefined;
	}

	// Strict parsing for ISO and YYYY-MM-DD formats
	const m = moment(dateStr, ["YYYY-MM-DD", "YYYY-MM-DDTHH:mm"], true);
	if (m.isValid()) {
		return m.toDate();
	}

	return undefined;
}
