/**
 * Utility functions for date picker functionality
 * Provides precise date matching using syntax tree and task line detection
 */

import { EditorState, Text, Line } from "@codemirror/state";
// @ts-ignore - This import is necessary but TypeScript can't find it
import { syntaxTree } from "@codemirror/language";

/**
 * Whitelist of date emoji markers to prevent false positives
 */
export const DATE_EMOJI_WHITELIST = [
	"📅", // Calendar
	"🚀", // Rocket (start date)
	"✅", // Check mark (completed)
	"❌", // Cross mark (cancelled)
	"🛫", // Airplane departure (scheduled)
	"⏰", // Alarm clock (due)
	"🏁", // Checkered flag (deadline)
	"▶️", // Play button (start)
];

/**
 * Interface representing a matched date in the document
 */
export interface DateMatch {
	from: number; // Absolute position in document
	to: number; // Absolute position in document
	dateText: string; // YYYY-MM-DD format
	marker: string; // Emoji or [field:: prefix
	fullMatch: string; // Complete matched text
}

/**
 * Interface for widget information tracking
 */
export interface WidgetInfo {
	id: string;
	match: DateMatch;
	lineNumber: number;
	offsetInLine: number;
	lastValidated: number;
}

/**
 * Check if a position is inside a task line using syntax tree
 * @param state Editor state
 * @param pos Position to check
 * @returns true if inside a task line
 */
export function isInsideTaskLine(state: EditorState, pos: number): boolean {
	try {
		// Get the line at this position first
		const line = state.doc.lineAt(pos);
		const lineText = line.text;

		// Quick check: must match task syntax
		const taskRegex = /^\s*[-*+]\s*\[.]/;
		if (!taskRegex.test(lineText)) {
			return false;
		}

		// Use syntax tree for more precise detection
		const tree = syntaxTree(state);
		let node: any = tree.resolveInner(pos, -1);

		// Traverse up to find list-item node
		while (node) {
			if (node.name === "list-item" || node.name.includes("list")) {
				return true;
			}
			node = node.parent;
		}

		// Fallback: if syntax tree doesn't help, rely on regex
		return taskRegex.test(lineText);
	} catch (e) {
		console.warn("Error checking task line:", e);
		return false;
	}
}

/**
 * Find the task line at a given position
 * @param state Editor state
 * @param pos Position to check
 * @returns Task line info or null
 */
export function findTaskLineAt(
	state: EditorState,
	pos: number
): { from: number; to: number; text: string; line: Line } | null {
	try {
		if (!isInsideTaskLine(state, pos)) {
			return null;
		}

		const line = state.doc.lineAt(pos);
		return {
			from: line.from,
			to: line.to,
			text: line.text,
			line: line,
		};
	} catch (e) {
		console.warn("Error finding task line:", e);
		return null;
	}
}

/**
 * Validate if a date string is in correct format
 * Prevents malformed dates like "2024-12-25-25" from being matched
 * @param dateStr Date string to validate
 * @returns true if valid
 */
function isValidDateFormat(dateStr: string): boolean {
	// Must match YYYY-MM-DD or YYYY-MM-DD HH:MM or YYYY-MM-DD HH:MM:SS
	const validDateRegex = /^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?$/;

	if (!validDateRegex.test(dateStr)) {
		return false;
	}

	// Additional validation: ensure there are no extra dashes or numbers after the date
	const parts = dateStr.split(/[\s:]/);
	const datePart = parts[0];
	const dateComponents = datePart.split("-");

	// Must have exactly 3 components for date (year, month, day)
	if (dateComponents.length !== 3) {
		return false;
	}

	// Validate ranges
	const year = parseInt(dateComponents[0], 10);
	const month = parseInt(dateComponents[1], 10);
	const day = parseInt(dateComponents[2], 10);

	if (year < 1900 || year > 2100) return false;
	if (month < 1 || month > 12) return false;
	if (day < 1 || day > 31) return false;

	return true;
}

/**
 * Find all dates in a task line
 * @param lineText Task line text
 * @param lineStart Absolute start position of the line
 * @param preferDataview Whether to use dataview format
 * @returns Array of date matches
 */
export function findDatesInTaskLine(
	lineText: string,
	lineStart: number,
	preferDataview: boolean
): DateMatch[] {
	const matches: DateMatch[] = [];

	try {
		if (preferDataview) {
			// Dataview format: [field:: YYYY-MM-DD]
			// Match common field names for dates
			const dataviewRegex =
				/\[([a-zA-Z]+)::\s*(\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)\]/g;
			let match: RegExpExecArray | null;

			while ((match = dataviewRegex.exec(lineText)) !== null) {
				const dateStr = match[2];

				matches.push({
					from: lineStart + match.index,
					to: lineStart + match.index + match[0].length,
					dateText: dateStr,
					marker: `[${match[1]}::`,
					fullMatch: match[0],
				});
			}
		} else {
			// Emoji format: use whitelist to prevent false positives
			for (const emoji of DATE_EMOJI_WHITELIST) {
				const escapedEmoji = escapeRegex(emoji);
				// Strict regex: date must be followed by word boundary or space
				// This prevents matching "2024-12-25-25"
				const emojiRegex = new RegExp(
					`${escapedEmoji}\\s*(\\d{4}-\\d{2}-\\d{2}(?:\\s+\\d{2}:\\d{2}(?::\\d{2})?)?)(?=\\s|$|\\])`,
					"g"
				);
				let match: RegExpExecArray | null;

				while ((match = emojiRegex.exec(lineText)) !== null) {
					const dateStr = match[1];

					matches.push({
						from: lineStart + match.index,
						to: lineStart + match.index + match[0].length,
						dateText: dateStr,
						marker: emoji,
						fullMatch: match[0],
					});
				}
			}
		}
	} catch (e) {
		console.warn("Error finding dates in task line:", e);
	}

	// Filter out overlapping matches (keep the first one)
	const filtered: DateMatch[] = [];
	for (const match of matches) {
		const overlaps = filtered.some(
			(existing) =>
				(match.from >= existing.from && match.from < existing.to) ||
				(match.to > existing.from && match.to <= existing.to)
		);
		if (!overlaps) {
			filtered.push(match);
		}
	}

	return filtered;
}

/**
 * Generate a unique ID for a widget based on its match
 * @param match Date match
 * @param lineNumber Line number
 * @returns Unique widget ID
 */
export function generateWidgetId(match: DateMatch, lineNumber: number): string {
	// Use line number + marker + offset for stable ID
	const offset = match.from;
	return `widget-${lineNumber}-${encodeURIComponent(match.marker)}-${offset}`;
}

/**
 * Validate that a position range is valid in the document
 * @param state Editor state
 * @param from Start position
 * @param to End position
 * @returns true if valid
 */
export function isValidPosition(
	state: EditorState,
	from: number,
	to: number
): boolean {
	try {
		// Basic range checks
		if (from < 0 || to < 0) {
			return false;
		}

		if (from > state.doc.length || to > state.doc.length) {
			return false;
		}

		if (from >= to) {
			return false;
		}

		// Ensure positions are on the same line
		const fromLine = state.doc.lineAt(from);
		const toLine = state.doc.lineAt(to);

		if (fromLine.number !== toLine.number) {
			return false;
		}

		return true;
	} catch (e) {
		return false;
	}
}

/**
 * Safely get a line by number
 * @param doc Document
 * @param lineNumber Line number (1-based)
 * @returns Line or null if invalid
 */
export function safeGetLine(doc: Text, lineNumber: number): Line | null {
	try {
		if (lineNumber < 1 || lineNumber > doc.lines) {
			return null;
		}
		return doc.line(lineNumber);
	} catch (e) {
		return null;
	}
}

/**
 * Escape special regex characters
 * @param str String to escape
 * @returns Escaped string
 */
export function escapeRegex(str: string): string {
	return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Get affected line numbers from a change set
 * @param state Editor state
 * @param changes Change set
 * @returns Set of affected line numbers
 */
export function getAffectedLineNumbers(
	state: EditorState,
	changes: {
		iterChangedRanges: (
			f: (fromA: number, toA: number, fromB: number, toB: number) => void
		) => void;
	}
): Set<number> {
	const lines = new Set<number>();

	try {
		changes.iterChangedRanges((fromA, toA, fromB, toB) => {
			try {
				// Get line numbers in the new document
				const startLine = state.doc.lineAt(fromB).number;
				const endLine = state.doc.lineAt(
					Math.min(toB, state.doc.length)
				).number;

				for (let i = startLine; i <= endLine; i++) {
					lines.add(i);
				}
			} catch (e) {
				// Ignore errors for individual ranges
			}
		});
	} catch (e) {
		console.warn("Error getting affected lines:", e);
	}

	return lines;
}

/**
 * Check if a node is inside a code block or frontmatter
 * @param state Editor state
 * @param from Start position
 * @param to End position
 * @returns true if should skip rendering
 */
export function shouldSkipRendering(
	state: EditorState,
	from: number,
	to: number
): boolean {
	try {
		const tree = syntaxTree(state);
		const node = tree.resolveInner(from, 1);

		// Check node and parent nodes for special contexts
		let current: any = node;
		while (current) {
			const nodeName = current.name.toLowerCase();
			const nodeType = current.type.name.toLowerCase();

			// Skip code blocks
			if (
				nodeName.includes("code") ||
				nodeName.includes("fenced") ||
				nodeType.includes("code")
			) {
				return true;
			}

			// Skip frontmatter
			if (
				nodeName.includes("frontmatter") ||
				nodeType.includes("frontmatter")
			) {
				return true;
			}

			current = current.parent;
		}

		return false;
	} catch (e) {
		// On error, default to rendering (better than breaking)
		return false;
	}
}
