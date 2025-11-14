export interface FluentTaskViewState {
	currentWorkspace: string;
	selectedProject?: string | null;
	viewMode: "list" | "kanban" | "tree" | "calendar";
	viewModeByViewId?: Record<
		string,
		"list" | "kanban" | "tree" | "calendar"
	>;
	searchQuery?: string;
	filterInputValue?: string;
	filters?: any;
}

export type FluentTaskNavigationItem = {
	id: string;
	label: string;
	icon: string;
	type: "primary" | "project" | "other";
	action?: () => void;
	badge?: number;
};

/**
 * Error context for structured error display
 */
export interface ErrorContext {
	/** View ID (inbox, today, projects, etc.) */
	viewId?: string;
	/** Component name (ContentComponent, KanbanComponent, etc.) */
	componentName?: string;
	/** Operation description (e.g., "Loading tasks", "Switching view") */
	operation?: string;
	/** File path where error occurred */
	filePath?: string;
	/** Original error object */
	originalError?: Error;
	/** User-friendly error message */
	userMessage?: string;
}
