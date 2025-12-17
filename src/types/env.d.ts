/**
 * Environment variable type declarations
 * These values are injected at build time via esbuild's define option
 */
declare namespace NodeJS {
	interface ProcessEnv {
		/** Base64-encoded Google OAuth client secret */
		GOOGLE_CLIENT_SECRET_B64?: string;
	}
}
