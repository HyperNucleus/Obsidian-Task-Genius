#!/usr/bin/env node

import { execSync } from "child_process";
import { readFileSync } from "fs";
import semver from "semver";

// Get the current version from package.json
const packageJson = JSON.parse(readFileSync("package.json", "utf8"));
const currentVersion = packageJson.version;

// Get the latest tag version
let latestTag = null;
try {
	const tagOutput = execSync("git tag -l --sort=-v:refname", {
		encoding: "utf8",
	}).trim();
	if (tagOutput) {
		// Get first line (latest tag) - cross-platform compatible
		latestTag = tagOutput.split("\n")[0].replace(/^v/, "");
	}
} catch (e) {
	// No tags found
}

// Check for version mismatch warning
if (latestTag && semver.gt(latestTag, currentVersion)) {
	console.warn(
		`⚠️  Warning: package.json version (${currentVersion}) is behind latest tag (${latestTag})`
	);
	console.warn(`   You may want to sync package.json version first.`);
	console.log("");
}

// Parse command line arguments (support flags like --dry-run anywhere)
// Filter out the '--' separator that npm/pnpm uses for argument passing
const args = process.argv.slice(2).filter((arg) => arg !== "--");
const knownIncrements = new Set(["patch", "minor", "major", "continue"]);
let increment = undefined;
const argParts = [];
for (const a of args) {
	if (knownIncrements.has(a) && !increment) increment = a;
	else argParts.push(a);
}

// Check if we're already on a beta version
const isCurrentlyBeta = semver.prerelease(currentVersion) !== null;

let releaseCommand = null;

if (isCurrentlyBeta && (!increment || increment === "continue")) {
	// If already on beta and no increment specified, just bump the prerelease number
	console.log(`📦 Current version: ${currentVersion} (beta)`);
	console.log("🔄 Continuing beta sequence...");
	// Use a special config without preRelease to continue existing beta sequence cleanly
	releaseCommand =
		"npx release-it --config .release-it.beta-continue.cjs prerelease";
} else if (
	increment === "patch" ||
	increment === "minor" ||
	increment === "major"
) {
	// If increment is specified, create new beta.0 for that version
	console.log(`📦 Current version: ${currentVersion}`);
	console.log(`🚀 Creating new ${increment} beta version...`);
	releaseCommand = `npx release-it --config .release-it.beta.cjs ${increment} --preRelease=beta`;
} else if (!isCurrentlyBeta) {
	// If not on beta and no increment, default to patch
	console.log(`📦 Current version: ${currentVersion} (stable)`);
	console.log("🚀 Creating new patch beta version...");
	releaseCommand =
		"npx release-it --config .release-it.beta.cjs patch --preRelease=beta";
} else {
	// Default to continuing prerelease
	console.log(`📦 Current version: ${currentVersion} (beta)`);
	console.log("🔄 Continuing beta sequence...");
	releaseCommand =
		"npx release-it --config .release-it.beta-continue.cjs prerelease";
}

// Add any additional arguments and ensure --dry-run passes through to release-it
let additionalArgs = argParts.join(" ");
if (additionalArgs) {
	releaseCommand += " " + additionalArgs;
}
// If this is a dry-run, relax git cleanliness requirements to avoid false negatives
if (argParts.includes("--dry-run")) {
	releaseCommand +=
		" --no-git.requireCleanWorkingDir --no-git.requireUpstream";
}

console.log(`\n📝 Executing: ${releaseCommand}\n`);

// Pass dry-run env flag downstream so hooks can skip writes
const __dry =
	args.includes("--dry-run") ||
	(additionalArgs && additionalArgs.includes("--dry-run"));
if (__dry) process.env.RELEASE_IT_DRY_RUN = "1";

// NOTE: In dry-run we want hooks to no-op; we propagate a flag via env

try {
	// (dry-run) pass env flag to hooks

	execSync(releaseCommand, { stdio: "inherit" });
} catch (error) {
	console.error("❌ Release failed:", error.message);
	process.exit(1);
}
