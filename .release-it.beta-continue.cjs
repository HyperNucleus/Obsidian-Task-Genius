// This config is specifically for continuing an existing beta sequence
// It keeps preRelease: 'beta' to continue incrementing the beta number
const baseConfig = require("./.release-it.beta.cjs");

// Destructure to remove properties we want to override
const { preRelease, ...rest } = baseConfig;

module.exports = {
	...rest,
	// Keep preRelease as 'beta' to continue the sequence (e.g., beta.0 -> beta.1)
	preRelease: "beta",
};
