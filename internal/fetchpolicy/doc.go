// Package fetchpolicy holds the pure logic for pacing URL downloads: site
// keys, rate rules, policy resolution and start/skip decisions. It does no
// I/O; the store's claim transaction supplies State and applies the result.
package fetchpolicy
