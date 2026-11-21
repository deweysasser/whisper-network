// Package egress enforces the SSRF address filter for downloader workers: a
// filtering Dialer, an http.Client built on it, and a loopback CONNECT proxy
// that a sandboxed yt-dlp child is forced through.
package egress
