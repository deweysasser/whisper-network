package worker

import (
	"regexp"
	"strconv"
	"strings"
)

// YtDlpArgs is the fixed yt-dlp command of a fetch: one audio-only stream
// (no ffmpeg merge) of a single video, never a live stream, written to
// stdout, with no config files, cache or part files, through the egress
// proxy. Clients influence only the URL, which follows "--" so it can never
// be read as an option. cookies, when set, is the per-run copy of the
// operator's cookies file in the lease directory.
//
// The generic extractor is disabled so that "no extractor" is the signal for
// the plain GET fallback. yt-dlp has no --no-generic-extractor option;
// "--use-extractors default,-generic" is how it says so (yt-dlp
// 2022.11.11 and later).
func YtDlpArgs(ytdlp, url string, proxyPort uint16, cookies string) []string {
	args := []string{ytdlp, "--ignore-config", "--no-playlist", "--match-filter", "!is_live", "-f", "bestaudio",
		"--use-extractors", "default,-generic", "--no-cache-dir", "--no-part", "-o", "-",
		"--proxy", "http://127.0.0.1:" + strconv.Itoa(int(proxyPort))}
	if cookies != "" {
		args = append(args, "--cookies", cookies)
	}
	return append(args, "--", url)
}

// ytdlpOutcome is how a yt-dlp run ended, as far as the fetch cares.
type ytdlpOutcome int

const (
	ytdlpOK ytdlpOutcome = iota
	// ytdlpUnsupported: no extractor matched the URL; try a plain GET.
	ytdlpUnsupported
	// ytdlpRateLimited: the site refused for rate or a bot check.
	ytdlpRateLimited
	// ytdlpPermanent: the media is gone, private, blocked, live or has no
	// audio; every downloader would fail the same way.
	ytdlpPermanent
	// ytdlpTransient: anything else (network, 5xx, crash, timeout).
	ytdlpTransient
)

func (o ytdlpOutcome) String() string {
	switch o {
	case ytdlpOK:
		return "ok"
	case ytdlpUnsupported:
		return "unsupported"
	case ytdlpRateLimited:
		return "rate_limited"
	case ytdlpPermanent:
		return "permanent"
	}
	return "transient"
}

// Lower-case stderr fragments of yt-dlp's messages, checked in this order.
var (
	ytdlpUnsupportedMsgs = []string{
		"unsupported url",              // the generic extractor gave up
		"no suitable extractor found ", // no extractor matched (generic disabled)
	}
	ytdlpRateLimitMsgs = []string{
		"http error 429",
		"too many requests",
		"sign in to confirm you", // YouTube's bot check ("you’re", with a typographic apostrophe)
		"rate-limited",
		"rate-limit reached",
	}
	ytdlpPermanentMsgs = []string{
		"video unavailable",
		"private video",
		"this video is not available",
		"http error 404",
		"http error 410",
		"members-only",
		"this live event will begin",
		"premieres in",
		"available in your country",
		"requested format is not available",
	}
)

// classifyYtDlp classifies a finished yt-dlp run from its exit error and
// stderr, case-insensitively. Only yt-dlp's own verdicts count: "ERROR:"
// lines, less their "[extractor] id: " prefix, and the live-stream filter's
// skip notice. Progress and WARNING lines echo the URL and page data, which
// the client controls; a URL ending in "&rate-limited" must not put a site
// into cooldown. The caller also redacts the URL itself (redactURL), since
// some ERROR lines quote it.
//
// A run that exited 0 is ok unless it skipped the video for the live-stream
// filter, which yt-dlp reports with exit 0.
func classifyYtDlp(exitErr error, stderr string) ytdlpOutcome {
	var errs []string
	filtered := false
	for _, line := range strings.Split(strings.ToLower(stderr), "\n") {
		line = strings.TrimSpace(line)
		if msg, ok := strings.CutPrefix(line, "error:"); ok {
			errs = append(errs, ytdlpPrefixRE.ReplaceAllString(strings.TrimSpace(msg), ""))
		} else if strings.HasPrefix(line, "[download] ") && strings.Contains(line, "does not pass filter (!is_live)") {
			filtered = true
		}
	}
	has := func(msgs []string) bool {
		for _, e := range errs {
			for _, m := range msgs {
				if strings.Contains(e, m) {
					return true
				}
			}
		}
		return false
	}
	switch {
	case exitErr == nil && filtered:
		return ytdlpPermanent
	case exitErr == nil:
		return ytdlpOK
	case has(ytdlpUnsupportedMsgs):
		return ytdlpUnsupported
	case has(ytdlpRateLimitMsgs):
		return ytdlpRateLimited
	case has(ytdlpPermanentMsgs):
		return ytdlpPermanent
	}
	return ytdlpTransient
}

// ytdlpPrefixRE is the "[extractor] id: " prefix of an ERROR line; the id
// comes from the URL.
var ytdlpPrefixRE = regexp.MustCompile(`^\[[^\]]*\] [^ ]*: `)

// redactURL replaces every occurrence of url in s.
func redactURL(s, url string) string {
	if url == "" {
		return s
	}
	return strings.ReplaceAll(s, url, "<url>")
}
