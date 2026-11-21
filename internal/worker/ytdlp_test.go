package worker

import (
	"errors"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestYtDlpArgsFixed(t *testing.T) {
	base := []string{"/opt/yt-dlp/yt-dlp", "--ignore-config", "--no-playlist", "--match-filter", "!is_live",
		"-f", "bestaudio", "--use-extractors", "default,-generic", "--no-cache-dir", "--no-part",
		"-o", "-", "--proxy", "http://127.0.0.1:3128"}
	assert.Equal(t, append(append([]string{}, base...), "--", "https://www.youtube.com/watch?v=x"),
		YtDlpArgs("/opt/yt-dlp/yt-dlp", "https://www.youtube.com/watch?v=x", 3128, ""))
	assert.Equal(t, append(append([]string{}, base...), "--cookies", "/w/l1/cookies.txt", "--", "--exec=rm"),
		YtDlpArgs("/opt/yt-dlp/yt-dlp", "--exec=rm", 3128, "/w/l1/cookies.txt"),
		"the URL follows --, so it can never be read as an option")
}

func TestClassifyYtDlp(t *testing.T) {
	exit := &exec.ExitError{}
	for _, c := range []struct {
		name   string
		err    error
		stderr string
		want   ytdlpOutcome
	}{
		{"ok", nil, "[youtube] x: Downloading webpage\n[download] 100% of 3.2MiB\n", ytdlpOK},
		{"unsupported", exit, "ERROR: Unsupported URL: https://example.com/page\n", ytdlpUnsupported},
		{"no extractor (generic disabled)", exit, "ERROR: No suitable extractor found for URL https://example.com/a.mp3\n", ytdlpUnsupported},
		{"429", exit, "ERROR: unable to download video data: HTTP Error 429: Too Many Requests\n", ytdlpRateLimited},
		{"too many requests", exit, "ERROR: [youtube] x: Unable to download webpage: HTTP Error 429: Too Many Requests (caused by <HTTPError 429: Too Many Requests>)\n", ytdlpRateLimited},
		{"warning only", exit, "WARNING: [youtube] Too Many Requests\nERROR: [youtube] x: Unable to download webpage: [Errno 104] Connection reset by peer\n", ytdlpTransient},
		{"url echo is not a verdict", exit, "[youtube] Extracting URL: https://www.youtube.com/watch?v=x&rate-limited&private+video\nERROR: [youtube] x: Unable to extract initial player response\n", ytdlpTransient},
		{"id is not a verdict", exit, "ERROR: [vimeo] rate-limited: Unable to download JSON metadata: [Errno 110] Connection timed out\n", ytdlpTransient},
		{"bot check", exit, "ERROR: [youtube] dQw4w9WgXcQ: Sign in to confirm you’re not a bot. Use --cookies-from-browser or --cookies for the authentication.\n", ytdlpRateLimited},
		{"bot check ascii", exit, "ERROR: [youtube] x: Sign in to confirm you're not a bot. This helps protect our community.\n", ytdlpRateLimited},
		{"session rate-limited", exit, "ERROR: [youtube] x: The current session has been rate-limited by YouTube for up to an hour.\n", ytdlpRateLimited},
		{"case-insensitive", exit, "error: http error 429: too many requests\n", ytdlpRateLimited},
		{"unavailable", exit, "ERROR: [youtube] dQw4w9WgXcQ: Video unavailable\n", ytdlpPermanent},
		{"terminated", exit, "ERROR: [youtube] x: Video unavailable. This video is no longer available because the YouTube account associated with this video has been terminated.\n", ytdlpPermanent},
		{"private", exit, "ERROR: [youtube] x: Private video. Sign in if you've been granted access to this video\n", ytdlpPermanent},
		{"members-only", exit, "ERROR: [youtube] x: Join this channel to get access to members-only content like this video, and other exclusive perks.\n", ytdlpPermanent},
		{"not available", exit, "ERROR: [vimeo] 12345: This video is not available\n", ytdlpPermanent},
		{"geo", exit, "ERROR: [youtube] x: The uploader has not made this video available in your country\n", ytdlpPermanent},
		{"404", exit, "ERROR: unable to download video data: HTTP Error 404: Not Found\n", ytdlpPermanent},
		{"410", exit, "ERROR: unable to download video data: HTTP Error 410: Gone\n", ytdlpPermanent},
		{"upcoming live", exit, "ERROR: [youtube] x: This live event will begin in 3 hours.\n", ytdlpPermanent},
		{"filter text in a title, exit 1", exit, "[download] Video does not pass filter (!is_live), skipping ..\nERROR: [Errno 32] Broken pipe\n", ytdlpTransient},
		{"live filtered (exit 0)", nil, "[youtube] x: Downloading m3u8 information\n[download] Live Show does not pass filter (!is_live), skipping ..\n", ytdlpPermanent},
		{"no audio format", exit, "ERROR: [youtube] x: Requested format is not available. Use --list-formats for a list of available formats\n", ytdlpPermanent},
		{"connection reset", exit, "ERROR: unable to download video data: [Errno 104] Connection reset by peer\n", ytdlpTransient},
		{"503", exit, "ERROR: unable to download video data: HTTP Error 503: Service Unavailable\n", ytdlpTransient},
		{"proxy refused", exit, "ERROR: [youtube] x: Unable to download webpage: ('Unable to connect to proxy', OSError('Tunnel connection failed: 403 Forbidden'))\n", ytdlpTransient},
		// A verdict counts only as yt-dlp phrases it, at the start of the
		// message: page data quoted later in an ERROR line must not cool
		// the site down.
		{"429 phrase later in the message", exit, "ERROR: [youtube] x: Unable to extract title; got 'HTTP Error 429: Too Many Requests'\n", ytdlpTransient},
		{"loose too many requests", exit, "ERROR: [youtube] x: Unable to extract uploader: Too Many Requests Fan Club\n", ytdlpTransient},
		{"loose rate-limited", exit, "ERROR: [soundcloud] x: Track rate-limited remix is not streamable\n", ytdlpTransient},
		{"loose rate-limit reached", exit, "ERROR: [vimeo] x: rate-limit reached by the uploader's title\n", ytdlpTransient},
		{"bot check quoted later", exit, "ERROR: [youtube] x: Unable to extract description: Sign in to confirm you're not a bot\n", ytdlpTransient},
		{"no stderr", exit, "", ytdlpTransient},
		{"killed", errors.New("signal: killed"), "", ytdlpTransient},
	} {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, classifyYtDlp(c.err, c.stderr))
		})
	}
}

func TestRedactURL(t *testing.T) {
	u := "https://example.com/a?b=c"
	assert.Equal(t, "ERROR: Unsupported URL: <url>\n[x] <url>", redactURL("ERROR: Unsupported URL: "+u+"\n[x] "+u, u))
	assert.Equal(t, "abc", redactURL("abc", ""))
}
