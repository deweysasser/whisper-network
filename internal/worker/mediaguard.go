package worker

import (
	"errors"
	"io"
	"mime"
	"strings"

	"github.com/deweysasser/whisper-network/internal/media"
)

// sniffLen is what http.DetectContentType looks at.
const sniffLen = 512

// errNotMedia refuses fetched content that is a page or document rather
// than media: a login wall, an error page, an API answer. Every downloader
// would get the same, so it is the input's fault.
type errNotMedia struct{ kind string }

func (e errNotMedia) Error() string { return "not media (" + e.kind + ")" }

// errTooLarge is a fetch that exceeded the lease's byte cap.
var errTooLarge = errors.New("too large")

// mediaGuard passes a stream through once its first bytes sniff as media.
type mediaGuard struct {
	r       io.Reader
	head    []byte
	checked bool
	err     error
}

// newMediaGuard returns a reader of r's bytes that fails with errNotMedia
// before passing any if the first 512 (or all, if fewer) are not media by
// media.NotMedia, the check the server repeats on receipt.
func newMediaGuard(r io.Reader) io.Reader { return &mediaGuard{r: r} }

func (g *mediaGuard) Read(p []byte) (int, error) {
	if g.err != nil {
		return 0, g.err
	}
	if !g.checked {
		buf := make([]byte, sniffLen)
		n, err := io.ReadFull(g.r, buf)
		g.head = buf[:n]
		g.checked = true
		switch {
		case err == io.EOF || err == io.ErrUnexpectedEOF:
			g.r = eofReader{}
		case err != nil:
			g.err = err
			return 0, err
		}
		if kind, bad := media.NotMedia(g.head); bad {
			g.err = errNotMedia{kind: kind}
			return 0, g.err
		}
	}
	if len(g.head) > 0 {
		n := copy(p, g.head)
		g.head = g.head[n:]
		return n, nil
	}
	return g.r.Read(p)
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

// notMediaType reports whether the Content-Type a server sent names a page
// or document rather than media, and its media type. (The sniffed bytes are
// media.NotMedia's.)
func notMediaType(ct string) (string, bool) {
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		mt = strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	}
	switch {
	case strings.HasPrefix(mt, "text/"),
		mt == "application/json", strings.HasSuffix(mt, "+json"),
		mt == "application/xml", strings.HasSuffix(mt, "+xml"), // includes application/xhtml+xml
		mt == "application/javascript":
		return mt, true
	}
	return mt, false
}

// capReader fails with errTooLarge once more than max bytes have passed.
type capReader struct {
	r    io.Reader
	left int64
}

// newCapReader bounds r at max bytes; 0 means no bound (the server enforces
// its own).
func newCapReader(r io.Reader, max int64) io.Reader {
	if max <= 0 {
		return r
	}
	return &capReader{r: r, left: max}
}

func (c *capReader) Read(p []byte) (int, error) {
	if c.left < 0 {
		return 0, errTooLarge
	}
	if c.left < int64(len(p)) {
		p = p[:c.left+1] // one byte more than allowed reveals an overrun
	}
	n, err := c.r.Read(p)
	if int64(n) > c.left {
		c.left = -1
		return 0, errTooLarge
	}
	c.left -= int64(n)
	return n, err
}
