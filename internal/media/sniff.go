package media

import (
	"bytes"
	"net/http"
	"strings"
)

// Content types returned by Sniff for the two fast-path formats.
const (
	TypeWAV  = "audio/wav"
	TypeFLAC = "audio/flac"
)

// Sniff returns the content type of an upload from its leading bytes (at
// least 512 are useful). The client-supplied Content-Type is never consulted.
// Anything that is not RIFF/WAVE or FLAC gets http.DetectContentType's answer.
func Sniff(head []byte) string {
	switch {
	case len(head) >= 12 && bytes.Equal(head[:4], []byte("RIFF")) && bytes.Equal(head[8:12], []byte("WAVE")):
		return TypeWAV
	case bytes.HasPrefix(head, []byte("fLaC")):
		return TypeFLAC
	}
	return http.DetectContentType(head)
}

// KnownMedia reports whether head starts with the signature of a common
// audio or video container, which is media whatever a text sniffer makes of
// it (MPEG audio frames, say, can look like text).
func KnownMedia(head []byte) bool {
	switch {
	case len(head) >= 12 && string(head[:4]) == "RIFF" && string(head[8:12]) == "WAVE",
		bytes.HasPrefix(head, []byte("fLaC")),
		bytes.HasPrefix(head, []byte("OggS")),
		bytes.HasPrefix(head, []byte("ID3")),
		bytes.HasPrefix(head, []byte{0x1A, 0x45, 0xDF, 0xA3}),     // EBML: Matroska, WebM
		len(head) >= 8 && string(head[4:8]) == "ftyp",             // ISO BMFF: MP4, M4A
		len(head) >= 2 && head[0] == 0xFF && head[1]&0xE0 == 0xE0: // MPEG audio frame sync, ADTS
		return true
	}
	return false
}

// NotMedia reports whether head, the leading bytes (512 are enough) of
// fetched content, is a web page or an API answer rather than media, and
// what it looks like. It is the one content check of fetch results, on the
// downloader (before streaming) and on the server (on receipt):
//
//   - UTF-16 text (a byte order mark, FF FE or FE FF, followed by ASCII in
//     UTF-16), checked first because FF FE is also an MPEG frame sync;
//   - else media by signature (KnownMedia) passes;
//   - else, past a UTF-8 BOM and whitespace, a leading '<' (HTML, XML) or
//     '{' (JSON);
//   - else what http.DetectContentType makes of it: text/*, JSON or XML.
func NotMedia(head []byte) (kind string, bad bool) {
	if utf16Text(head) {
		return "utf-16 text", true
	}
	if len(head) == 0 || KnownMedia(head) {
		return "", false
	}
	switch t := bytes.TrimLeft(bytes.TrimPrefix(head, []byte("\xef\xbb\xbf")), " \t\r\n\f"); {
	case len(t) > 0 && t[0] == '<':
		return "markup", true
	case len(t) > 0 && t[0] == '{':
		return "json", true
	}
	mt, _, _ := strings.Cut(http.DetectContentType(head), ";")
	switch mt = strings.TrimSpace(mt); {
	case strings.HasPrefix(mt, "text/"), mt == "application/json", mt == "application/xml",
		mt == "application/xhtml+xml":
		return mt, true
	}
	return "", false
}

// utf16Text reports whether head is a UTF-16 byte order mark followed by
// ASCII characters in UTF-16 (up to 16 of them, at least 2): a page saved as
// UTF-16 rather than an MPEG frame that happens to start FF FE.
func utf16Text(head []byte) bool {
	var hi int // index of the high byte within each code unit
	switch {
	case bytes.HasPrefix(head, []byte{0xFF, 0xFE}):
		hi = 1
	case bytes.HasPrefix(head, []byte{0xFE, 0xFF}):
		hi = 0
	default:
		return false
	}
	units := head[2:]
	n := min(len(units)/2, 16)
	if n < 2 {
		return false
	}
	for i := range n {
		u := units[2*i : 2*i+2]
		if c := u[1-hi]; u[hi] != 0 || c == 0 || c > 0x7e || c < 0x20 && c != '\t' && c != '\n' && c != '\r' {
			return false
		}
	}
	return true
}
