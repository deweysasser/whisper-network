package media

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSniff(t *testing.T) {
	tests := map[string]struct {
		in   []byte
		want string
	}{
		"wav":        {toWAV(make([]int16, 32)), "audio/wav"},
		"riff avi":   {[]byte("RIFF\x00\x00\x00\x00AVI LIST"), "video/avi"},
		"flac":       {[]byte("fLaC\x00\x00\x00\x22"), "audio/flac"},
		"mp3":        {[]byte("ID3\x03\x00\x00\x00\x00\x00\x00"), "audio/mpeg"},
		"html":       {[]byte("<!DOCTYPE html><html>"), "text/html; charset=utf-8"},
		"empty":      {nil, "text/plain; charset=utf-8"},
		"short riff": {[]byte("RIFF"), "text/plain; charset=utf-8"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, Sniff(tc.in))
		})
	}
}

func TestNotMedia(t *testing.T) {
	pad := func(b []byte) []byte { return append(b, bytes.Repeat([]byte("plain text "), 40)...) }
	utf16le := func(s string) []byte {
		out := []byte{0xFF, 0xFE}
		for _, r := range s {
			out = append(out, byte(r), 0)
		}
		return out
	}
	utf16be := func(s string) []byte {
		out := []byte{0xFE, 0xFF}
		for _, r := range s {
			out = append(out, 0, byte(r))
		}
		return out
	}
	for name, head := range map[string][]byte{
		"mpeg layer I": pad([]byte{0xFF, 0xFE, 0x90, 0x00}), // FF FE, but not UTF-16 text after it
		"mp3 frame":    pad([]byte{0xFF, 0xFB, 0x90, 0x00}),
		"id3":          pad([]byte("ID3\x04\x00")),
		"ogg":          pad([]byte("OggS\x00")),
		"mp4":          pad([]byte("\x00\x00\x00\x20ftypisom")),
		"webm":         pad([]byte{0x1A, 0x45, 0xDF, 0xA3}),
		"wav":          toWAV(make([]int16, 32)),
		"flac":         pad([]byte("fLaC")),
		"binary":       bytes.Repeat([]byte{0, 1, 2, 0xff}, 128),
		"empty":        nil,
	} {
		_, bad := NotMedia(head)
		assert.False(t, bad, name)
		if len(head) > 0 && name != "binary" {
			assert.True(t, KnownMedia(head), name)
		}
	}
	for name, head := range map[string][]byte{
		"html":            []byte("<!doctype html><html></html>"),
		"html leading ws": []byte(" \r\n\t\f<html><body>hi</body></html>"),
		"utf-8 bom html":  []byte("\xef\xbb\xbf<html></html>"),
		"json":            []byte(`  {"a":1}`),
		"xml":             []byte(`<?xml version="1.0"?><a/>`),
		"text":            []byte("Too many requests, slow down"),
		"utf-16le html":   utf16le("<!DOCTYPE html><html><body>blocked</body></html>"),
		"utf-16be html":   utf16be("<!DOCTYPE html><html><body>blocked</body></html>"),
		"utf-16le text":   utf16le("Access denied"),
	} {
		kind, bad := NotMedia(head)
		assert.True(t, bad, name)
		assert.NotEmpty(t, kind, name)
	}
}
