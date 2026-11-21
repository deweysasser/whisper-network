package worker

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMediaGuard(t *testing.T) {
	bin := func(head string) []byte { return append([]byte(head), bytes.Repeat([]byte{0, 1, 2, 0xff}, 300)...) }
	refused := map[string][]byte{
		"html":            []byte("<!DOCTYPE html><html><head><title>x</title></head><body></body></html>"),
		"html leading ws": []byte(" \r\n\t<html><body>hi</body></html>"),
		"json":            []byte(`{"error": "not found"}`),
		"json ws":         []byte("\n  {\"a\": 1}"),
		"xml":             []byte(`<?xml version="1.0"?><rss></rss>`),
		"text":            []byte("Too many requests, slow down.\n"),
		"long html":       append([]byte("<html>"), bytes.Repeat([]byte("<p>x</p>"), 200)...),
	}
	for name, body := range refused {
		t.Run(name, func(t *testing.T) {
			_, err := io.ReadAll(newMediaGuard(iotest.OneByteReader(bytes.NewReader(body))))
			var nm errNotMedia
			require.True(t, errors.As(err, &nm), "got %v", err)
			assert.NotEmpty(t, nm.kind)
			assert.Contains(t, err.Error(), "not media")
		})
	}
	passed := map[string][]byte{
		"flac":  bin("fLaC\x00\x00\x00\x22"),
		"mp4":   bin("\x00\x00\x00\x20ftypM4A \x00\x00\x02\x00"),
		"webm":  bin("\x1a\x45\xdf\xa3\x9f\x42\x86\x81\x01"),
		"ogg":   bin("OggS\x00\x02"),
		"mp3":   bin("ID3\x04\x00\x00"),
		"short": []byte("fLaC\x00"),
		"empty": {},
	}
	for name, body := range passed {
		t.Run(name, func(t *testing.T) {
			got, err := io.ReadAll(newMediaGuard(iotest.OneByteReader(bytes.NewReader(body))))
			require.NoError(t, err)
			assert.Equal(t, body, got, "the guard passes every byte through")
		})
	}
	t.Run("read error", func(t *testing.T) {
		boom := errors.New("boom")
		_, err := io.ReadAll(newMediaGuard(io.MultiReader(strings.NewReader("fLaC"), iotest.ErrReader(boom))))
		assert.ErrorIs(t, err, boom)
	})
}

func TestCapReader(t *testing.T) {
	got, err := io.ReadAll(newCapReader(strings.NewReader("12345"), 5))
	require.NoError(t, err)
	assert.Equal(t, "12345", string(got), "exactly the cap is fine")
	_, err = io.ReadAll(newCapReader(strings.NewReader("123456"), 5))
	assert.ErrorIs(t, err, errTooLarge)
	got, err = io.ReadAll(newCapReader(strings.NewReader("123456"), 0))
	require.NoError(t, err)
	assert.Equal(t, "123456", string(got), "0 means no worker-side cap")
}
