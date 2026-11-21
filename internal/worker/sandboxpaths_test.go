package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeExe writes an executable file, creating its directory.
func writeExe(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o755))
}

func TestYtDlpReadOnly(t *testing.T) {
	state := t.TempDir()
	home := t.TempDir() // a home directory that holds none of the tools

	t.Run("venv root", func(t *testing.T) {
		venv := canonical(t.TempDir())
		writeExe(t, filepath.Join(venv, "bin", "yt-dlp"), "#!"+filepath.Join(venv, "bin", "python")+"\n")
		writeExe(t, filepath.Join(venv, "bin", "python"), "")
		require.NoError(t, os.WriteFile(filepath.Join(venv, "pyvenv.cfg"), []byte("home = /usr/bin\n"), 0o644))
		ro, err := ytdlpReadOnly(filepath.Join(venv, "bin", "yt-dlp"), state, home)
		require.NoError(t, err)
		assert.Contains(t, ro, venv, "the venv's lib/ holds yt-dlp's modules")
		assert.Contains(t, ro, filepath.Join(venv, "bin"))
	})
	t.Run("no venv", func(t *testing.T) {
		dir := canonical(t.TempDir())
		writeExe(t, filepath.Join(dir, "bin", "yt-dlp"), "#!/usr/bin/env python3\n")
		ro, err := ytdlpReadOnly(filepath.Join(dir, "bin", "yt-dlp"), state, home)
		require.NoError(t, err)
		assert.NotContains(t, ro, dir)
		assert.Equal(t, []string{filepath.Join(dir, "bin", "yt-dlp"), filepath.Join(dir, "bin")}, ro)
	})
	t.Run("interpreter outside the system paths", func(t *testing.T) {
		tool, py := canonical(t.TempDir()), canonical(t.TempDir())
		writeExe(t, filepath.Join(py, "python3"), "")
		writeExe(t, filepath.Join(tool, "yt-dlp"), "#!"+filepath.Join(py, "python3")+" -I\n")
		ro, err := ytdlpReadOnly(filepath.Join(tool, "yt-dlp"), state, home)
		require.NoError(t, err)
		assert.Contains(t, ro, filepath.Join(py, "python3"))
		assert.Contains(t, ro, py)
	})
	t.Run("refused under home", func(t *testing.T) {
		h := canonical(t.TempDir())
		writeExe(t, filepath.Join(h, ".local", "bin", "yt-dlp"), "#!/usr/bin/python3\n")
		_, err := ytdlpReadOnly(filepath.Join(h, ".local", "bin", "yt-dlp"), state, h)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "home directory")
	})
	t.Run("venv under home", func(t *testing.T) {
		h := canonical(t.TempDir())
		venv := filepath.Join(h, "venv")
		writeExe(t, filepath.Join(venv, "bin", "yt-dlp"), "#!/usr/bin/python3\n")
		require.NoError(t, os.WriteFile(filepath.Join(venv, "pyvenv.cfg"), nil, 0o644))
		_, err := ytdlpReadOnly(filepath.Join(venv, "bin", "yt-dlp"), state, h)
		require.Error(t, err)
	})
	t.Run("interpreter under home", func(t *testing.T) {
		h := canonical(t.TempDir())
		tool := canonical(t.TempDir())
		writeExe(t, filepath.Join(h, "py", "python3"), "")
		writeExe(t, filepath.Join(tool, "yt-dlp"), "#!"+filepath.Join(h, "py", "python3")+"\n")
		_, err := ytdlpReadOnly(filepath.Join(tool, "yt-dlp"), state, h)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "interpreter")
	})
}

func TestShebang(t *testing.T) {
	dir := t.TempDir()
	for content, want := range map[string]string{
		"#!/usr/bin/python3\nimport x\n": "/usr/bin/python3",
		"#! /usr/bin/env python3\n":      "/usr/bin/env",
		"\x7fELF\x02\x01":                "",
		"":                               "",
	} {
		p := filepath.Join(dir, "f")
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
		assert.Equal(t, want, shebang(p), "%q", content)
	}
	assert.Equal(t, "", shebang(filepath.Join(dir, "missing")))
}
