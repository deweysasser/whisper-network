package worker

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// systemReadOnly are the system paths every child may read: shared
// libraries and the dynamic loader's configuration.
var systemReadOnly = []string{"/usr", "/lib", "/lib64", "/etc/ld.so.cache", "/etc/ld.so.conf", "/etc/ld.so.conf.d"}

// childSystemFiles are harmless system files tools commonly probe.
var childSystemFiles = []string{"/proc/cpuinfo", "/sys/devices/system/cpu", "/dev/null", "/dev/urandom"}

// toolReadOnly returns what a child running tool may read besides the
// system paths: the binary itself and, when the binary lives outside the
// system paths, its directory, because whisper.cpp's shared libraries and
// ggml's CPU backends sit next to the executable (/opt/whisper in the
// image). That directory is granted whole, so it must be dedicated to the
// tool: one that contains the worker's state (credentials, other tasks'
// work) or lies under the home directory is refused, as is a state
// directory under any read-only path.
func toolReadOnly(tool, stateDir, home string) ([]string, error) {
	resolved, err := filepath.EvalSymlinks(tool)
	if err != nil {
		return nil, fmt.Errorf("resolving %s: %w", tool, err)
	}
	ro := []string{resolved}
	if tool != resolved {
		ro = append(ro, tool)
	}
	dir := filepath.Dir(resolved)
	if !underAny(dir, systemReadOnly) {
		home = canonical(home)
		switch {
		case dir == "/":
			return nil, fmt.Errorf("%s: a tool in / would expose the whole filesystem to the sandbox; install it in a dedicated directory such as /opt/whisper", tool)
		case home != "" && home != "/" && within(dir, home):
			return nil, fmt.Errorf("%s: the sandbox grants read access to the tool's directory %s, which is under the home directory %s; install the tool in a dedicated directory such as /opt/whisper", tool, dir, home)
		}
		ro = append(ro, dir)
	}
	if err := checkStateOutside(stateDir, ro); err != nil {
		return nil, fmt.Errorf("%s: %w", tool, err)
	}
	return ro, nil
}

// checkStateOutside refuses a state directory that children could read
// through one of the read-only paths.
func checkStateOutside(stateDir string, ro []string) error {
	state := canonical(stateDir)
	for _, p := range ro {
		if within(state, canonical(p)) {
			return fmt.Errorf("the state directory %s is inside %s, which sandboxed children may read; move it", stateDir, p)
		}
	}
	return nil
}

// canonical returns p absolute, with symlinks resolved where it exists.
func canonical(p string) string {
	if p == "" {
		return ""
	}
	if r, err := filepath.EvalSymlinks(p); err == nil {
		p = r
	}
	if a, err := filepath.Abs(p); err == nil {
		p = a
	}
	return filepath.Clean(p)
}

// within reports whether path is root or lies below it.
func within(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func underAny(path string, roots []string) bool {
	for _, r := range roots {
		if within(path, r) {
			return true
		}
	}
	return false
}

// homeDir is the home directory tool directories must stay out of.
func homeDir() string {
	h, _ := os.UserHomeDir()
	return h
}

// fetchSystemReadOnly are the system files yt-dlp (Python) needs besides
// systemReadOnly: CA certificates and the OpenSSL configuration for TLS
// through the proxy (Debian and Red Hat layouts), and the timezone. Not
// /etc/ssl or /etc/pki whole: their private/ directories hold host keys.
var fetchSystemReadOnly = []string{"/etc/ssl/certs", "/etc/ssl/openssl.cnf", "/etc/ca-certificates",
	"/etc/pki/tls/certs", "/etc/pki/tls/openssl.cnf", "/etc/pki/ca-trust", "/etc/localtime"}

// ytdlpReadOnly is what a yt-dlp child may read besides the system paths:
// toolReadOnly of yt-dlp itself; the root of the virtualenv it lives in
// (bin/yt-dlp with a pyvenv.cfg one level up), whose lib/ holds its
// modules; and the interpreter of its #! line when that lies outside the
// system paths. The same rules apply to each: no directory under the home
// directory or holding the worker's state.
func ytdlpReadOnly(ytdlp, stateDir, home string) ([]string, error) {
	ro, err := toolReadOnly(ytdlp, stateDir, home)
	if err != nil {
		return nil, err
	}
	resolved := ro[0]
	if root := filepath.Dir(filepath.Dir(resolved)); root != "/" {
		if _, err := os.Stat(filepath.Join(root, "pyvenv.cfg")); err == nil {
			if h := canonical(home); h != "" && h != "/" && within(root, h) {
				return nil, fmt.Errorf("%s: the sandbox grants read access to its virtualenv %s, which is under the home directory %s; install it in a dedicated directory such as /opt/yt-dlp", ytdlp, root, h)
			}
			ro = append(ro, root)
		}
	}
	if interp := shebang(resolved); interp != "" && filepath.IsAbs(interp) {
		if r, err := filepath.EvalSymlinks(interp); err == nil && !underAny(r, systemReadOnly) && !underAny(r, ro) {
			iro, err := toolReadOnly(interp, stateDir, home)
			if err != nil {
				return nil, fmt.Errorf("%s's interpreter: %w", ytdlp, err)
			}
			ro = append(ro, iro...)
		}
	}
	if err := checkStateOutside(stateDir, ro); err != nil {
		return nil, fmt.Errorf("%s: %w", ytdlp, err)
	}
	return ro, nil
}

// shebang returns the interpreter of a #! script, or "".
func shebang(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	buf := make([]byte, 256)
	n, _ := io.ReadFull(f, buf)
	line, _, _ := strings.Cut(string(buf[:n]), "\n")
	rest, ok := strings.CutPrefix(line, "#!")
	if !ok {
		return ""
	}
	if f := strings.Fields(rest); len(f) > 0 {
		return f[0]
	}
	return ""
}
