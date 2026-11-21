package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/deweysasser/whisper-network/internal/egress"
)

const (
	// egressProbeMax bounds the probe response body.
	egressProbeMax = 64
	// egressProbeTimeout bounds one probe request.
	egressProbeTimeout = 15 * time.Second
)

// proxyPortTries is how many ephemeral ports startEgress tries for the proxy
// before giving up on finding one outside the egress ports.
const proxyPortTries = 8

// startEgress starts the SSRF-filtering CONNECT proxy yt-dlp is pointed at
// and builds the filtered clients of plain GET fetches and of the egress
// probe (which may also reach its own URL's port). The returned func stops
// the proxy.
//
// The fetch child may connect to the proxy's port on any address (Landlock
// has no address rules), so an operator's egress firewall that allows only
// the egress ports and the server's port closes the direct path around the
// proxy (DESIGN.md §13) only if the proxy's port is none of those: a port
// that is gets replaced.
func (w *Worker) startEgress(ctx context.Context) (func(), error) {
	d := &egress.Dialer{AllowPrivate: w.cfg.AllowPrivateEgress, AllowPorts: w.cfg.EgressPorts}
	forbidden := slices.Concat(egress.DefaultPorts, w.cfg.EgressPorts)
	if p := serverPort(w.cfg.Client.BaseURL()); p != 0 {
		forbidden = append(forbidden, p)
	}
	var p *egress.Proxy
	for range proxyPortTries {
		var err error
		if p, err = egress.StartProxy(ctx, d); err != nil {
			return nil, fmt.Errorf("starting the egress proxy: %w", err)
		}
		if !slices.Contains(forbidden, p.Port()) {
			break
		}
		_ = p.Close()
		p = nil
	}
	if p == nil {
		return nil, fmt.Errorf("starting the egress proxy: no port outside %v after %d tries", forbidden, proxyPortTries)
	}
	w.proxy = p
	probe := d
	if pp := serverPort(w.cfg.EgressProbe); pp != 0 {
		probe = d.WithPorts(pp)
	}
	w.egressClient = probe.HTTPClient(egressProbeTimeout)
	w.fetchClient = d.HTTPClient(w.cfg.Limits.FetchTimeout)
	if w.cfg.AllowPrivateEgress {
		w.log.Warn().Msg("--allow-private-egress: this downloader may reach loopback, private and link-local " +
			"addresses; fetch URLs can then probe the local network")
	}
	w.log.Info().Uint16("proxy_port", p.Port()).Msg("egress proxy started")
	return func() { _ = p.Close() }, nil
}

// serverPort is the TCP port of an http(s) URL, explicit or the scheme's
// default; 0 if there is none.
func serverPort(raw string) uint16 {
	u, err := url.Parse(raw)
	if err != nil {
		return 0
	}
	if p := u.Port(); p != "" {
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			return 0
		}
		return uint16(n)
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		return 80
	case "https":
		return 443
	}
	return 0
}

// initialEgressIP probes once during startup so the registration carries the
// IP. A failure is logged and leaves it unknown.
func (w *Worker) initialEgressIP(ctx context.Context) string {
	if w.cfg.EgressProbe == "" {
		return ""
	}
	ip, err := w.probeEgressIP(ctx)
	if err != nil {
		w.log.Warn().Str("error_class", errClass(err)).Msg("egress probe failed; egress IP unknown")
		return ""
	}
	return ip
}

// Probe failures of errClass's "status" and "body" classes.
var (
	errProbeStatus = errors.New("egress probe: unexpected status")
	errProbeBody   = errors.New("egress probe: unusable response body")
)

// probeEgressIP asks the operator's probe URL for this worker's public IP
// through the filtered egress client. The body must be at most 64 bytes and
// a global IP address (any address with AllowPrivateEgress): a private one
// would group this worker with unrelated hosts behind other NATs.
func (w *Worker) probeEgressIP(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, egressProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.cfg.EgressProbe, nil)
	if err != nil {
		return "", stripURL(err)
	}
	resp, err := w.egressClient.Do(req)
	if err != nil {
		return "", stripURL(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%w %d", errProbeStatus, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, egressProbeMax+1))
	if err != nil {
		return "", fmt.Errorf("%w: reading: %w", errProbeBody, stripURL(err))
	}
	if len(body) > egressProbeMax {
		return "", fmt.Errorf("%w: too large", errProbeBody)
	}
	a, err := netip.ParseAddr(strings.TrimSpace(string(body)))
	if err != nil {
		return "", fmt.Errorf("%w: not an IP address", errProbeBody)
	}
	a = a.WithZone("").Unmap()
	if !w.cfg.AllowPrivateEgress && egress.Refused(a) {
		return "", fmt.Errorf("%w: not a global address", errProbeBody)
	}
	return a.String(), nil
}

// stripURL drops the *url.Error wrapper, whose text contains the URL.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// errClass is a log-safe description of an egress error (probe or fetch):
// one of timeout, refused, dns, status, body or other, never the error's
// text, which may contain the URL or its host.
func errClass(err error) string {
	var ne net.Error
	var dns *net.DNSError
	switch {
	case errors.Is(err, egress.ErrRefused):
		return "refused"
	case errors.As(err, &dns):
		return "dns"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	case errors.Is(err, errProbeStatus):
		return "status"
	case errors.Is(err, errProbeBody):
		return "body"
	}
	return "other"
}

// requestEgressProbe asks runEgressProbe to probe now (for example after a
// fetch network error). It never blocks.
func (w *Worker) requestEgressProbe() {
	select {
	case w.egressTrigger <- struct{}{}:
	default:
	}
}

// runEgressProbe re-probes the egress IP every EgressProbeInterval and on
// request, patching the facts on the server when it changes. It returns when
// ctx ends.
func (w *Worker) runEgressProbe(ctx context.Context) {
	sent := w.currentFacts().EgressIP
	t := time.NewTicker(w.cfg.EgressProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-w.egressTrigger:
		}
		ip, err := w.probeEgressIP(ctx)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn().Str("error_class", errClass(err)).Msg("egress probe failed; keeping the last known IP")
			}
			continue
		}
		if ip == sent {
			continue
		}
		old := w.currentFacts()
		old.EgressIP = sent
		w.egressMu.Lock()
		w.egressIP = ip
		w.egressMu.Unlock()
		if err := w.patchFacts(ctx, old); err != nil {
			if ctx.Err() == nil {
				w.log.Warn().Err(err).Msg("could not update the egress IP on the server")
			}
			continue
		}
		sent = ip
		w.log.Info().Msg("egress IP changed; facts updated")
	}
}
