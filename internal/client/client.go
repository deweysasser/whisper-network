// Package client is a typed Go client for the whisper-network HTTP API
// (DESIGN.md §6): the client API used by submitters and the worker API used
// by workers. Every call takes a context; errors from the server are returned
// as *Error carrying the RFC 9457 problem body.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Client talks to one whisper-network server. It is safe for concurrent use.
// A Client is immutable; WithToken returns a copy with other credentials.
type Client struct {
	base      *url.URL
	token     string
	hc        *http.Client
	userAgent string
}

// Option configures a Client.
type Option func(*Client)

// WithToken sets the bearer token sent on every request.
func WithToken(token string) Option { return func(c *Client) { c.token = token } }

// WithHTTPClient sets the HTTP client used for requests. Its Timeout should be
// zero (or generous): claims long-poll and media transfers are long-lived and
// are bounded by the caller's context instead. The client is copied, and the
// copy's CheckRedirect is replaced by the same-origin policy of New.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

// ErrRedirectRefused reports a redirect away from the server's origin.
var ErrRedirectRefused = errors.New("refusing redirect to another origin")

// sameOrigin is the client's CheckRedirect: a redirect may change the path
// but not the scheme, host or port, so the bearer token (which net/http
// forwards on same-host redirects, even from https to http) never leaves the
// server's origin or its TLS.
func sameOrigin(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	orig := via[0].URL
	if req.URL.Scheme != orig.Scheme || req.URL.Host != orig.Host {
		return fmt.Errorf("%w: %s://%s to %s://%s", ErrRedirectRefused, orig.Scheme, orig.Host, req.URL.Scheme, req.URL.Host)
	}
	return nil
}

// WithUserAgent sets the User-Agent header.
func WithUserAgent(ua string) Option { return func(c *Client) { c.userAgent = ua } }

// New returns a client for the server at baseURL (for example
// "https://wn.example" or "https://proxy.example/wn"). Only http and https
// URLs are accepted.
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("server URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("server URL %q: want http(s)://host[/prefix]", baseURL)
	}
	u.RawQuery, u.Fragment = "", ""
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	c := &Client{base: u, hc: &http.Client{}, userAgent: "whisper-network-client"}
	for _, o := range opts {
		o(c)
	}
	hc := *c.hc
	hc.CheckRedirect = sameOrigin
	c.hc = &hc
	return c, nil
}

// WithToken returns a copy of c that authenticates with token.
func (c *Client) WithToken(token string) *Client {
	cp := *c
	cp.token = token
	return &cp
}

// BaseURL returns the server URL the client was built with.
func (c *Client) BaseURL() string { return c.base.String() }

// endpoint builds an absolute URL from escaped path segments.
func (c *Client) endpoint(segments ...string) *url.URL {
	esc := make([]string, len(segments))
	for i, s := range segments {
		esc[i] = url.PathEscape(s)
	}
	u := *c.base
	u.RawPath = ""
	u.Path = c.base.Path + "/" + strings.Join(segments, "/")
	u.RawPath = c.base.EscapedPath() + "/" + strings.Join(esc, "/")
	return &u
}

// resolve turns a server-provided reference (such as a lease's input_url)
// into an absolute URL. Paths are taken relative to the server base, so a
// path prefix survives; absolute URLs must point at the same origin, so the
// token is never sent to another host.
func (c *Client) resolve(ref string) (*url.URL, error) {
	r, err := url.Parse(ref)
	if err != nil {
		return nil, fmt.Errorf("server reference %q: %w", ref, err)
	}
	if r.IsAbs() || r.Host != "" {
		if r.Scheme != c.base.Scheme || r.Host != c.base.Host {
			return nil, fmt.Errorf("server reference %q is not on %s", ref, c.base.Host)
		}
		return r, nil
	}
	u := *c.base
	u.Path = c.base.Path + "/" + strings.TrimPrefix(r.Path, "/")
	u.RawPath = ""
	u.RawQuery = r.RawQuery
	return &u, nil
}

// newRequest builds an authenticated request.
func (c *Client) newRequest(ctx context.Context, method string, u *url.URL, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	return req, nil
}

// send performs req and converts any status outside want into *Error. On
// success the caller owns resp.Body.
func (c *Client) send(req *http.Request, want ...int) (*http.Response, error) {
	return c.sendWith(c.hc, req, want...)
}

// sendWith is send through hc.
func (c *Client) sendWith(hc *http.Client, req *http.Request, want ...int) (*http.Response, error) {
	resp, err := hc.Do(req)
	if err != nil {
		if errors.Is(err, ErrRedirectRefused) {
			// Not wrapped in *url.Error, which would read as a transient
			// network failure: retrying cannot help.
			return nil, fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, errors.Unwrap(err))
		}
		return nil, err
	}
	for _, w := range want {
		if resp.StatusCode == w {
			return resp, nil
		}
	}
	defer resp.Body.Close()
	return nil, decodeError(req, resp)
}

// doJSON sends in (if non-nil) as JSON and decodes a JSON response into out
// (if non-nil). It returns the response status.
func (c *Client) doJSON(ctx context.Context, method string, u *url.URL, in, out any, want ...int) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := c.newRequest(ctx, method, u, body)
	if err != nil {
		return 0, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.send(req, want...)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: decoding response: %w", method, u.Path, err)
		}
	}
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode, nil
}

// errNoBody is returned when a streaming endpoint unexpectedly has no body.
var errNoBody = errors.New("empty response body")
