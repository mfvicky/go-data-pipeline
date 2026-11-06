package security

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type policyKey struct{}

func WithPolicy(ctx context.Context, policy *SourcePolicy) context.Context {
	if policy == nil {
		return ctx
	}
	return context.WithValue(ctx, policyKey{}, policy)
}
func PolicyFromContext(ctx context.Context) *SourcePolicy {
	policy, _ := ctx.Value(policyKey{}).(*SourcePolicy)
	return policy
}

// HTTPClient validates redirect targets and the resolved IP of each outbound
// connection. Proxy use is disabled so a proxy cannot bypass address checks.
func (p SourcePolicy) HTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if p.AllowPrivateSources {
				return dialer.DialContext(ctx, network, address)
			}
			ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, fmt.Errorf("resolve source: %w", err)
			}
			if len(ips) == 0 {
				return nil, fmt.Errorf("source hostname has no IPs")
			}
			for _, ip := range ips {
				if !isPublicIP(ip) {
					return nil, fmt.Errorf("private or reserved source IP blocked")
				}
			}
			var last error
			for _, ip := range ips {
				conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				if dialErr == nil {
					return conn, nil
				}
				last = dialErr
			}
			return nil, last
		},
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   true,
	}
	return &http.Client{Timeout: timeout, Transport: transport, CheckRedirect: func(next *http.Request, previous []*http.Request) error {
		if len(previous) >= 5 {
			return fmt.Errorf("too many source redirects")
		}
		if err := p.validateURL(next.Context(), next.URL.String()); err != nil {
			return err
		}
		if len(previous) > 0 && !strings.EqualFold(next.URL.Hostname(), previous[0].URL.Hostname()) {
			return fmt.Errorf("redirect to another host is not allowed")
		}
		// Do not forward caller-supplied auth or other sensitive headers.
		next.Header = http.Header{"Accept": previous[0].Header.Values("Accept")}
		return nil
	}}
}

// OpenFile repeats directory validation immediately before opening the file.
// For hostile local processes changing symlinks concurrently, OS sandboxing
// remains necessary; filepath checks are not equivalent to openat2 confinement.
func (p SourcePolicy) OpenFile(endpoint string) (io.ReadCloser, error) {
	if err := p.validateFile(endpoint); err != nil {
		return nil, err
	}
	file, err := openScopedFile(p.InputDir, endpoint)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("source must be a regular file")
	}
	if err := p.validateFile(endpoint); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

// ValidateForExecution is used at job processing time; request-time checks
// alone cannot protect against DNS and filesystem changes after submission.
func (p SourcePolicy) ValidateForExecution(ctx context.Context, source domain.SourceConfig) error {
	if strings.Contains(source.Endpoint, "://") {
		return p.validateURL(ctx, source.Endpoint)
	}
	return p.validateFile(source.Endpoint)
}
