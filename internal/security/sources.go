package security

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type SourcePolicy struct {
	InputDir            string
	AllowPrivateSources bool
}

func (p SourcePolicy) Validate(ctx context.Context, sources []domain.SourceConfig) error {
	for i, source := range sources {
		var err error
		if strings.Contains(source.Endpoint, "://") {
			err = p.validateURL(ctx, source.Endpoint)
		} else {
			err = p.validateFile(source.Endpoint)
		}
		if err != nil {
			return fmt.Errorf("sources[%d].endpoint: %w", i, err)
		}
	}
	return nil
}

func (p SourcePolicy) validateFile(path string) error {
	if p.InputDir == "" {
		return fmt.Errorf("input directory is not configured")
	}
	root, err := filepath.Abs(p.InputDir)
	if err != nil {
		return err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("input directory: %w", err)
	}
	file, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	file, err = filepath.EvalSymlinks(file)
	if err != nil {
		return fmt.Errorf("input file: %w", err)
	}
	relative, err := filepath.Rel(root, file)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("file must be inside INPUT_DIR")
	}
	info, err := os.Stat(file)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("source must be a regular file")
	}
	return nil
}

func (p SourcePolicy) validateURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return fmt.Errorf("only http(s) URLs without credentials are allowed")
	}
	if p.AllowPrivateSources {
		return nil
	}
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", u.Hostname())
	if err != nil {
		return fmt.Errorf("resolve source hostname: %w", err)
	}
	if len(addresses) == 0 {
		return fmt.Errorf("hostname has no addresses")
	}
	for _, address := range addresses {
		if !isPublicIP(address) {
			return fmt.Errorf("private or reserved network addresses are not allowed")
		}
	}
	return nil
}

func isPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range []string{"100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32"} {
		if netip.MustParsePrefix(prefix).Contains(ip) {
			return false
		}
	}
	return true
}
