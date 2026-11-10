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
	OutputDir           string
	AllowPrivateSources bool
	AllowedHosts        []string
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
	if len(p.AllowedHosts) > 0 && !allowedHost(u.Hostname(), p.AllowedHosts) {
		return fmt.Errorf("source hostname is not in SOURCE_HOST_ALLOWLIST")
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

// ValidateExports limits API-selected exports to an explicit output directory.
// The check rejects existing symlink components, but is not a replacement for
// OS-level filesystem isolation against a malicious local process.
func (p SourcePolicy) ValidateExports(targets []domain.ExportConfig) error {
	if len(targets) == 0 {
		return nil
	}
	fileExports := false
	for _, target := range targets {
		if target.Type != "postgres" {
			fileExports = true
			break
		}
	}
	if !fileExports {
		return nil
	}
	if p.OutputDir == "" {
		return fmt.Errorf("output directory is not configured")
	}
	root, err := filepath.Abs(p.OutputDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	for i, target := range targets {
		if target.Type == "postgres" {
			continue
		}
		path, err := filepath.Abs(target.Path)
		if err != nil {
			return fmt.Errorf("exports[%d].path: %w", i, err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("exports[%d].path: must be inside OUTPUT_DIR", i)
		}
		current := root
		for _, piece := range strings.Split(relative, string(os.PathSeparator)) {
			current = filepath.Join(current, piece)
			info, statErr := os.Lstat(current)
			if statErr != nil {
				if os.IsNotExist(statErr) {
					continue
				}
				return fmt.Errorf("exports[%d].path: %w", i, statErr)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("exports[%d].path: symlink components are not allowed", i)
			}
		}
	}
	return nil
}

// Allowed hosts match exact names or explicit *.suffix patterns; plain suffixes
// never match subdomains to avoid example.com.evil.com bypasses.
func allowedHost(host string, patterns []string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, item := range patterns {
		item = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(item), "."))
		if item == host {
			return true
		}
		if strings.HasPrefix(item, "*.") && strings.HasSuffix(host, item[1:]) && host != item[2:] {
			return true
		}
	}
	return false
}
