//go:build !go1.24

package security

import (
	"os"
)

// Legacy Go fallback: revalidation is not atomic against malicious local
// symlink changes. Use Go 1.24+ and filesystem sandboxing for untrusted input.
func openScopedFile(base, endpoint string) (*os.File, error) { return os.Open(endpoint) }
