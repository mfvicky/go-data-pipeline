//go:build go1.24

package security

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Go 1.24+ os.Root confines resolution atomically, including symlinks.
func openScopedFile(base, endpoint string) (*os.File, error) {
	rootPath, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	filePath, err := filepath.Abs(endpoint)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(rootPath, filePath)
	if err != nil || relative == ".." || relative == "." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return nil, fmt.Errorf("source file must be inside INPUT_DIR")
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	return root.Open(relative)
}
