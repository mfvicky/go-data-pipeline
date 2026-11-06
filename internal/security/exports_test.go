package security

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestExportPathConfinement(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "output")
	policy := SourcePolicy{OutputDir: root}
	allowed := []domain.ExportConfig{{Type: "json", Path: filepath.Join(root, "report.json")}, {Type: "csv", Path: filepath.Join(root, "nested", "records.csv")}}
	if err := policy.ValidateExports(allowed); err != nil {
		t.Fatal(err)
	}
	outside := []domain.ExportConfig{{Type: "json", Path: filepath.Join(dir, "secret.json")}}
	if err := policy.ValidateExports(outside); err == nil {
		t.Fatal("should reject writes outside output directory")
	}
	if err := policy.ValidateExports([]domain.ExportConfig{{Type: "json", Path: root}}); err == nil {
		t.Fatal("should reject output root as a file")
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir, filepath.Join(root, "escape")); err == nil {
		if err := policy.ValidateExports([]domain.ExportConfig{{Type: "json", Path: filepath.Join(root, "escape", "data.json")}}); err == nil {
			t.Fatal("should reject symlink escape")
		}
	}
}
