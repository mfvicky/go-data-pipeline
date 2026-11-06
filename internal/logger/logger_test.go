package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestLoggerLevelAndFormat(t *testing.T) {
	var output bytes.Buffer
	log, err := New(&output, "warn", "json")
	if err != nil {
		t.Fatal(err)
	}
	log.Info("hidden")
	log.Warn("visible")
	if strings.Contains(output.String(), "hidden") || !strings.Contains(output.String(), "visible") {
		t.Fatalf("level filtering: %q", output.String())
	}
	for _, tc := range []struct{ level, format string }{{"noise", "json"}, {"info", "xml"}} {
		if _, err := New(&output, tc.level, tc.format); err == nil {
			t.Fatal("expected invalid logger configuration")
		}
	}
}
