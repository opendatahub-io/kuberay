package support

import (
	"fmt"
	"testing"
	"time"
)

// LogWithTimestamp logs a message with a timestamp prefix
// This is useful for debugging test failures by showing when each event occurred
func LogWithTimestamp(t *testing.T, format string, args ...interface{}) {
	t.Helper()
	timestamp := time.Now().Format("15:04:05.000")
	message := fmt.Sprintf(format, args...)
	t.Logf("[%s] %s", timestamp, message)
}
