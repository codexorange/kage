package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// writeYAML writes content to a temp file and returns its path.
func writeYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "kage.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeYAML: %v", err)
	}
	return path
}

// clearEnv unsets all KAGE_* variables and restores them on cleanup.
func clearEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		"KAGE_PORT", "KAGE_LOG_DIRECTORY",
		"KAGE_MAX_SEGMENT_SIZE", "KAGE_WORKER_POOL_SIZE",
		"KAGE_SHUTDOWN_TIMEOUT",
	}
	saved := make(map[string]string, len(keys))
	for _, k := range keys {
		saved[k] = os.Getenv(k)
		os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for k, v := range saved {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	})
}

// ── defaults ──────────────────────────────────────────────────────────────────

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load("/nonexistent/kage.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9092 {
		t.Errorf("Port = %d, want 9092", cfg.Port)
	}
	if cfg.LogDirectory != "/data" {
		t.Errorf("LogDirectory = %q, want /data", cfg.LogDirectory)
	}
	if cfg.MaxSegmentSize != 1<<30 {
		t.Errorf("MaxSegmentSize = %d, want %d", cfg.MaxSegmentSize, 1<<30)
	}
	if cfg.WorkerPoolSize != 10_000 {
		t.Errorf("WorkerPoolSize = %d, want 10000", cfg.WorkerPoolSize)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
}

// ── YAML loading ──────────────────────────────────────────────────────────────

func TestLoad_YAML_AllFields(t *testing.T) {
	clearEnv(t)
	yaml := `
# Kage broker config
port: 9093
log_directory: /var/kage
max_segment_size: 536870912
worker_pool_size: 500
shutdown_timeout: 30s
`
	path := writeYAML(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9093 {
		t.Errorf("Port = %d, want 9093", cfg.Port)
	}
	if cfg.LogDirectory != "/var/kage" {
		t.Errorf("LogDirectory = %q, want /var/kage", cfg.LogDirectory)
	}
	if cfg.MaxSegmentSize != 536870912 {
		t.Errorf("MaxSegmentSize = %d, want 536870912", cfg.MaxSegmentSize)
	}
	if cfg.WorkerPoolSize != 500 {
		t.Errorf("WorkerPoolSize = %d, want 500", cfg.WorkerPoolSize)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 30s", cfg.ShutdownTimeout)
	}
}

func TestLoad_YAML_CommentsAndBlankLines(t *testing.T) {
	clearEnv(t)
	yaml := `
# this is a comment
port: 9094  # inline comment
# another comment

log_directory: /tmp/kage
`
	path := writeYAML(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9094 {
		t.Errorf("Port = %d, want 9094", cfg.Port)
	}
	if cfg.LogDirectory != "/tmp/kage" {
		t.Errorf("LogDirectory = %q, want /tmp/kage", cfg.LogDirectory)
	}
}

func TestLoad_YAML_UnknownKeysIgnored(t *testing.T) {
	clearEnv(t)
	yaml := `
port: 9095
unknown_future_key: some_value
`
	path := writeYAML(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load with unknown key: %v", err)
	}
	if cfg.Port != 9095 {
		t.Errorf("Port = %d, want 9095", cfg.Port)
	}
}

func TestLoad_YAML_MissingFileUsesDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load("/does/not/exist/kage.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9092 {
		t.Errorf("Port = %d, want 9092 (default)", cfg.Port)
	}
}

func TestLoad_YAML_InvalidPort(t *testing.T) {
	clearEnv(t)
	path := writeYAML(t, "port: notanumber\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for non-numeric port, got nil")
	}
}

func TestLoad_YAML_PortOutOfRange(t *testing.T) {
	clearEnv(t)
	path := writeYAML(t, "port: 99999\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for port > 65535, got nil")
	}
}

func TestLoad_YAML_InvalidDuration(t *testing.T) {
	clearEnv(t)
	path := writeYAML(t, "shutdown_timeout: not-a-duration\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid duration, got nil")
	}
}

func TestLoad_YAML_InvalidMaxSegmentSize(t *testing.T) {
	clearEnv(t)
	path := writeYAML(t, "max_segment_size: -1\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for negative max_segment_size, got nil")
	}
}

func TestLoad_YAML_InvalidWorkerPoolSize(t *testing.T) {
	clearEnv(t)
	path := writeYAML(t, "worker_pool_size: 0\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for worker_pool_size=0, got nil")
	}
}

// ── Environment variables ─────────────────────────────────────────────────────

func TestLoad_Env_AllVars(t *testing.T) {
	clearEnv(t)
	os.Setenv("KAGE_PORT", "9099")
	os.Setenv("KAGE_LOG_DIRECTORY", "/env/dir")
	os.Setenv("KAGE_MAX_SEGMENT_SIZE", "104857600")
	os.Setenv("KAGE_WORKER_POOL_SIZE", "250")
	os.Setenv("KAGE_SHUTDOWN_TIMEOUT", "5s")

	cfg, err := Load("/nonexistent/kage.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9099 {
		t.Errorf("Port = %d, want 9099", cfg.Port)
	}
	if cfg.LogDirectory != "/env/dir" {
		t.Errorf("LogDirectory = %q, want /env/dir", cfg.LogDirectory)
	}
	if cfg.MaxSegmentSize != 104857600 {
		t.Errorf("MaxSegmentSize = %d, want 104857600", cfg.MaxSegmentSize)
	}
	if cfg.WorkerPoolSize != 250 {
		t.Errorf("WorkerPoolSize = %d, want 250", cfg.WorkerPoolSize)
	}
	if cfg.ShutdownTimeout != 5*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 5s", cfg.ShutdownTimeout)
	}
}

func TestLoad_Env_InvalidPort(t *testing.T) {
	clearEnv(t)
	os.Setenv("KAGE_PORT", "nope")
	_, err := Load("/nonexistent/kage.yaml")
	if err == nil {
		t.Fatal("expected error for invalid KAGE_PORT, got nil")
	}
}

func TestLoad_Env_InvalidDuration(t *testing.T) {
	clearEnv(t)
	os.Setenv("KAGE_SHUTDOWN_TIMEOUT", "forever")
	_, err := Load("/nonexistent/kage.yaml")
	if err == nil {
		t.Fatal("expected error for invalid KAGE_SHUTDOWN_TIMEOUT, got nil")
	}
}

// ── Priority: env wins over YAML ──────────────────────────────────────────────

func TestLoad_EnvOverridesYAML(t *testing.T) {
	clearEnv(t)
	path := writeYAML(t, "port: 9093\nlog_directory: /from/yaml\n")
	os.Setenv("KAGE_PORT", "9100") // env wins

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9100 {
		t.Errorf("Port = %d, want 9100 (env should override YAML)", cfg.Port)
	}
	// LogDirectory not set in env → YAML value survives.
	if cfg.LogDirectory != "/from/yaml" {
		t.Errorf("LogDirectory = %q, want /from/yaml", cfg.LogDirectory)
	}
}

// ── Addr helper ───────────────────────────────────────────────────────────────

func TestConfig_Addr(t *testing.T) {
	cfg := &Config{Port: 9092}
	if got := cfg.Addr(); got != "0.0.0.0:9092" {
		t.Errorf("Addr = %q, want 0.0.0.0:9092", got)
	}
}

// ── parseYAML edge cases ──────────────────────────────────────────────────────

func TestParseYAML_LinesWithoutColon(t *testing.T) {
	clearEnv(t)
	// Lines without ':' should be silently skipped.
	path := writeYAML(t, "just a line with no colon\nport: 9092\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 9092 {
		t.Errorf("Port = %d, want 9092", cfg.Port)
	}
}

func TestParseYAML_ValueWithColon(t *testing.T) {
	clearEnv(t)
	// log_directory can contain ':' on some systems — only first ':' is the separator.
	path := writeYAML(t, "log_directory: /data/kage:test\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LogDirectory != "/data/kage:test" {
		t.Errorf("LogDirectory = %q, want /data/kage:test", cfg.LogDirectory)
	}
}

// ── validate ──────────────────────────────────────────────────────────────────

func TestValidate_EmptyLogDirectory(t *testing.T) {
	clearEnv(t)
	// Provide a YAML that sets log_directory to empty — applyField returns error
	// because it rejects empty string directly.
	yaml := "log_directory: \"\"\n"
	// parseYAML will see val="" and applyField will return an error.
	r := strings.NewReader(yaml)
	cfg := defaults()
	err := parseYAML(r, &cfg)
	if err == nil {
		t.Fatal("expected error for empty log_directory, got nil")
	}
}

func TestValidate_NegativeShutdownTimeout(t *testing.T) {
	clearEnv(t)
	path := writeYAML(t, "shutdown_timeout: -1s\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for negative shutdown_timeout, got nil")
	}
}
