// Package config loads Kage broker configuration.
//
// Priority (highest to lowest):
//  1. kage.yaml in the current working directory
//  2. Environment variables (KAGE_*)
//  3. Built-in defaults
//
// The YAML parser handles only flat "key: value" lines — no nesting, no
// multi-document support, no external dependencies.
package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const DefaultConfigFile = "kage.yaml"

// Config holds all tunable parameters for the Kage broker.
type Config struct {
	// Port is the TCP port the broker listens on (default 9092).
	Port int

	// LogDirectory is the directory where segment files are stored (default "/data").
	LogDirectory string

	// MaxSegmentSize is the maximum size in bytes of a single log segment
	// (default 1 GiB = 1073741824).
	MaxSegmentSize int64

	// WorkerPoolSize is the maximum number of concurrent client connections
	// (default 10000).
	WorkerPoolSize int

	// ShutdownTimeout is how long the broker waits for in-flight connections
	// to drain before forcing exit (default 10s).
	ShutdownTimeout time.Duration
}

// defaults returns a Config pre-filled with the built-in defaults.
func defaults() Config {
	return Config{
		Port:            9092,
		LogDirectory:    "/data",
		MaxSegmentSize:  1 << 30, // 1 GiB
		WorkerPoolSize:  10_000,
		ShutdownTimeout: 10 * time.Second,
	}
}

// Load returns a Config populated from (in priority order):
//  1. kage.yaml (path controlled by configFile; pass DefaultConfigFile for the standard location)
//  2. Environment variables
//  3. Built-in defaults
func Load(configFile string) (*Config, error) {
	cfg := defaults()

	// 1. Try the YAML file.
	f, err := os.Open(configFile)
	if err == nil {
		if err := parseYAML(f, &cfg); err != nil {
			f.Close()
			return nil, fmt.Errorf("config: parse %s: %w", configFile, err)
		}
		f.Close()
	}
	// A missing file is not an error — fall through to env vars.

	// 2. Overlay environment variables (they win over the file).
	if err := applyEnv(&cfg); err != nil {
		return nil, fmt.Errorf("config: env: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: invalid: %w", err)
	}

	return &cfg, nil
}

// Addr returns the full TCP listen address derived from Port.
func (c *Config) Addr() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

// ── YAML parser ───────────────────────────────────────────────────────────────

// parseYAML reads r line by line and applies recognised "key: value" pairs to
// cfg.  Lines beginning with '#' or containing no ':' are silently ignored.
func parseYAML(r io.Reader, cfg *Config) error {
	scanner := bufio.NewScanner(r)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		// Strip surrounding single or double quotes.
		if len(val) >= 2 && (val[0] == '"' && val[len(val)-1] == '"' ||
			val[0] == '\'' && val[len(val)-1] == '\'') {
			val = val[1 : len(val)-1]
		}
		// Strip inline comments.
		if ci := strings.IndexByte(val, '#'); ci >= 0 {
			val = strings.TrimSpace(val[:ci])
		}
		if err := applyField(cfg, key, val); err != nil {
			return fmt.Errorf("line %d (%q): %w", lineNo, key, err)
		}
	}
	return scanner.Err()
}

// ── Environment variables ─────────────────────────────────────────────────────

// applyEnv reads KAGE_* variables and overlays them on cfg.
// An unset variable is a no-op; an invalid value is an error.
func applyEnv(cfg *Config) error {
	vars := map[string]string{
		"port":             os.Getenv("KAGE_PORT"),
		"log_directory":    os.Getenv("KAGE_LOG_DIRECTORY"),
		"max_segment_size": os.Getenv("KAGE_MAX_SEGMENT_SIZE"),
		"worker_pool_size": os.Getenv("KAGE_WORKER_POOL_SIZE"),
		"shutdown_timeout": os.Getenv("KAGE_SHUTDOWN_TIMEOUT"),
	}
	for key, val := range vars {
		if val == "" {
			continue
		}
		if err := applyField(cfg, key, val); err != nil {
			return fmt.Errorf("KAGE_%s=%q: %w", strings.ToUpper(key), val, err)
		}
	}
	return nil
}

// ── Field dispatcher ──────────────────────────────────────────────────────────

// applyField sets the Config field identified by key to the string value val.
// key must match the YAML key names (snake_case).
func applyField(cfg *Config, key, val string) error {
	switch key {
	case "port":
		n, err := parseInt(val, 1, 65535)
		if err != nil {
			return err
		}
		cfg.Port = int(n)

	case "log_directory":
		if val == "" {
			return fmt.Errorf("must not be empty")
		}
		cfg.LogDirectory = val

	case "max_segment_size":
		n, err := parseInt(val, 1, 1<<40)
		if err != nil {
			return err
		}
		cfg.MaxSegmentSize = n

	case "worker_pool_size":
		n, err := parseInt(val, 1, 1<<20)
		if err != nil {
			return err
		}
		cfg.WorkerPoolSize = int(n)

	case "shutdown_timeout":
		d, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", val, err)
		}
		if d < 0 {
			return fmt.Errorf("shutdown_timeout must be non-negative")
		}
		cfg.ShutdownTimeout = d

	default:
		// Unknown keys are silently ignored — forward-compatibility.
	}
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// parseInt parses s as a base-10 int64 and validates it is in [min, max].
func parseInt(s string, min, max int64) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a valid integer: %q", s)
	}
	if n < min || n > max {
		return 0, fmt.Errorf("value %d out of range [%d, %d]", n, min, max)
	}
	return n, nil
}

// validate checks internal consistency after all sources have been applied.
func (c *Config) validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("port %d out of range [1, 65535]", c.Port)
	}
	if c.LogDirectory == "" {
		return fmt.Errorf("log_directory must not be empty")
	}
	if c.MaxSegmentSize < 1 {
		return fmt.Errorf("max_segment_size must be at least 1")
	}
	if c.WorkerPoolSize < 1 {
		return fmt.Errorf("worker_pool_size must be at least 1")
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("shutdown_timeout must be non-negative")
	}
	return nil
}
