package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "nsync.toml")
	content := `
[[input.mysql]]
name = "primary_mysql"
host = "127.0.0.1"
port = 3306
username = "root"
password = "123456"

[[input.postgres]]
name = "replica_pg"
host = "127.0.0.1"
port = 5432
username = "postgres"
password = "123456"
database = "appdb"
slot = "nsync_slot"
publication = "nsync_pub"
status_interval_sec = 10
`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if len(cfg.Input.Mysqls) != 1 {
		t.Fatalf("expected 1 mysql source, got %d", len(cfg.Input.Mysqls))
	}
	if len(cfg.Input.Pgsqls) != 1 {
		t.Fatalf("expected 1 postgres source, got %d", len(cfg.Input.Pgsqls))
	}
	if cfg.Input.Mysqls[0].Name != "primary_mysql" {
		t.Fatalf("mysql source name = %q, want primary_mysql", cfg.Input.Mysqls[0].Name)
	}
	if cfg.Input.Pgsqls[0].Name != "replica_pg" {
		t.Fatalf("postgres source name = %q, want replica_pg", cfg.Input.Pgsqls[0].Name)
	}
}
