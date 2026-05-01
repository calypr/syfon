package config

import (
	"os"
	"path"
	"path/filepath"
	"testing"
)

// Test MED-3 fix: Config directory created with 0700 permissions
func TestEnsureExists_DirectoryPermissions(t *testing.T) {
	defer func() {
		os.Unsetenv("HOME")
	}()

	// Create a temporary directory to use as HOME
	tmpHome := t.TempDir()
	os.Setenv("HOME", tmpHome)

	manager := &Manager{}

	// Call EnsureExists
	err := manager.EnsureExists()
	if err != nil {
		t.Fatalf("EnsureExists() error = %v", err)
	}

	// Check that the config directory was created with restrictive permissions
	configPath := path.Join(tmpHome, ".gen3")
	stat, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("os.Stat() error = %v", err)
	}

	perms := stat.Mode().Perm()

	// Should be 0700 (owner read/write/exec only)
	expectedPerms := os.FileMode(0700)
	if perms != expectedPerms {
		t.Errorf(".gen3 directory permissions = %o, want %o", perms, expectedPerms)
	}

	if !stat.IsDir() {
		t.Errorf(".gen3 is not a directory")
	}
}

// Test MED-3 fix: Config file is created
func TestEnsureExists_ConfigFileCreation(t *testing.T) {
	defer func() {
		os.Unsetenv("HOME")
	}()

	tmpHome := t.TempDir()
	os.Setenv("HOME", tmpHome)

	manager := &Manager{}

	// Call EnsureExists
	err := manager.EnsureExists()
	if err != nil {
		t.Fatalf("EnsureExists() error = %v", err)
	}

	// Check that the config file was created
	configPath := path.Join(tmpHome, ".gen3", "gen3_client_config.ini")
	_, err = os.Stat(configPath)
	if err != nil {
		t.Fatalf("Config file not created: %v", err)
	}
}

// Test MED-3 fix: Directory permissions in nested path
func TestConfigPath_NestedDirectoryStructure(t *testing.T) {
	tmpHome := t.TempDir()
	defer os.RemoveAll(tmpHome)

	// Set HOME to our temp directory
	oldHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpHome)
	defer os.Setenv("HOME", oldHome)

	manager := &Manager{}

	// Ensure the directory structure exists
	if err := manager.EnsureExists(); err != nil {
		t.Fatalf("EnsureExists() failed: %v", err)
	}

	// Verify the directory structure
	gen3Dir := filepath.Join(tmpHome, ".gen3")
	stat, err := os.Stat(gen3Dir)
	if err != nil {
		t.Fatalf("Failed to stat .gen3 directory: %v", err)
	}

	if !stat.IsDir() {
		t.Fatal(".gen3 is not a directory")
	}

	perms := stat.Mode().Perm()
	// Check that permissions are restricted (owner only)
	if perms&0077 != 0 {
		t.Errorf("Directory permissions allow non-owner access: %o", perms)
	}
}
