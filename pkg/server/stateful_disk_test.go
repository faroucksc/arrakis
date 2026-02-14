package server

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCreateStatefulDisk_ExistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	diskPath := filepath.Join(tmpDir, "existing.img")

	// Create a file with known content to simulate pre-existing disk.
	originalContent := []byte("existing disk data")
	if err := os.WriteFile(diskPath, originalContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Call createStatefulDisk — should skip because file exists and has size > 0.
	err := createStatefulDisk(diskPath, 100)
	if err != nil {
		t.Fatalf("createStatefulDisk returned error for existing file: %v", err)
	}

	// Verify file content is unchanged.
	gotContent, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("failed to read file after createStatefulDisk: %v", err)
	}
	if string(gotContent) != string(originalContent) {
		t.Errorf("file content changed: got %q, want %q", string(gotContent), string(originalContent))
	}
}

func TestCreateStatefulDisk_ZeroSizeFile(t *testing.T) {
	// A zero-byte file should NOT be treated as an existing disk.
	// createStatefulDisk must proceed past the existence guard and attempt
	// to truncate+format, effectively treating it as a fresh path.
	tmpDir := t.TempDir()
	diskPath := filepath.Join(tmpDir, "zero.img")

	// Create a 0-byte file.
	if err := os.WriteFile(diskPath, nil, 0644); err != nil {
		t.Fatalf("failed to create zero-byte file: %v", err)
	}

	err := createStatefulDisk(diskPath, 10)

	if _, lookErr := exec.LookPath("truncate"); lookErr != nil {
		// On systems without truncate (e.g., macOS) the function should
		// fail at the truncate step — NOT return nil from the guard.
		if err == nil {
			t.Fatal("expected error from truncate on system without truncate, got nil (guard returned early)")
		}
		t.Logf("correctly proceeded past guard; truncate unavailable: %v", err)
		return
	}
	if _, lookErr := exec.LookPath("mkfs.ext4"); lookErr != nil {
		// truncate succeeded but mkfs.ext4 missing — still proves guard was bypassed.
		if err == nil {
			t.Fatal("expected error from mkfs.ext4 on system without it, got nil")
		}
		t.Logf("correctly proceeded past guard; mkfs.ext4 unavailable: %v", err)
		return
	}

	// Full toolchain available — verify the file was overwritten.
	if err != nil {
		t.Fatalf("createStatefulDisk returned error: %v", err)
	}
	info, err := os.Stat(diskPath)
	if err != nil {
		t.Fatalf("disk file missing after createStatefulDisk: %v", err)
	}
	if info.Size() == 0 {
		t.Error("disk file still zero-size after createStatefulDisk")
	}
}

func TestCreateStatefulDisk_FreshPath(t *testing.T) {
	// Skip on systems without truncate and mkfs.ext4 (e.g., macOS).
	if _, err := exec.LookPath("truncate"); err != nil {
		t.Skip("truncate not available, skipping fresh disk creation test")
	}
	if _, err := exec.LookPath("mkfs.ext4"); err != nil {
		t.Skip("mkfs.ext4 not available, skipping fresh disk creation test")
	}

	tmpDir := t.TempDir()
	diskPath := filepath.Join(tmpDir, "new.img")

	err := createStatefulDisk(diskPath, 10)
	if err != nil {
		t.Fatalf("createStatefulDisk returned error for fresh path: %v", err)
	}

	info, err := os.Stat(diskPath)
	if err != nil {
		t.Fatalf("disk file was not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("disk file has zero size after creation")
	}
}
