package server

import (
	"os"
	"path/filepath"
	"testing"

	"gvisor.dev/gvisor/pkg/cleanup"
)

// These tests simulate the cleanup stack logic from createVM.
// Full integration tests for createVM are not feasible here because createVM
// requires QEMU/cloud-hypervisor, tap devices, IP allocators, etc.
// Instead we replicate the exact conditional-cleanup pattern used in createVM
// to verify that pre-existing state dirs and disks survive error-path cleanup.

func TestCreateVM_PreExistingStateDir_FailedBoot(t *testing.T) {
	tmpDir := t.TempDir()
	vmStateDir := filepath.Join(tmpDir, "vm-state", "test-vm")

	// Pre-create the state dir to simulate a pre-existing VM.
	if err := os.MkdirAll(vmStateDir, 0755); err != nil {
		t.Fatalf("failed to create pre-existing state dir: %v", err)
	}
	// Put a marker file inside to verify dir survives.
	markerPath := filepath.Join(vmStateDir, "marker.txt")
	if err := os.WriteFile(markerPath, []byte("important"), 0644); err != nil {
		t.Fatalf("failed to write marker: %v", err)
	}

	// Replicate createVM cleanup logic.
	cu := cleanup.Make(func() {})
	defer cu.Clean()

	_, stateDirStatErr := os.Stat(vmStateDir)
	stateDirPreExisted := stateDirStatErr == nil

	_ = os.MkdirAll(vmStateDir, 0755)
	cu.Add(func() {
		if !stateDirPreExisted {
			os.RemoveAll(vmStateDir)
		}
	})

	// Simulate boot failure: don't call cu.Release(), so cleanup fires.
	cu.Clean()

	// Assert dir and marker survive.
	if _, err := os.Stat(vmStateDir); err != nil {
		t.Errorf("pre-existing state dir was removed: %v", err)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Errorf("marker file in pre-existing state dir was removed: %v", err)
	}
}

func TestCreateVM_PreExistingDisk_FailedBoot(t *testing.T) {
	tmpDir := t.TempDir()
	vmStateDir := filepath.Join(tmpDir, "vm-state", "test-vm")
	if err := os.MkdirAll(vmStateDir, 0755); err != nil {
		t.Fatalf("setup: %v", err)
	}

	statefulDiskPath := filepath.Join(vmStateDir, "stateful.img")
	originalContent := []byte("disk data that must survive")
	if err := os.WriteFile(statefulDiskPath, originalContent, 0644); err != nil {
		t.Fatalf("failed to write pre-existing disk: %v", err)
	}

	cu := cleanup.Make(func() {})
	defer cu.Clean()

	_, diskStatErr := os.Stat(statefulDiskPath)
	diskPreExisted := diskStatErr == nil

	cu.Add(func() {
		if !diskPreExisted {
			if err := os.Remove(statefulDiskPath); err != nil && !os.IsNotExist(err) {
				t.Errorf("unexpected remove error: %v", err)
			}
		}
	})

	// Simulate boot failure.
	cu.Clean()

	// Assert disk survives.
	got, err := os.ReadFile(statefulDiskPath)
	if err != nil {
		t.Fatalf("pre-existing disk was removed: %v", err)
	}
	if string(got) != string(originalContent) {
		t.Errorf("disk content changed: got %q, want %q", string(got), string(originalContent))
	}
}

func TestCreateVM_FreshName_FailedBoot(t *testing.T) {
	tmpDir := t.TempDir()
	vmStateDir := filepath.Join(tmpDir, "vm-state", "fresh-vm")

	cu := cleanup.Make(func() {})
	defer cu.Clean()

	_, stateDirStatErr := os.Stat(vmStateDir)
	stateDirPreExisted := stateDirStatErr == nil

	if err := os.MkdirAll(vmStateDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	cu.Add(func() {
		if !stateDirPreExisted {
			os.RemoveAll(vmStateDir)
		}
	})

	// Simulate boot failure.
	cu.Clean()

	// Assert fresh dir is cleaned up.
	if _, err := os.Stat(vmStateDir); !os.IsNotExist(err) {
		t.Errorf("fresh state dir should have been removed, but still exists")
	}
}

func TestCreateVM_FreshName_FailedBoot_DiskCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	vmStateDir := filepath.Join(tmpDir, "vm-state", "fresh-vm")
	if err := os.MkdirAll(vmStateDir, 0755); err != nil {
		t.Fatalf("setup: %v", err)
	}

	statefulDiskPath := filepath.Join(vmStateDir, "stateful.img")

	cu := cleanup.Make(func() {})
	defer cu.Clean()

	// Disk doesn't pre-exist.
	_, diskStatErr := os.Stat(statefulDiskPath)
	diskPreExisted := diskStatErr == nil

	// Simulate disk creation (without truncate/mkfs, just create a file).
	if err := os.WriteFile(statefulDiskPath, []byte("new disk"), 0644); err != nil {
		t.Fatalf("failed to create test disk: %v", err)
	}

	cu.Add(func() {
		if !diskPreExisted {
			if err := os.Remove(statefulDiskPath); err != nil && !os.IsNotExist(err) {
				t.Errorf("unexpected remove error: %v", err)
			}
		}
	})

	// Simulate boot failure.
	cu.Clean()

	// Assert fresh disk is cleaned up.
	if _, err := os.Stat(statefulDiskPath); !os.IsNotExist(err) {
		t.Errorf("fresh disk should have been removed, but still exists")
	}
}

func TestCreateVM_Success_NoCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	vmStateDir := filepath.Join(tmpDir, "vm-state", "success-vm")

	cu := cleanup.Make(func() {})

	_, stateDirStatErr := os.Stat(vmStateDir)
	stateDirPreExisted := stateDirStatErr == nil

	if err := os.MkdirAll(vmStateDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	cu.Add(func() {
		if !stateDirPreExisted {
			os.RemoveAll(vmStateDir)
		}
	})

	statefulDiskPath := filepath.Join(vmStateDir, "stateful.img")
	_, diskStatErr := os.Stat(statefulDiskPath)
	diskPreExisted := diskStatErr == nil
	if err := os.WriteFile(statefulDiskPath, []byte("success disk"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cu.Add(func() {
		if !diskPreExisted {
			if err := os.Remove(statefulDiskPath); err != nil && !os.IsNotExist(err) {
				t.Errorf("unexpected remove error: %v", err)
			}
		}
	})

	// Simulate success: call Release so cleanup does NOT fire.
	cu.Release()

	// Assert both dir and disk survive.
	if _, err := os.Stat(vmStateDir); err != nil {
		t.Errorf("state dir should survive on success: %v", err)
	}
	if _, err := os.Stat(statefulDiskPath); err != nil {
		t.Errorf("disk should survive on success: %v", err)
	}
}
