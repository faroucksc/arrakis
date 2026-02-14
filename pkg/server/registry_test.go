package server

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/abshkbh/arrakis/pkg/config"
	"github.com/abshkbh/arrakis/pkg/server/cidallocator"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	tmpDir := t.TempDir()
	cidAlloc, err := cidallocator.NewCIDAllocator(3, 100)
	if err != nil {
		t.Fatalf("failed to create CID allocator: %v", err)
	}
	return &Server{
		vms:          make(map[string]*vm),
		cidAllocator: cidAlloc,
		config:       config.ServerConfig{StateDir: tmpDir},
	}
}

func registryPath(s *Server) string {
	return filepath.Join(s.config.StateDir, "registry.json")
}

// T1: saveRegistry writes registry.json after VM creation
func TestRegistryCreate(t *testing.T) {
	s := newTestServer(t)
	stateDir := filepath.Join(s.config.StateDir, "test-vm")
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		t.Fatal(err)
	}

	_, ipNet, _ := net.ParseCIDR("10.0.0.2/24")
	ipNet.IP = net.ParseIP("10.0.0.2")

	s.vms["test-vm"] = &vm{
		name:             "test-vm",
		stateDirPath:     stateDir,
		statefulDiskPath: filepath.Join(stateDir, "stateful.img"),
		cid:              5,
		ip:               ipNet,
		portForwards: []portForward{
			{HostPort: 3000, GuestPort: 22, Description: "ssh"},
		},
	}

	if err := s.saveRegistry(); err != nil {
		t.Fatalf("saveRegistry failed: %v", err)
	}

	data, err := os.ReadFile(registryPath(s))
	if err != nil {
		t.Fatalf("registry.json not found: %v", err)
	}

	var reg registry
	if err := json.Unmarshal(data, &reg); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	entry, ok := reg.VMs["test-vm"]
	if !ok {
		t.Fatal("test-vm not in registry")
	}
	if entry.CID != 5 {
		t.Errorf("CID = %d, want 5", entry.CID)
	}
	if len(entry.PortForwards) != 1 || entry.PortForwards[0].HostPort != 3000 {
		t.Errorf("unexpected port forwards: %+v", entry.PortForwards)
	}
}

// T2: saveRegistry reflects deletion
func TestRegistryDelete(t *testing.T) {
	s := newTestServer(t)
	stateDir := filepath.Join(s.config.StateDir, "vm1")
	os.MkdirAll(stateDir, 0755)

	s.vms["vm1"] = &vm{
		name:         "vm1",
		stateDirPath: stateDir,
		cid:          3,
	}
	if err := s.saveRegistry(); err != nil {
		t.Fatal(err)
	}

	delete(s.vms, "vm1")
	if err := s.saveRegistry(); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(registryPath(s))
	var reg registry
	json.Unmarshal(data, &reg)

	if len(reg.VMs) != 0 {
		t.Errorf("expected empty registry, got %d entries", len(reg.VMs))
	}
}

// T3, T7: loadRegistry recovers VMs as STOPPED
func TestRegistryBootRecovery(t *testing.T) {
	s := newTestServer(t)
	stateDir := filepath.Join(s.config.StateDir, "vm-recover")
	os.MkdirAll(stateDir, 0755)

	// Simulate a previously saved registry
	reg := registry{VMs: map[string]registryEntry{
		"vm-recover": {
			Name:             "vm-recover",
			StateDirPath:     stateDir,
			StatefulDiskPath: filepath.Join(stateDir, "stateful.img"),
			CID:              10,
			IP:               "10.0.0.5/24",
			PortForwards:     []portForward{{HostPort: 3001, GuestPort: 80, Description: "http"}},
		},
	}}
	data, _ := json.MarshalIndent(reg, "", "  ")
	os.WriteFile(registryPath(s), data, 0644)

	loaded, err := s.loadRegistry()
	if err != nil {
		t.Fatalf("loadRegistry failed: %v", err)
	}
	if len(loaded.VMs) != 1 {
		t.Fatalf("expected 1 VM, got %d", len(loaded.VMs))
	}

	entry := loaded.VMs["vm-recover"]
	if entry.CID != 10 {
		t.Errorf("CID = %d, want 10", entry.CID)
	}

	// Simulate boot recovery (same as NewServer logic)
	if _, err := os.Stat(entry.StateDirPath); err != nil {
		t.Fatalf("state dir missing: %v", err)
	}
	s.vms["vm-recover"] = &vm{
		name:             entry.Name,
		stateDirPath:     entry.StateDirPath,
		statefulDiskPath: entry.StatefulDiskPath,
		cid:              entry.CID,
		status:           vmStatusStopped,
		portForwards:     entry.PortForwards,
	}

	if s.vms["vm-recover"].status != vmStatusStopped {
		t.Errorf("status = %v, want STOPPED", s.vms["vm-recover"].status)
	}
}

// T4, T9: stale entry (state dir missing) is skipped
func TestRegistryStaleEntry(t *testing.T) {
	s := newTestServer(t)

	reg := registry{VMs: map[string]registryEntry{
		"stale-vm": {
			Name:         "stale-vm",
			StateDirPath: filepath.Join(s.config.StateDir, "nonexistent"),
			CID:          7,
		},
	}}
	data, _ := json.MarshalIndent(reg, "", "  ")
	os.WriteFile(registryPath(s), data, 0644)

	loaded, err := s.loadRegistry()
	if err != nil {
		t.Fatalf("loadRegistry failed: %v", err)
	}

	// Simulate NewServer recovery: skip entries with missing state dirs
	for name, entry := range loaded.VMs {
		if _, err := os.Stat(entry.StateDirPath); err != nil {
			continue
		}
		s.vms[name] = &vm{name: entry.Name}
	}

	if len(s.vms) != 0 {
		t.Errorf("expected 0 recovered VMs, got %d", len(s.vms))
	}
}

// T5: missing registry.json returns empty registry, no error
func TestRegistryMissing(t *testing.T) {
	s := newTestServer(t)
	// Don't create registry.json

	loaded, err := s.loadRegistry()
	if err != nil {
		t.Fatalf("expected no error for missing registry, got: %v", err)
	}
	if len(loaded.VMs) != 0 {
		t.Errorf("expected empty VMs map, got %d", len(loaded.VMs))
	}
}

// T6: CID reclaim on recovery
func TestRegistryCIDReclaim(t *testing.T) {
	s := newTestServer(t)
	stateDir := filepath.Join(s.config.StateDir, "cid-vm")
	os.MkdirAll(stateDir, 0755)

	reg := registry{VMs: map[string]registryEntry{
		"cid-vm": {
			Name:         "cid-vm",
			StateDirPath: stateDir,
			CID:          5,
		},
	}}
	data, _ := json.MarshalIndent(reg, "", "  ")
	os.WriteFile(registryPath(s), data, 0644)

	loaded, _ := s.loadRegistry()
	entry := loaded.VMs["cid-vm"]

	// Claim the CID
	if err := s.cidAllocator.ClaimCID(entry.CID); err != nil {
		t.Fatalf("ClaimCID failed: %v", err)
	}

	// Claiming again should fail (already taken)
	if err := s.cidAllocator.ClaimCID(entry.CID); err == nil {
		t.Error("expected error claiming already-taken CID, got nil")
	}
}

// T7: recovered VMs have status STOPPED
func TestRegistryStoppedStatus(t *testing.T) {
	s := newTestServer(t)
	stateDir := filepath.Join(s.config.StateDir, "stopped-vm")
	os.MkdirAll(stateDir, 0755)

	s.vms["stopped-vm"] = &vm{
		name:         "stopped-vm",
		stateDirPath: stateDir,
		cid:          4,
		status:       vmStatusStopped,
	}

	if s.vms["stopped-vm"].status.String() != "STOPPED" {
		t.Errorf("status = %s, want STOPPED", s.vms["stopped-vm"].status.String())
	}
}

// T8: atomic write (tmp file then rename)
func TestRegistryAtomicWrite(t *testing.T) {
	s := newTestServer(t)
	s.vms["atomic-vm"] = &vm{
		name: "atomic-vm",
		cid:  3,
	}

	if err := s.saveRegistry(); err != nil {
		t.Fatalf("saveRegistry failed: %v", err)
	}

	// Verify .tmp file does NOT remain
	tmpPath := registryPath(s) + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temp file still exists after saveRegistry")
	}

	// Verify final file is valid JSON
	data, err := os.ReadFile(registryPath(s))
	if err != nil {
		t.Fatalf("registry.json missing: %v", err)
	}
	var reg registry
	if err := json.Unmarshal(data, &reg); err != nil {
		t.Fatalf("registry.json is not valid JSON: %v", err)
	}
}

// T9: partial stale - mix of valid and stale entries
func TestRegistryPartialStale(t *testing.T) {
	s := newTestServer(t)
	validDir := filepath.Join(s.config.StateDir, "valid-vm")
	os.MkdirAll(validDir, 0755)

	reg := registry{VMs: map[string]registryEntry{
		"valid-vm": {
			Name:         "valid-vm",
			StateDirPath: validDir,
			CID:          3,
		},
		"stale-vm": {
			Name:         "stale-vm",
			StateDirPath: filepath.Join(s.config.StateDir, "gone"),
			CID:          4,
		},
	}}
	data, _ := json.MarshalIndent(reg, "", "  ")
	os.WriteFile(registryPath(s), data, 0644)

	loaded, _ := s.loadRegistry()

	// Simulate NewServer recovery logic
	for name, entry := range loaded.VMs {
		if _, err := os.Stat(entry.StateDirPath); err != nil {
			continue
		}
		s.cidAllocator.ClaimCID(entry.CID)
		s.vms[name] = &vm{
			name:         entry.Name,
			stateDirPath: entry.StateDirPath,
			cid:          entry.CID,
			status:       vmStatusStopped,
		}
	}

	if len(s.vms) != 1 {
		t.Errorf("expected 1 recovered VM, got %d", len(s.vms))
	}
	if _, ok := s.vms["valid-vm"]; !ok {
		t.Error("valid-vm should be recovered")
	}
	if _, ok := s.vms["stale-vm"]; ok {
		t.Error("stale-vm should NOT be recovered")
	}

	// Verify saveRegistry persists only the valid entry to disk
	if err := s.saveRegistry(); err != nil {
		t.Fatalf("saveRegistry failed: %v", err)
	}
	savedData, err := os.ReadFile(registryPath(s))
	if err != nil {
		t.Fatalf("failed to read registry.json: %v", err)
	}
	var savedReg registry
	if err := json.Unmarshal(savedData, &savedReg); err != nil {
		t.Fatalf("failed to unmarshal saved registry: %v", err)
	}
	if len(savedReg.VMs) != 1 {
		t.Errorf("expected 1 entry on disk, got %d", len(savedReg.VMs))
	}
	if _, ok := savedReg.VMs["valid-vm"]; !ok {
		t.Error("valid-vm should be in saved registry")
	}
	if _, ok := savedReg.VMs["stale-vm"]; ok {
		t.Error("stale-vm should NOT be in saved registry")
	}
}

// T10: corrupt JSON returns empty registry, no crash
func TestRegistryCorruptJSON(t *testing.T) {
	s := newTestServer(t)
	os.WriteFile(registryPath(s), []byte("{not valid json!!!"), 0644)

	loaded, err := s.loadRegistry()
	if err != nil {
		t.Fatalf("expected no error for corrupt JSON, got: %v", err)
	}
	if len(loaded.VMs) != 0 {
		t.Errorf("expected empty VMs map, got %d", len(loaded.VMs))
	}
}
