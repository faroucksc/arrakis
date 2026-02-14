package server

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/abshkbh/arrakis/pkg/server/fountain"
)

// T7: updateSnapshotConfig updates tap name and guest_ip correctly.
func TestUpdateSnapshotConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	oldCmdline := `console=ttyS0 gateway_ip="10.0.0.1" guest_ip="10.0.0.5/24"`
	nets := []NetworkConfig{{Tap: "tap0"}}
	cfg := VMConfig{
		Net: &nets,
		Payload: PayloadConfig{
			Firmware: String("/path/to/firmware"),
			Cmdline:  &oldCmdline,
		},
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	newIP := &net.IPNet{
		IP:   net.ParseIP("10.0.0.99"),
		Mask: net.CIDRMask(24, 32),
	}

	if err := updateSnapshotConfig(configPath, "tap7", newIP); err != nil {
		t.Fatalf("updateSnapshotConfig: %v", err)
	}

	// Read back and verify
	updatedData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var updated VMConfig
	if err := json.Unmarshal(updatedData, &updated); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Verify tap name updated
	if updated.Net == nil || len(*updated.Net) == 0 {
		t.Fatal("net config missing after update")
	}
	if (*updated.Net)[0].Tap != "tap7" {
		t.Errorf("tap = %q, want %q", (*updated.Net)[0].Tap, "tap7")
	}

	// Verify guest_ip updated in cmdline
	if updated.Payload.Cmdline == nil {
		t.Fatal("cmdline nil after update")
	}
	gotIP, err := extractGuestIPFromCmdline(*updated.Payload.Cmdline)
	if err != nil {
		t.Fatalf("extractGuestIP: %v", err)
	}
	if !gotIP.IP.Equal(newIP.IP) {
		t.Errorf("guest_ip = %v, want %v", gotIP.IP, newIP.IP)
	}
}

// T7: updateSnapshotConfig preserves ALL fields, including those NOT in VMConfig
// (cpus, memory, disks, vsock, serial, console).
func TestUpdateSnapshotConfigPreservesOtherFields(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	// Build a full CHV config as raw JSON including fields outside VMConfig struct
	fullConfig := map[string]interface{}{
		"payload": map[string]interface{}{
			"firmware":  "/path/to/firmware",
			"kernel":    "/path/to/kernel",
			"cmdline":   `console=ttyS0 gateway_ip="10.0.0.1" guest_ip="10.0.0.5/24"`,
			"initramfs": "/path/to/initramfs",
		},
		"net": []interface{}{
			map[string]interface{}{
				"tap":        "tap0",
				"num_queues": float64(2),
				"queue_size": float64(256),
				"id":         "_net0",
			},
		},
		"cpus": map[string]interface{}{
			"boot_vcpus": float64(2),
			"max_vcpus":  float64(4),
		},
		"memory": map[string]interface{}{
			"size": float64(1073741824),
		},
		"disks": []interface{}{
			map[string]interface{}{
				"path":     "/path/to/rootfs.img",
				"readonly": true,
			},
			map[string]interface{}{
				"path": "/path/to/stateful.img",
			},
		},
		"vsock": map[string]interface{}{
			"cid":    float64(3),
			"socket": "/tmp/vsock.sock",
		},
		"serial": map[string]interface{}{
			"mode": "Tty",
		},
		"console": map[string]interface{}{
			"mode": "Off",
		},
	}

	data, err := json.MarshalIndent(fullConfig, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	newIP := &net.IPNet{
		IP:   net.ParseIP("10.0.0.50"),
		Mask: net.CIDRMask(24, 32),
	}
	if err := updateSnapshotConfig(configPath, "tap3", newIP); err != nil {
		t.Fatalf("updateSnapshotConfig: %v", err)
	}

	updatedData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var updated map[string]interface{}
	if err := json.Unmarshal(updatedData, &updated); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Verify tap and IP were updated
	nets := updated["net"].([]interface{})
	net0 := nets[0].(map[string]interface{})
	if net0["tap"] != "tap3" {
		t.Errorf("tap = %v, want tap3", net0["tap"])
	}
	payload := updated["payload"].(map[string]interface{})
	gotIP, err := extractGuestIPFromCmdline(payload["cmdline"].(string))
	if err != nil {
		t.Fatalf("extractGuestIP: %v", err)
	}
	if !gotIP.IP.Equal(newIP.IP) {
		t.Errorf("guest_ip = %v, want %v", gotIP.IP, newIP.IP)
	}

	// Verify payload sub-fields preserved
	if payload["firmware"] != "/path/to/firmware" {
		t.Errorf("firmware = %v, want /path/to/firmware", payload["firmware"])
	}
	if payload["kernel"] != "/path/to/kernel" {
		t.Errorf("kernel = %v, want /path/to/kernel", payload["kernel"])
	}
	if payload["initramfs"] != "/path/to/initramfs" {
		t.Errorf("initramfs = %v, want /path/to/initramfs", payload["initramfs"])
	}

	// Verify fields NOT in VMConfig survived round-trip
	cpus, ok := updated["cpus"].(map[string]interface{})
	if !ok {
		t.Fatal("cpus field lost after updateSnapshotConfig")
	}
	if cpus["boot_vcpus"] != float64(2) {
		t.Errorf("cpus.boot_vcpus = %v, want 2", cpus["boot_vcpus"])
	}
	if cpus["max_vcpus"] != float64(4) {
		t.Errorf("cpus.max_vcpus = %v, want 4", cpus["max_vcpus"])
	}

	memory, ok := updated["memory"].(map[string]interface{})
	if !ok {
		t.Fatal("memory field lost after updateSnapshotConfig")
	}
	if memory["size"] != float64(1073741824) {
		t.Errorf("memory.size = %v, want 1073741824", memory["size"])
	}

	disks, ok := updated["disks"].([]interface{})
	if !ok || len(disks) != 2 {
		t.Fatalf("disks field lost or wrong length after updateSnapshotConfig: %v", updated["disks"])
	}

	vsock, ok := updated["vsock"].(map[string]interface{})
	if !ok {
		t.Fatal("vsock field lost after updateSnapshotConfig")
	}
	if vsock["cid"] != float64(3) {
		t.Errorf("vsock.cid = %v, want 3", vsock["cid"])
	}
	if vsock["socket"] != "/tmp/vsock.sock" {
		t.Errorf("vsock.socket = %v, want /tmp/vsock.sock", vsock["socket"])
	}

	// Verify net sub-fields preserved (not just tap)
	if net0["num_queues"] != float64(2) {
		t.Errorf("net[0].num_queues = %v, want 2", net0["num_queues"])
	}
	if net0["id"] != "_net0" {
		t.Errorf("net[0].id = %v, want _net0", net0["id"])
	}

	// Verify serial and console preserved
	serial, ok := updated["serial"].(map[string]interface{})
	if !ok {
		t.Fatal("serial field lost after updateSnapshotConfig")
	}
	if serial["mode"] != "Tty" {
		t.Errorf("serial.mode = %v, want Tty", serial["mode"])
	}

	console, ok := updated["console"].(map[string]interface{})
	if !ok {
		t.Fatal("console field lost after updateSnapshotConfig")
	}
	if console["mode"] != "Off" {
		t.Errorf("console.mode = %v, want Off", console["mode"])
	}
}

// T5: copyAndPrepareSnapshot creates an isolated copy; original is unchanged.
func TestSnapshotCopyIsolation(t *testing.T) {
	// Create a fake snapshot directory
	origDir := t.TempDir()
	cmdline := `console=ttyS0 gateway_ip="10.0.0.1" guest_ip="10.0.0.5/24"`
	nets := []NetworkConfig{{Tap: "tap0"}}
	cfg := VMConfig{
		Net: &nets,
		Payload: PayloadConfig{
			Cmdline: &cmdline,
		},
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(origDir, "config.json"), data, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Write a dummy stateful disk
	if err := os.WriteFile(filepath.Join(origDir, "stateful.img"), []byte("disk-data"), 0644); err != nil {
		t.Fatalf("write stateful: %v", err)
	}

	newIP := &net.IPNet{
		IP:   net.ParseIP("10.0.0.77"),
		Mask: net.CIDRMask(24, 32),
	}

	tmpDir, cleanupFn, err := copyAndPrepareSnapshot(origDir, "tap9", newIP)
	if err != nil {
		t.Fatalf("copyAndPrepareSnapshot: %v", err)
	}
	defer cleanupFn()

	// Verify original is unchanged
	origData, err := os.ReadFile(filepath.Join(origDir, "config.json"))
	if err != nil {
		t.Fatalf("read original: %v", err)
	}
	var origCfg VMConfig
	if err := json.Unmarshal(origData, &origCfg); err != nil {
		t.Fatalf("unmarshal original: %v", err)
	}
	if (*origCfg.Net)[0].Tap != "tap0" {
		t.Errorf("original tap changed to %q, want %q", (*origCfg.Net)[0].Tap, "tap0")
	}

	// Verify copy has new values
	copyData, err := os.ReadFile(filepath.Join(tmpDir, "config.json"))
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	var copyCfg VMConfig
	if err := json.Unmarshal(copyData, &copyCfg); err != nil {
		t.Fatalf("unmarshal copy: %v", err)
	}
	if (*copyCfg.Net)[0].Tap != "tap9" {
		t.Errorf("copy tap = %q, want %q", (*copyCfg.Net)[0].Tap, "tap9")
	}
	gotIP, err := extractGuestIPFromCmdline(*copyCfg.Payload.Cmdline)
	if err != nil {
		t.Fatalf("extractGuestIP from copy: %v", err)
	}
	if !gotIP.IP.Equal(newIP.IP) {
		t.Errorf("copy guest_ip = %v, want %v", gotIP.IP, newIP.IP)
	}

	// Verify stateful disk was copied
	diskData, err := os.ReadFile(filepath.Join(tmpDir, "stateful.img"))
	if err != nil {
		t.Fatalf("read copy stateful: %v", err)
	}
	if string(diskData) != "disk-data" {
		t.Errorf("stateful disk content = %q, want %q", string(diskData), "disk-data")
	}
}

// T6: CreateTapDevice(nil) returns unique IDs each time.
// NOTE: Skipped on macOS — requires Linux ip/tuntap commands.
func TestCreateTapDeviceAutoAllocate(t *testing.T) {
	if os.Getenv("ARRAKIS_TEST_TAP") == "" {
		t.Skip("skipping: requires Linux tap device support (set ARRAKIS_TEST_TAP=1 to run)")
	}
	f := fountain.NewFountain("br0")
	seen := make(map[int32]bool)
	for i := 0; i < 3; i++ {
		dev, err := f.CreateTapDevice(nil)
		if err != nil {
			t.Fatalf("CreateTapDevice(%d): %v", i, err)
		}
		if seen[dev.ID] {
			t.Errorf("duplicate ID %d on iteration %d", dev.ID, i)
		}
		seen[dev.ID] = true
	}
}

// T1: CreateTapDevice with a taken ID returns error.
// NOTE: Skipped on macOS — requires Linux ip/tuntap commands.
func TestCreateTapDeviceClaimTaken(t *testing.T) {
	if os.Getenv("ARRAKIS_TEST_TAP") == "" {
		t.Skip("skipping: requires Linux tap device support (set ARRAKIS_TEST_TAP=1 to run)")
	}
	f := fountain.NewFountain("br0")
	// Allocate tap0
	dev, err := f.CreateTapDevice(nil)
	if err != nil {
		t.Fatalf("first CreateTapDevice: %v", err)
	}
	// Try to claim the same ID
	takenID := dev.ID
	_, err = f.CreateTapDevice(&takenID)
	if err == nil {
		t.Error("expected error when claiming taken tap ID, got nil")
	}
}

// Integration tests that require CHV/QEMU are not included here.
// TestRestoreTwice, TestConcurrentRestore, TestCHVRestore would need
// a running cloud-hypervisor binary and Linux environment.
