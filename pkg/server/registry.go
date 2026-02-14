package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

type registryEntry struct {
	Name             string        `json:"name"`
	StateDirPath     string        `json:"state_dir"`
	StatefulDiskPath string        `json:"stateful_disk_path"`
	CID              uint32        `json:"cid"`
	IP               string        `json:"ip"`
	PortForwards     []portForward `json:"port_forwards"`
}

type registry struct {
	VMs map[string]registryEntry `json:"vms"`
}

// saveRegistry must be called while holding s.lock.
func (s *Server) saveRegistry() error {
	reg := registry{VMs: make(map[string]registryEntry)}
	for name, vm := range s.vms {
		var ipStr string
		if vm.ip != nil {
			ipStr = vm.ip.String()
		}
		reg.VMs[name] = registryEntry{
			Name:             vm.name,
			StateDirPath:     vm.stateDirPath,
			StatefulDiskPath: vm.statefulDiskPath,
			CID:              vm.cid,
			IP:               ipStr,
			PortForwards:     vm.portForwards,
		}
	}
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal registry: %w", err)
	}
	registryPath := filepath.Join(s.config.StateDir, "registry.json")
	tmpPath := registryPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp registry: %w", err)
	}
	if err := os.Rename(tmpPath, registryPath); err != nil {
		return fmt.Errorf("failed to rename registry: %w", err)
	}
	return nil
}

func (s *Server) loadRegistry() (*registry, error) {
	registryPath := filepath.Join(s.config.StateDir, "registry.json")
	data, err := os.ReadFile(registryPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &registry{VMs: make(map[string]registryEntry)}, nil
		}
		return nil, fmt.Errorf("failed to read registry: %w", err)
	}
	var reg registry
	if err := json.Unmarshal(data, &reg); err != nil {
		log.WithError(err).Warn("corrupt registry.json, starting fresh")
		return &registry{VMs: make(map[string]registryEntry)}, nil
	}
	if reg.VMs == nil {
		reg.VMs = make(map[string]registryEntry)
	}
	return &reg, nil
}
