package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

type ContentOverridesMap map[string]map[string]interface{}

type OverridesData struct {
	ContentOverrides map[string]ContentOverridesMap `json:"content_overrides"`
}

type OverrideManager struct {
	data     OverridesData
	filePath string
	mu       sync.RWMutex
	loaded   bool
}

func NewOverrideManager(filePath string) *OverrideManager {
	return &OverrideManager{
		filePath: filePath,
		loaded:   false,
	}
}

func (m *OverrideManager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if file exists, if not, create an empty one
	if _, err := os.Stat(m.filePath); os.IsNotExist(err) {
		log.Printf("Creating empty overrides file at %s", m.filePath)
		emptyData := OverridesData{
			ContentOverrides: map[string]ContentOverridesMap{
				"nft_collections": {},
				"nft_items":       {},
				"jetton_masters":  {},
			},
		}
		bytes, err := json.MarshalIndent(emptyData, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal empty overrides: %v", err)
		}
		if err := os.WriteFile(m.filePath, bytes, 0644); err != nil {
			return fmt.Errorf("failed to create empty overrides file: %v", err)
		}
		m.data = emptyData
		m.loaded = true
		return nil
	}

	// Read and parse the file
	bytes, err := os.ReadFile(m.filePath)
	if err != nil {
		return fmt.Errorf("failed to read overrides file: %v", err)
	}

	var data OverridesData
	if err := json.Unmarshal(bytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal overrides: %v", err)
	}

	// Ensure all required type maps exist
	if data.ContentOverrides == nil {
		data.ContentOverrides = make(map[string]ContentOverridesMap)
	}
	for _, addrType := range []string{"nft_collections", "nft_items", "jetton_masters"} {
		if _, ok := data.ContentOverrides[addrType]; !ok {
			data.ContentOverrides[addrType] = make(ContentOverridesMap)
		}
	}

	m.data = data
	m.loaded = true
	log.Printf("Loaded %d content overrides from %s", m.countOverrides(), m.filePath)
	return nil
}

func (m *OverrideManager) countOverrides() int {
	count := 0
	for _, typeMap := range m.data.ContentOverrides {
		count += len(typeMap)
	}
	return count
}

func (m *OverrideManager) GetContentOverride(addrType, address string) (map[string]interface{}, bool) {
	if !m.loaded {
		return nil, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	typeMap, ok := m.data.ContentOverrides[addrType]
	if !ok {
		return nil, false
	}

	override, ok := typeMap[address]
	if !ok {
		return nil, false
	}

	return override, true
}
