package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

const defaultOverridesContent = `
{
  "content_overrides": {
    "jetton_masters": {
      "0:65AAC9B5E380EAE928DB3C8E238D9BC0D61A9320FDC2BC7A2F6C87D6FEDF9208": {
        "address": "0:65AAC9B5E380EAE928DB3C8E238D9BC0D61A9320FDC2BC7A2F6C87D6FEDF9208",
        "name": "DeDust",
        "description": "DUST is the governance token for the DeDust Protocol.",
        "social": [
          "https://t.me/dedust",
          "https://x.com/dedust_io"
        ],
        "websites": [
          "https://dedust.io"
        ],
        "symbol": "DUST",
        "image": "https://assets.dedust.io/images/dust.gif"
      }
    },
    "nft_collections": {},
    "nft_items": {}
  }
}`

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

	content := []byte(defaultOverridesContent)
	// If file exists, read it
	if _, err := os.Stat(m.filePath); err == nil {
		bytes, err := os.ReadFile(m.filePath)
		if err != nil {
			log.Printf("failed to read overrides file: %v. Using default content.", err)
		} else {
			content = bytes
		}
	}

	var data OverridesData
	if err := json.Unmarshal(content, &data); err != nil {
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
	log.Printf("Loaded %d content overrides", m.countOverrides())
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
