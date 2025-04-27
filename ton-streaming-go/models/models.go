package models

import (
	"encoding/json"
	"time"
)

// BlockchainEvent represents an event that occurred on the blockchain
type BlockchainEvent struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// SubscriptionRequest represents a request to subscribe to blockchain events
type SubscriptionRequest struct {
	EventTypes []string `json:"event_types"`
	Addresses  []string `json:"addresses,omitempty"`
	Filter     string   `json:"filter,omitempty"`
}

// SubscriptionResponse represents a response to a subscription request
type SubscriptionResponse struct {
	SubscriptionID string `json:"subscription_id"`
	Status         string `json:"status"`
	Message        string `json:"message,omitempty"`
}

// UnsubscribeRequest represents a request to unsubscribe from blockchain events
type UnsubscribeRequest struct {
	SubscriptionID string `json:"subscription_id"`
}

// UnsubscribeResponse represents a response to an unsubscribe request
type UnsubscribeResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}
