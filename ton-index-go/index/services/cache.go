package services

import (
	. "github.com/toncenter/ton-indexer/ton-index-go/index/models"

	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type AddressInfoRequest struct {
	Addresses          []string `json:"addresses"`
	IncludeAddressBook bool     `json:"include_address_book"`
	IncludeMetadata    bool     `json:"include_metadata"`
}

type AddressInfoResponse struct {
	Metadata    map[string]AddressMetadata `json:"metadata,omitempty"`
	AddressBook AddressBook                `json:"address_book,omitempty"`
}

var ErrServiceUnavailable = errors.New("cache service is unavailable")

type CacheClient struct {
	url          string
	client       *http.Client
	healthClient *http.Client
	healthy      atomic.Bool
	stopCheck    chan struct{}
}

var addressInfoCacheClient *CacheClient

func GetCacheClient() *CacheClient { return addressInfoCacheClient }

func InitCacheClient(baseURL string) {
	tr := &http.Transport{
		MaxIdleConns: 32,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   10 * time.Second,
	}
	healthClient := &http.Client{
		Timeout: 2 * time.Second,
	}

	c := &CacheClient{
		url:          baseURL,
		client:       client,
		healthClient: healthClient,
		stopCheck:    make(chan struct{}),
	}
	c.healthy.Store(true) // Assume healthy initially

	go c.runHealthCheck(5 * time.Second)

	addressInfoCacheClient = c
}

func (r *CacheClient) runHealthCheck(interval time.Duration) {
	// Initial check
	r.checkHealth()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.checkHealth()
		case <-r.stopCheck:
			return
		}
	}
}

func (r *CacheClient) checkHealth() {
	healthURL, err := url.JoinPath(r.url, "/health")
	if err != nil {
		r.healthy.Store(false)
		return
	}

	resp, err := r.healthClient.Get(healthURL)
	if err != nil {
		r.healthy.Store(false)
		return
	}
	defer resp.Body.Close()

	r.healthy.Store(resp.StatusCode == http.StatusOK)
}

func (r *CacheClient) IsHealthy() bool {
	return r.healthy.Load()
}

func (r *CacheClient) Stop() {
	close(r.stopCheck)
}

func (r *CacheClient) getAddressInfo(addresses []string, includeMetadata bool, includeAddressBook bool) (*AddressInfoResponse, error) {
	// Fail fast if service is unhealthy
	if !r.healthy.Load() {
		return nil, ErrServiceUnavailable
	}

	start := time.Now()
	payload, err := json.Marshal(AddressInfoRequest{
		Addresses:          addresses,
		IncludeAddressBook: includeAddressBook,
		IncludeMetadata:    includeMetadata,
	})
	if err != nil {
		return nil, err
	}
	result, err := url.JoinPath(r.url, "/address_info")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", result, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		// Mark unhealthy on connection errors
		r.healthy.Store(false)
		return nil, err
	}
	defer resp.Body.Close()
	var response AddressInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}
	println("Cache response: ", time.Since(start).Milliseconds())
	return &response, nil
}

func (r *CacheClient) GetMetadata(addresses []string) (Metadata, error) {
	res, err := r.getAddressInfo(addresses, true, false)
	if err != nil {
		return Metadata{}, err
	}
	return res.Metadata, err
}

func (r *CacheClient) GetAddressBook(addresses []string) (AddressBook, error) {
	res, err := r.getAddressInfo(addresses, false, true)
	if err != nil {
		return AddressBook{}, err
	}
	return res.AddressBook, err
}
