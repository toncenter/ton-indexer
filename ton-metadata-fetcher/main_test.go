package main

import (
	"context"
	neturl "net/url"
	"strings"
	"testing"
)

func TestParseProxyURLAcceptsSupportedSchemes(t *testing.T) {
	rawURLs := []string{
		"http://proxy.example:3128",
		"https://proxy.example:8443",
		"socks5://user:pass@127.0.0.1:1080",
		"socks5h://proxy.example",
	}

	for _, rawURL := range rawURLs {
		if _, err := parseProxyURL(rawURL); err != nil {
			t.Fatalf("parseProxyURL(%q) returned error: %v", rawURL, err)
		}
	}
}

func TestParseProxyURLRejectsInvalidValues(t *testing.T) {
	rawURLs := []string{
		"",
		"ftp://proxy.example:21",
		"http://",
		"http://proxy.example/path",
		"http://proxy.example?token=secret",
	}

	for _, rawURL := range rawURLs {
		if _, err := parseProxyURL(rawURL); err == nil {
			t.Fatalf("parseProxyURL(%q) returned nil error", rawURL)
		}
	}
}

func TestIsProxyDialAddressUsesDefaultPort(t *testing.T) {
	proxyURL, err := parseProxyURL("socks5://127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}

	if !isProxyDialAddress("127.0.0.1:1080", proxyURL) {
		t.Fatal("expected default SOCKS5 port to match proxy dial address")
	}
	if isProxyDialAddress("127.0.0.1:80", proxyURL) {
		t.Fatal("did not expect non-default SOCKS5 port to match proxy dial address")
	}
}

func TestValidateHTTPMetadataURLBlocksPrivateIP(t *testing.T) {
	oldBlockedNetworks := blockedNetworks
	t.Cleanup(func() {
		blockedNetworks = oldBlockedNetworks
	})

	var err error
	blockedNetworks, err = loadBlockedNetworks("")
	if err != nil {
		t.Fatal(err)
	}

	metadataURL, err := neturl.Parse("http://127.0.0.1/metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	err = validateHTTPMetadataURL(context.Background(), metadataURL)
	if err == nil || !strings.Contains(err.Error(), "blocked request to private IP") {
		t.Fatalf("expected private IP block error, got %v", err)
	}
}
