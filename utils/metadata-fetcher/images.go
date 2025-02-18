package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
)

type ImgProxyUrlBuilder struct {
	key                   []byte
	salt                  []byte
	ipfs_resolve_base_url string
}

func NewImgProxyUrlBuilder(key, salt []byte, ipfs_resolve_base_url string) *ImgProxyUrlBuilder {
	return &ImgProxyUrlBuilder{key: key, salt: salt, ipfs_resolve_base_url: ipfs_resolve_base_url}
}

func (b *ImgProxyUrlBuilder) BuildUrl(src string, preset string) string {
	var path string
	if isIpfs(src) {
		src = strings.TrimPrefix(src, "ipfs://")
		src = fmt.Sprintf("%s/%s", b.ipfs_resolve_base_url, src)
	}
	encoded_url := base64.RawURLEncoding.EncodeToString([]byte(src))
	path = fmt.Sprintf("/pr:%s/%s", preset, encoded_url)
	mac := hmac.New(sha256.New, b.key)
	mac.Write(b.salt)
	mac.Write([]byte(path))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("/%s%s", signature, path)
}

func isIpfs(path string) bool {
	return strings.HasPrefix(path, "ipfs://")
}
