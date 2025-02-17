package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

type ImgProxyUrlBuilder struct {
	key  []byte
	salt []byte
}

func NewImgProxyUrlBuilder(key, salt []byte) *ImgProxyUrlBuilder {
	return &ImgProxyUrlBuilder{key: key, salt: salt}
}

func (b *ImgProxyUrlBuilder) BuildUrl(src string, preset string) string {
	encoded_url := base64.RawURLEncoding.EncodeToString([]byte(src))
	path := fmt.Sprintf("/pr:%s/%s", preset, encoded_url)
	mac := hmac.New(sha256.New, b.key)
	mac.Write(b.salt)
	mac.Write([]byte(path))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("/%s%s", signature, path)
}
