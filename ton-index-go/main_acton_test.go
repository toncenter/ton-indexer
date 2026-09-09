package main

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/toncenter/ton-indexer/ton-index-go/index/actonapi"
)

func TestActonFiberBodyLimitStatus(t *testing.T) {
	// Invoking Fiber's body-limit error directly avoids app.Test's connection
	// reset on an oversized upload and exercises the production error handler.
	app := fiber.New(fiber.Config{ErrorHandler: ErrorHandlerFunc})
	app.Post("/fiber-limit", func(*fiber.Ctx) error { return fiber.ErrRequestEntityTooLarge })
	api := actonapi.New(nil, "test", actonapi.Dependencies{})
	app.Post("/api-limit", func(c *fiber.Ctx) error {
		err := api.PostAccounts(c)
		if e, ok := err.(*actonapi.Error); ok {
			return fiber.NewError(e.Code, e.Message)
		}
		return err
	})
	for _, path := range []string{"/fiber-limit", "/api-limit"} {
		req := httptest.NewRequest("POST", path, strings.NewReader(strings.Repeat(" ", actonapi.MaxBodyBytes+1)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 413 {
			t.Fatalf("%s: status=%d body=%s", path, resp.StatusCode, body)
		}
	}
}
