package docs

import "github.com/swaggo/swag"

const docTemplate = `{
    "schemes": {{ marshal .Schemes }},
    "swagger": "2.0",
    "info": {
        "description": "{{escape .Description}}",
        "title": "{{.Title}}",
        "contact": {},
        "version": "{{.Version}}"
    },
    "host": "{{.Host}}",
    "basePath": "{{.BasePath}}",
    "paths": {
        "/v1/sse": {
            "get": {
                "description": "Subscribe to real-time blockchain events using Server-Sent Events.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "text/event-stream"
                ],
                "tags": [
                    "streaming"
                ],
                "summary": "Subscribe to blockchain events via SSE",
                "responses": {}
            }
        },
        "/v1/ws": {
            "get": {
                "description": "Subscribe to real-time blockchain events using WebSocket.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "streaming"
                ],
                "summary": "Subscribe to blockchain events via WebSocket",
                "responses": {}
            }
        }
    }
}`

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = &swag.Spec{
	Version:          "0.0.1",
	Host:             "",
	BasePath:         "/api/streaming/",
	Schemes:          []string{},
	Title:            "TON Streaming API",
	Description:      "TON Streaming API provides real-time blockchain events via SSE, WebSocket, and WebTransport.",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	LeftDelim:        "{{",
	RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}
