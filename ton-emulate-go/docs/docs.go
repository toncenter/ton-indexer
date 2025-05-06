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
        "/v1/emulateTrace": {
            "post": {
                "description": "Emulate trace by external message.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "emulate"
                ],
                "summary": "Emulate trace by external message",
                "parameters": [
                    {
                        "description": "External Message Request",
                        "name": "request",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/main.EmulateRequest"
                        }
                    },
                    {
                        "type": "string",
                        "description": "Supported actions version",
                        "name": "X-Actions-Version",
                        "in": "header"
                    }
                ],
                "responses": {}
            }
        }
    },
    "definitions": {
        "main.EmulateRequest": {
            "type": "object",
            "properties": {
                "boc": {
                    "type": "string",
                    "example": "te6ccgEBAQEAAgAAAA=="
                },
                "ignore_chksig": {
                    "type": "boolean",
                    "example": false
                },
                "include_code_data": {
                    "type": "boolean",
                    "example": false
                },
                "include_address_book": {
                    "type": "boolean",
                    "example": false
                },
                "include_metadata": {
                    "type": "boolean",
                    "example": false
                },
                "with_actions": {
                    "type": "boolean",
                    "example": false
                },
                "mc_block_seqno": {
                    "type": "integer",
                    "example": null,
                    "nullable": true
                }
            }
        }
    }
}`

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = &swag.Spec{
	Version:          "0.0.1",
	Host:             "",
	BasePath:         "/api/emulate/",
	Schemes:          []string{},
	Title:            "TON Emulate API",
	Description:      "TON Emulate API provides an endpoint to emulate transactions and traces before committing them to the blockchain.",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	LeftDelim:        "{{",
	RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}
