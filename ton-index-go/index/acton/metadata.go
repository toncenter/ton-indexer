// Package acton exposes protocol-neutral metadata from the shared ABI catalog.
package acton

import (
	"sort"

	tolkabi "github.com/ton-blockchain/tolk-abi-to-go"
)

type Parameter struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Method struct {
	Name        string      `json:"name"`
	ID          int64       `json:"id"`
	Parameters  []Parameter `json:"parameters"`
	Return      string      `json:"return"`
	Description string      `json:"description,omitempty"`
	Unsupported string      `json:"unsupported,omitempty"`
}

type Metadata struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	Links   []tolkabi.Link `json:"links"`
	Methods []Method       `json:"methods"`
}

// Describe projects the native registry into compact, JSON-safe metadata.
// A declared method is not proof that it executed successfully on an account.
func Describe(c *tolkabi.Contract) Metadata {
	out := Metadata{ID: c.ID, Name: c.DisplayName, Links: append([]tolkabi.Link{}, c.Links...), Methods: []Method{}}
	for _, m := range c.GetMethods {
		method := Method{Name: m.Name, ID: m.ID, Return: m.Return.Name, Description: m.Description, Unsupported: m.Unsupported, Parameters: []Parameter{}}
		for _, p := range m.Parameters {
			method.Parameters = append(method.Parameters, Parameter{p.Name, p.Type.Name})
		}
		out.Methods = append(out.Methods, method)
	}
	sort.Slice(out.Methods, func(i, j int) bool { return out.Methods[i].ID < out.Methods[j].ID })
	return out
}
