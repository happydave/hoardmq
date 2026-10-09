// Package schema publishes the JSON Schema for the HoardMQ configuration
// document. The file is the contract; the loader validates against it rather
// than against a parallel set of rules.
package schema

import _ "embed"

// Bytes is the JSON Schema (draft 2020-12) for configuration document version 1.
//
//go:embed hoardmq.schema.json
var Bytes []byte
