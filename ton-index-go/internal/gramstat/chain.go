package gramstat

import _ "embed"

// ChainSchema stores explicitly prepared, coverage-audited chain intervals.
// Ordinary blocks or an advancing source head alone do not certify coverage.
//
//go:embed chain.sql
var ChainSchema string
