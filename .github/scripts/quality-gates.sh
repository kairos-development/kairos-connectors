#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

fail=0

report() {
  echo "::error title=$1::$2"
  fail=1
}

ignored_decimal_errors=$(rg -n '(\w+|_)\s*,\s*_\s*:=\s*decimal\.NewFromString|_\s*,\s*(\w+|_)\s*:=\s*decimal\.NewFromString|_\s*=\s*decimal\.NewFromString' . --glob '*.go' --glob '!*_test.go' || true)
if [[ -n "$ignored_decimal_errors" ]]; then
  report "ignored decimal parse error" "decimal.NewFromString errors must be handled explicitly in connector parsing paths"
  echo "$ignored_decimal_errors"
fi

agent_imports=$(rg -n 'github\.com/kairos-development/kairos-agent|kairos-agent/internal' . --glob '*.go' --glob '!*_test.go' || true)
if [[ -n "$agent_imports" ]]; then
  report "forbidden agent dependency" "connectors must not import kairos-agent or agent internals"
  echo "$agent_imports"
fi

float_usage=$(rg -n '\bfloat(32|64)\b' bybit pkg --glob '*.go' --glob '!*_test.go' || true)
if [[ -n "$float_usage" ]]; then
	report "float in connector trading path" "connector price/qty/balance fields must use decimal-compatible parsing, not float32/float64"
	echo "$float_usage"
fi

decimal_float_usage=$(rg -n 'decimal\.NewFromFloat' bybit pkg --glob '*.go' --glob '!*_test.go' || true)
if [[ -n "$decimal_float_usage" ]]; then
	report "decimal from float" "connector trading constants must be exact decimals, not decimal.NewFromFloat"
	echo "$decimal_float_usage"
fi

runtime_panics=$(rg -n '\bpanic\s*\(' bybit pkg --glob '*.go' --glob '!*_test.go' || true)
if [[ -n "$runtime_panics" ]]; then
  report "production panic" "connector runtime paths must return errors or stream events instead of panicking"
  echo "$runtime_panics"
fi

fatal_logs=$(rg -n '\blog\.(Fatal|Fatalf|Panic|Panicf)\b|\.Fatal\(|\.Panic\(' . --glob '*.go' --glob '!*_test.go' || true)
if [[ -n "$fatal_logs" ]]; then
  report "fatal logging" "library connector code must not terminate the process"
  echo "$fatal_logs"
fi

secret_logs=$(rg -n '(apiSecret|secret|token|signature|password|privateKey)' . --glob '*.go' --glob '!*_test.go' | rg -n '(WithField|WithFields|Println|Printf|Warnf|Infof|Debugf|Tracef|log\.)' || true)
if [[ -n "$secret_logs" ]]; then
  report "possible secret logging" "review logging around secret-like fields before merging"
  echo "$secret_logs"
fi

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi

echo "Connector quality gates passed"
