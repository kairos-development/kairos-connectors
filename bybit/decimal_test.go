package bybit

import (
	"strings"
	"testing"
)

func TestParseRequiredDecimal_RejectsMalformedValue(t *testing.T) {
	_, err := parseRequiredDecimal("qty", "not-a-number")
	if err == nil {
		t.Fatal("expected malformed decimal error")
	}
	if !strings.Contains(err.Error(), "qty") {
		t.Fatalf("expected field name in error, got %v", err)
	}
}

func TestParseOptionalDecimal_AllowsEmptyValue(t *testing.T) {
	value, err := parseOptionalDecimal("avgPrice", "")
	if err != nil {
		t.Fatalf("expected empty optional decimal to be accepted, got %v", err)
	}
	if !value.IsZero() {
		t.Fatalf("expected zero value, got %s", value)
	}
}
