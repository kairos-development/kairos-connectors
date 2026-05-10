package bybit

import (
	"fmt"

	"github.com/shopspring/decimal"
)

func parseRequiredDecimal(field string, raw string) (decimal.Decimal, error) {
	if raw == "" {
		return decimal.Zero, fmt.Errorf("missing decimal field %s", field)
	}

	value, err := decimal.NewFromString(raw)
	if err != nil {
		return decimal.Zero, fmt.Errorf("parse decimal field %s=%q: %w", field, raw, err)
	}
	return value, nil
}

func parseOptionalDecimal(field string, raw string) (decimal.Decimal, error) {
	if raw == "" {
		return decimal.Zero, nil
	}
	return parseRequiredDecimal(field, raw)
}
