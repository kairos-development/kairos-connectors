package bybit

import (
	"github.com/kairos-development/kairos-contracts/connector"
)

// unmapOrderStatus converts Bybit order status to connector order status.
func unmapOrderStatus(status string) connector.OrderStatus {
	switch status {
	case "New":
		return connector.OrderStatusSubmitted
	case "PartiallyFilled":
		return connector.OrderStatusPartial
	case "Filled":
		return connector.OrderStatusFilled
	case "Cancelled":
		return connector.OrderStatusCanceled
	case "Rejected":
		return connector.OrderStatusRejected
	default:
		return connector.OrderStatusPending
	}
}

// unmapPositionSide converts Bybit position side to connector position side.
func unmapPositionSide(side string) connector.PositionSide {
	switch side {
	case "Buy":
		return connector.PositionSideLong
	case "Sell":
		return connector.PositionSideShort
	default:
		return connector.PositionSideFlat
	}
}
