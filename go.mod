module github.com/kairos-development/kairos-connectors

go 1.26

require (
	github.com/gorilla/websocket v1.5.3
	github.com/kairos-development/kairos-contracts v0.0.0-00010101000000-000000000000
	github.com/shopspring/decimal v1.4.0
	github.com/sirupsen/logrus v1.9.4
)

require golang.org/x/sys v0.13.0 // indirect

replace github.com/kairos-development/kairos-contracts => ../kairos-contracts

replace github.com/kairos-development/kairos-agent => ../kairos-agent
