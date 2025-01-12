package dbutils

type EventType string

const (
	EventTypeTransfer      EventType = "transfer"
	EventTypeMint          EventType = "mint"
	EventTypeBurn          EventType = "burn"
	EventTypeCloseAccount  EventType = "close_account"
	EventTypeMintCNft      EventType = "mint_cnft"
	EventTypeSwap          EventType = "swap"
	EventTypeUnstakeLiquid EventType = "unstake_liquid"
)
