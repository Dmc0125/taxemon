package ixparser

import (
	"fmt"
	"strings"
	dbutils "taxemon/pkg/db_utils"
)

type EventTransfer struct {
	ProgramAddress string `json:"program_address"`
	IsRent         bool   `json:"is_rent"`
	From           string `json:"from"`
	To             string `json:"to"`
	Amount         uint64 `json:"amount"`
}

func (e *EventTransfer) Type() dbutils.EventType {
	return dbutils.EventTypeTransfer
}

type EventMint struct {
	ProgramAddress string `json:"program_address"`
	To             string `json:"to"`
	Amount         uint64 `json:"amount"`
}

func (e *EventMint) Type() dbutils.EventType {
	return dbutils.EventTypeMint
}

type EventBurn struct {
	ProgramAddress string `json:"program_address"`
	From           string `json:"to"`
	Amount         uint64 `json:"amount"`
}

func (e *EventBurn) Type() dbutils.EventType {
	return dbutils.EventTypeBurn
}

type EventCloseAccount struct {
	ProgramAddress string `json:"program_address"`
	From           string `json:"from"`
	To             string `json:"to"`
}

func (e *EventCloseAccount) Type() dbutils.EventType {
	return dbutils.EventTypeCloseAccount
}

type SwapToken struct {
	Amount  uint64 `json:"amount"`
	Account string `json:"account"`
}

type EventSwap struct {
	ProgramAddress string `json:"program_address"`
	// tokens sent away
	From []*SwapToken `json:"from"`
	// tokens received
	To []*SwapToken `json:"to"`
}

func (e *EventSwap) Type() dbutils.EventType {
	return dbutils.EventTypeSwap
}

func (e *EventSwap) String() string {
	b := strings.Builder{}
	b.WriteString("FROM:")
	for _, from := range e.From {
		b.WriteString(fmt.Sprintf("\n\t%s: %d", from.Account, from.Amount))
	}
	b.WriteString("\nTO:")
	for _, to := range e.To {
		b.WriteString(fmt.Sprintf("\n\t%s: %d", to.Account, to.Amount))
	}
	return b.String()
}
