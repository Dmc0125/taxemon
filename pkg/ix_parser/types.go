package ixparser

import (
	"encoding/json"
	"fmt"
	"strings"
)

type AssociatedAccountToken struct {
	address string
	mint    string
}

func (account *AssociatedAccountToken) Address() string {
	return account.address
}

func (account *AssociatedAccountToken) Type() uint8 {
	return 0
}

func (account *AssociatedAccountToken) Data() ([]byte, error) {
	d := map[string]interface{}{
		"mint": account.mint,
	}
	return json.Marshal(d)
}

type EventTransfer struct {
	ProgramAddress string `json:"program_address"`
	IsRent         bool   `json:"is_rent"`
	From           string `json:"from"`
	To             string `json:"to"`
	Amount         uint64 `json:"amount"`
}

func (e *EventTransfer) Type() uint8 {
	return 0
}

type EventMint struct {
	ProgramAddress string `json:"program_address"`
	To             string `json:"to"`
	Amount         uint64 `json:"amount"`
}

func (e *EventMint) Type() uint8 {
	return 1
}

type EventBurn struct {
	ProgramAddress string `json:"program_address"`
	From           string `json:"to"`
	Amount         uint64 `json:"amount"`
}

func (e *EventBurn) Type() uint8 {
	return 2
}

type EventCloseAccount struct {
	ProgramAddress string `json:"program_address"`
	From           string `json:"from"`
	To             string `json:"to"`
}

func (e *EventCloseAccount) Type() uint8 {
	return 3
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

func (e *EventSwap) Type() uint8 {
	return 4
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
