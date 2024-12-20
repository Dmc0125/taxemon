package ixparser

import (
	"iter"
)

type Event interface {
	Type() uint8
	Data() []byte
}

type ParsableIxBase interface {
	ProgramAddress() string
	AccountsAddresses() []string
	Data() []byte
}

type ParsableIx interface {
	ParsableIxBase
	InnerIxs() []ParsableIxBase
	AddEvent(Event)
}

type ParsableTx interface {
	Instructions() iter.Seq2[int, ParsableIx]
	Logs() iter.Seq2[int, string]
}

type AssociatedAccount struct {
	Address string
	Type    int
}

func ParseTx(tx ParsableTx) []*AssociatedAccount {
	associatedAccounts := make([]*AssociatedAccount, 0)

	// for _, ix := range tx.Instructions() {

	// }

	return associatedAccounts
}
