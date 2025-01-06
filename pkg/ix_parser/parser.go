package ixparser

import (
	"errors"
	"iter"
	"slices"
)

var (
	errDataTooSmall     = errors.New("data too small")
	errAccountsTooSmall = errors.New("accounts too small")
)

type Event interface {
	Type() uint8
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
	Signature() string
	Instructions() iter.Seq2[int, ParsableIx]
	Logs() iter.Seq2[int, string]
}

type AssociatedAccount interface {
	Address() string
	Type() uint8
	Data() ([]byte, error)
}

type AssociatedAccounts interface {
	Append(AssociatedAccount)
}

const (
	computeBudgetProgramAddress = "ComputeBudget111111111111111111111111111111"
)

func ParseAssociatedAccounts(associatedAccounts AssociatedAccounts, tx ParsableTx, walletAddress string) error {
	for _, ix := range tx.Instructions() {
		var err error
		switch ix.ProgramAddress() {
		case tokenProgramAddress:
			err = parseTokenIxAssociatedAccounts(associatedAccounts, ix)
		case associatedTokenProgramAddress:
			err = parseAssociatedTokenIxAssociatedAccounts(associatedAccounts, ix, walletAddress)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

type EventsParser struct {
	WalletAddress string
}

func (parser *EventsParser) isRelated(accounts ...string) bool {
	return slices.Contains(accounts, parser.WalletAddress)
}

func (parser *EventsParser) ParseTx(tx ParsableTx) error {
	for _, ix := range tx.Instructions() {
		var err error

		switch ix.ProgramAddress() {
		case computeBudgetProgramAddress:
			//
		case systemProgramAddress:
			err = parser.parseSystemIxEvents(ix)
		case tokenProgramAddress:
			err = parser.parseTokenIxEvents(ix)
		case associatedTokenProgramAddress:
			err = parser.parseAssociatedTokenIxEvents(ix, tx.Signature())
		case jupiterV6ProgramAddress:
			err = parser.parseJupV6Ix(ix, tx.Signature())
		}

		if err != nil {
			return err
		}
	}

	return nil
}
