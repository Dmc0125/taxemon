package ixparser

import (
	"errors"
	"fmt"
	"iter"
	"maps"
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

func ParseTx(tx ParsableTx, walletAddress string) (map[string]AssociatedAccount, error) {
	associatedAccounts := make(map[string]AssociatedAccount, 0)

	for _, ix := range tx.Instructions() {
		var err error
		var currentAssociatedAccounts map[string]AssociatedAccount

		switch ix.ProgramAddress() {
		case systomProgramAddress:
			err = parseSystemIx(ix)
		case tokenProgramAddress:
			currentAssociatedAccounts, err = parseTokenIx(ix)
		case associatedTokenProgramAddress:
			currentAssociatedAccounts, err = parseAssociatedTokenIx(ix, walletAddress, tx.Signature())
		}

		fmt.Printf("aas ix %#v\n", currentAssociatedAccounts)

		if err != nil {
			return nil, err
		}
		if currentAssociatedAccounts != nil {
			maps.Insert(associatedAccounts, maps.All(currentAssociatedAccounts))
		}
	}

	return associatedAccounts, nil
}
