package ixparser

import (
	"errors"
	"iter"
	"slices"
	dbutils "taxemon/pkg/db_utils"
)

var (
	errDataTooSmall     = errors.New("data too small")
	errAccountsTooSmall = errors.New("accounts too small")
)

type EventData interface {
	Type() uint8
}

type ParsedEvent struct {
	IxIdx int32
	Idx   int16
	Data  EventData
}

type ParsableIxBase interface {
	ProgramAddress() string
	AccountsAddresses() []string
	Data() []byte
}

type ParsableIx interface {
	ParsableIxBase
	InnerIxs() []ParsableIxBase
}

type ParsableTx interface {
	Signature() string
	GetInstructions() iter.Seq2[int, ParsableIx]
	Logs() iter.Seq2[int, string]
}

type AssociatedAccount interface {
	Address() string
	Type() dbutils.AssociatedAccountType
	Data() ([]byte, error)
	ShouldFetch() bool
}

type AssociatedAccounts interface {
	Append(AssociatedAccount)
	Contains(string) bool
	Get(string) AssociatedAccount
}

const (
	computeBudgetProgramAddress = "ComputeBudget111111111111111111111111111111"
)

func ParseAssociatedAccounts(associatedAccounts AssociatedAccounts, tx ParsableTx, walletAddress string) error {
	for _, ix := range tx.GetInstructions() {
		var err error
		switch ix.ProgramAddress() {
		case tokenProgramAddress:
			err = parseTokenIxAssociatedAccounts(associatedAccounts, ix, walletAddress)
		case associatedTokenProgramAddress:
			err = parseAssociatedTokenIxAssociatedAccounts(associatedAccounts, ix, walletAddress)
		case jupLimitProgramAddress:
			err = parseJupLimitIxAssociatedAccounts(associatedAccounts, ix, walletAddress)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

type EventsParser struct {
	walletAddress      string
	associatedAccounts AssociatedAccounts
}

func NewEventsParser(walletAddress string, associatedAccounts AssociatedAccounts) *EventsParser {
	return &EventsParser{
		walletAddress,
		associatedAccounts,
	}
}

func (parser *EventsParser) isRelated(accounts ...string) bool {
	return slices.Contains(accounts, parser.walletAddress)
}

type parsedEvents []*ParsedEvent

func (evts *parsedEvents) append(ixIdx int, events ...EventData) {
	for i, e := range events {
		*evts = append(*evts, &ParsedEvent{
			IxIdx: int32(ixIdx),
			Idx:   int16(i),
			Data:  e,
		})
	}
}

func (parser *EventsParser) ParseTx(tx ParsableTx) ([]*ParsedEvent, error) {
	events := parsedEvents([]*ParsedEvent{})

	for i, ix := range tx.GetInstructions() {
		var err error
		// dont use index from loop but from ix

		switch ix.ProgramAddress() {
		case computeBudgetProgramAddress:
			//
		case systemProgramAddress:
			var event EventData
			if event, err = parser.parseSystemIxEvents(ix); event != nil {
				events.append(i, event)
			}
		case tokenProgramAddress:
			var event EventData
			if event, err = parser.parseTokenIxEvents(ix); event != nil {
				events.append(i, event)
			}
		case associatedTokenProgramAddress:
			var event EventData
			if event, err = parser.parseAssociatedTokenIxEvents(ix, tx.Signature()); event != nil {
				events.append(i, event)
			}
		case jupiterV6ProgramAddress:
			var event EventData
			if event, err = parser.parseJupV6Ix(ix, tx.Signature()); event != nil {
				events.append(i, event)
			}
		case jupLimitProgramAddress:
			var currentEvents []EventData
			if currentEvents, err = parser.parseJupLimitIx(ix, tx.Signature()); currentEvents != nil {
				events.append(i, currentEvents...)
			}
		}

		if err != nil {
			return nil, err
		}
	}

	return events, nil
}
