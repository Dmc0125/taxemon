package ixparser

import (
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"slices"
)

const jupiterV6ProgramAddress = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

var (
	ixJupV6Route, _               = hex.DecodeString("e517cb977ae3ad2a")
	ixJupV6SharedAccountsRoute, _ = hex.DecodeString("c1209b3341d69c81")
	ixJupV6Log, _                 = hex.DecodeString("e445a52e51cb9a1d")

	evJupV6Swap, _ = hex.DecodeString("40c6cde8260871e2")
)

func parseJupIxIntoSwapEvent(parser *EventsParser, innerIxs []ParsableIxBase, signature string) *EventSwap {
	amounts := make(map[string]int64)

	for _, iix := range innerIxs {
		if iix.ProgramAddress() != tokenProgramAddress {
			continue
		}

		accounts := iix.AccountsAddresses()
		data := iix.Data()
		var (
			from, to string
		)

		switch data[0] {
		case ixTokenTransfer:
			from = accounts[0]
			to = accounts[1]
		case ixTokenTransferChecked:
			from = accounts[0]
			to = accounts[2]
		default:
			continue
		}

		var amount int64
		var key string

		if parser.associatedAccounts.Contains(from) {
			amount = -int64(binary.LittleEndian.Uint64(data[1:]))
			key = from
		} else if parser.associatedAccounts.Contains(to) {
			amount = int64(binary.LittleEndian.Uint64(data[1:]))
			key = to
		} else {
			continue
		}

		if _, ok := amounts[key]; !ok {
			amounts[key] = amount
		} else {
			amounts[key] += amount
		}
	}

	tokensIn := make([]*SwapToken, 0)
	tokensOut := make([]*SwapToken, 0)

	for account, amount := range amounts {
		if amount > 0 {
			tokensIn = append(tokensIn, &SwapToken{
				Amount:  uint64(amount),
				Account: account,
			})
		} else if amount < 0 {
			tokensOut = append(tokensOut, &SwapToken{
				Amount:  uint64(-amount),
				Account: account,
			})
		}
	}

	if (len(tokensIn) != 0 && len(tokensOut) == 0) || (len(tokensIn) == 0 && len(tokensOut) != 0) {
		slog.Warn("jupiter v6 swap with another wallet", "signature", signature)
	}
	if len(tokensIn) == 0 || len(tokensOut) == 0 {
		return nil
	}

	return &EventSwap{
		From: tokensOut,
		To:   tokensIn,
	}
}

func (parser *EventsParser) parseJupV6Ix(ix ParsableIx, signature string) (EventData, error) {
	data := ix.Data()
	if len(data) < 8 {
		return nil, errDataTooSmall
	}
	disc := data[0:8]

	if slices.Equal(disc, ixJupV6SharedAccountsRoute) {
		accounts := ix.AccountsAddresses()
		authority := accounts[2]

		if authority != parser.walletAddress {
			return nil, nil
		}

		if swapEvent := parseJupIxIntoSwapEvent(parser, ix.InnerIxs(), signature); swapEvent != nil {
			swapEvent.ProgramAddress = jupiterV6ProgramAddress
			return swapEvent, nil
		}
	} else if slices.Equal(disc, ixJupV6Route) {
		slog.Warn("unhandled ix: ixJupV6Route", "signature", signature)
	} else {
		slog.Warn("unhandled ix: unknown", "signature", signature)
	}

	return nil, nil
}
