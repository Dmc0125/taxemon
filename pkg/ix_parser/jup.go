package ixparser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"taxemon/pkg/assert"
)

const jupiterV6ProgramAddress = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

var (
	ixJupV6SharedAccountsRoute, _ = hex.DecodeString("c1209b3341d69c81")
	ixJupV6Log, _                 = hex.DecodeString("e445a52e51cb9a1d")

	evJupV6Swap, _ = hex.DecodeString("40c6cde8260871e2")
)

func parseJupIxIntoSwapEvent(programAddress string, innerIxs []ParsableIxBase) *EventSwap {
	amounts := make(map[string]uint64)

	for _, iix := range innerIxs {
		fmt.Printf("pa %s\n", iix.ProgramAddress())

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

		amount := binary.LittleEndian.Uint64(data[1:])

		if _, ok := amounts[from]; !ok {
			amounts[from] = -amount
		} else {
			amounts[from] -= amount
		}

		if _, ok := amounts[to]; !ok {
			amounts[to] = +amount
		} else {
			amounts[to] += amount
		}
	}

	tokensIn := make([]*SwapToken, 0)
	tokensOut := make([]*SwapToken, 0)

	for account, amount := range amounts {
		if amount > 0 {
			tokensIn = append(tokensIn, &SwapToken{
				Amount:  amount,
				Account: account,
			})
		} else if amount < 0 {
			tokensOut = append(tokensOut, &SwapToken{
				Amount:  -amount,
				Account: account,
			})
		}
	}

	assert.True(
		len(tokensIn) > 0 && len(tokensOut) > 0,
		"empty tokensOut or tokensIn",
		"tokensIn",
		tokensIn,
		"tokensOut",
		tokensOut,
	)
	return &EventSwap{
		ProgramAddress: programAddress,
		From:           tokensOut,
		To:             tokensIn,
	}
}

func (parser *EventsParser) parseJupV6Ix(ix ParsableIx, signature string) error {
	// data := ix.Data()
	// if len(data) < 8 {
	// 	return errDataTooSmall
	// }
	// disc := data[0:8]

	// if slices.Equal(disc, ixJupV6SharedAccountsRoute) {
	// 	accounts := ix.AccountsAddresses()
	// 	authority := accounts[2]

	// 	if authority != walletAddress {
	// 		return nil
	// 	}

	// 	swapEvent := parseJupIxIntoSwapEvent(jupiterV6ProgramAddress, ix.InnerIxs())
	// 	fmt.Printf("event %#v\n", swapEvent)
	// }

	return nil
}
