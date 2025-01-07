package ixparser

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	dbutils "taxemon/pkg/db_utils"
)

const (
	jupLimitProgramAddress  = "jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu"
	jupLimit2ProgramAddress = "j1o2qRpjcyUwEvwtcfhEQefh773ZgjxcVRry7LDqg5X"
)

var (
	ixJupLimitInitOrder, _         = hex.DecodeString("856e4aaf709ff59f")
	ixJupLimitFlashFillOrder, _    = hex.DecodeString("fc681286a44e128c")
	ixJupLimitPreFlashFillOrder, _ = hex.DecodeString("f02f99440dbee12a")
)

type AssociatedAccountJupLimit struct {
	address    string
	amountIn   uint64
	accountIn  string
	accountOut string
}

func NewAssociatedAccountJupLimit(address, accountIn, accountOut string) *AssociatedAccountJupLimit {
	return &AssociatedAccountJupLimit{address: address}
}

func (account *AssociatedAccountJupLimit) Type() dbutils.AssociatedAccountType {
	return dbutils.AssociatedAccountJupLimit
}

func (account *AssociatedAccountJupLimit) Address() string {
	return account.address
}

func (account *AssociatedAccountJupLimit) Data() ([]byte, error) {
	data := map[string]string{
		"accountIn":  account.accountIn,
		"accountOut": account.accountOut,
	}
	return json.Marshal(data)
}

func (account *AssociatedAccountJupLimit) ShouldFetch() bool {
	return false
}

// if order account does not exist
// 		count is 4 (create oa and ta)
// if order account exsists
// 		count is 3 (create ta if it does not exist)

func parseJupLimitIxAssociatedAccounts(associatedAccounts AssociatedAccounts, ix ParsableIx, walletAddress string) error {
	data := ix.Data()
	if len(data) < 8 {
		return errDataTooSmall
	}
	disc := data[:8]

	if slices.Equal(disc, ixJupLimitInitOrder) {
		innerIxs := ix.InnerIxs()
		accounts := ix.AccountsAddresses()
		maker := accounts[1]
		if maker != walletAddress {
			return nil
		}

		if len(innerIxs) == 4 {
			orderAccount := accounts[2]
			accountIn := accounts[3]
			accountOut := accounts[6]
			associatedAccounts.Append(&AssociatedAccountJupLimit{
				address:    orderAccount,
				accountIn:  accountIn,
				accountOut: accountOut,
			})
		}

		if len(innerIxs) != 1 {
			tokenAccount := accounts[3]
			mint := accounts[5]
			associatedAccounts.Append(&AssociatedAccountToken{
				address:     tokenAccount,
				mint:        mint,
				shouldFetch: false,
			})
		}
	}

	return nil
}

func (parser *EventsParser) parseJupLimitIx(ix ParsableIx, signature string) ([]EventData, error) {
	data := ix.Data()
	if len(data) < 8 {
		return nil, errDataTooSmall
	}

	disc := data[:8]
	if slices.Equal(disc, ixJupLimitInitOrder) {
		accounts := ix.AccountsAddresses()
		maker := accounts[1]
		if maker != parser.walletAddress {
			return nil, nil
		}

		events := make([]EventData, 0)
		innerIxs := ix.InnerIxs()

		if len(innerIxs) == 4 {
			iix := innerIxs[0]
			data := iix.Data()
			amount := binary.LittleEndian.Uint64(data[1:])

			accounts := iix.AccountsAddresses()
			from := accounts[0]
			to := accounts[1]

			events = append(events, &EventTransfer{
				ProgramAddress: jupLimitProgramAddress,
				IsRent:         true,
				From:           from,
				To:             to,
				Amount:         amount,
			})
		}

		if len(innerIxs) != 1 {
			iix := innerIxs[1]
			data := iix.Data()
			amount := binary.LittleEndian.Uint64(data[1:])

			accounts := iix.AccountsAddresses()
			from := accounts[0]
			to := accounts[1]

			events = append(events, &EventTransfer{
				ProgramAddress: jupLimitProgramAddress,
				IsRent:         true,
				From:           from,
				To:             to,
				Amount:         amount,
			})
		}

		transferIix := innerIxs[len(innerIxs)-1]

		transferData := transferIix.Data()
		amount := binary.LittleEndian.Uint64(transferData)
		transferAccounts := transferIix.AccountsAddresses()

		events = append(events, &EventTransfer{
			ProgramAddress: jupLimitProgramAddress,
			IsRent:         false,
			From:           transferAccounts[0],
			To:             transferAccounts[2],
			Amount:         amount,
		})

		return events, nil
	} else if slices.Equal(disc, ixJupLimitPreFlashFillOrder) {
		accounts := ix.AccountsAddresses()
		orderAccountAddress := accounts[0]

		oa := parser.associatedAccounts.Get(orderAccountAddress)
		if oa != nil {
			orderAccount := oa.(*AssociatedAccountJupLimit)
			orderAccount.amountIn = binary.LittleEndian.Uint64(data[8:])
		}

		return nil, nil
	} else if slices.Equal(disc, ixJupLimitFlashFillOrder) {
		accounts := ix.AccountsAddresses()
		orderAddress := accounts[0]

		if oa := parser.associatedAccounts.Get(orderAddress); oa != nil {
			orderAccount := oa.(*AssociatedAccountJupLimit)

			innerIxs := ix.InnerIxs()
			transferIx := innerIxs[0]
			// feeIx := innerIxs[1]
			data := transferIx.Data()
			amountOut := binary.LittleEndian.Uint64(data[1:])

			events := []EventData{&EventSwap{
				ProgramAddress: jupLimitProgramAddress,
				From: []*SwapToken{{
					Amount:  orderAccount.amountIn,
					Account: orderAccount.accountIn,
				}},
				To: []*SwapToken{{
					Amount:  amountOut,
					Account: orderAccount.accountOut,
				}},
			}}

			lastIix := innerIxs[len(innerIxs)-1]
			lastIixData := lastIix.Data()

			if lastIix.ProgramAddress() == tokenProgramAddress && lastIixData[0] == ixTokenCloseAccount {
				walletAddress := accounts[2]
				events = append(
					events,
					&EventCloseAccount{
						ProgramAddress: jupLimitProgramAddress,
						From:           accounts[1],
						To:             walletAddress,
					},
					&EventCloseAccount{
						ProgramAddress: jupLimitProgramAddress,
						From:           orderAddress,
						To:             walletAddress,
					},
				)
			}

			ser, _ := json.Marshal(events)
			fmt.Printf("signature %s event %s\n\n", signature, string(ser))

			return events, nil
		}

		return nil, nil
	} else {
		slog.Warn("unknown jup limit instruction", "signature", signature)
		return nil, nil
	}
}
