package ixparser

import (
	"encoding/binary"
	"log/slog"
)

const associatedTokenProgramAddress = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

func parseAssociatedTokenIxAssociatedAccounts(
	associatedAccounts AssociatedAccounts,
	ix ParsableIx,
	walletAddress string,
) error {
	data := ix.Data()
	isCreate := len(data) == 0 || data[0] == 0 || data[0] == 1

	if isCreate {
		accounts := ix.AccountsAddresses()
		if len(accounts) < 4 {
			return errAccountsTooSmall
		}

		owner := accounts[2]
		if owner == walletAddress {
			associatedAccounts.Append(&AssociatedAccountToken{
				address:     accounts[1],
				mint:        accounts[3],
				shouldFetch: true,
			})
			return nil
		}
	}

	return nil
}

func (parser *EventsParser) parseAssociatedTokenIxEvents(ix ParsableIx, signature string) (EventData, error) {
	data := ix.Data()
	isCreate := len(data) == 0 || data[0] == 0 || data[0] == 1

	if isCreate {
		// CREATE || CREATE IDEMPOTENT
		innerIxs := ix.InnerIxs()
		innerIxsLen := len(innerIxs)

		if innerIxsLen == 0 {
			return nil, nil
		}

		accounts := ix.AccountsAddresses()
		if len(accounts) < 3 {
			return nil, errAccountsTooSmall
		}

		from := accounts[0]
		to := accounts[1]

		if !parser.isRelated(from) {
			return nil, nil
		}

		if innerIxsLen == 4 || innerIxsLen == 6 {
			// create from zero lamports || create with lamports
			// rent index = 1
			createAccountIx := innerIxs[1]
			data := createAccountIx.Data()[4:]
			lamports := binary.LittleEndian.Uint64(data)

			event := &EventTransfer{
				ProgramAddress: associatedTokenProgramAddress,
				From:           from,
				To:             to,
				Amount:         lamports,
				IsRent:         true,
			}
			return event, nil
		}
	} else {
		// RECOVER NESTED
		slog.Error("unimplemented associated token instruction (recover nested)", "signature", signature)
	}

	return nil, nil
}
