package ixparser

import (
	"encoding/binary"
	"log/slog"
)

const associatedTokenProgramAddress = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

func parseAssociatedTokenIx(ix ParsableIx, walletAddress, signature string) (map[string]AssociatedAccount, error) {
	data := ix.Data()
	isCreate := len(data) == 0 || data[0] == 0 || data[0] == 1

	if isCreate {
		// CREATE || CREATE IDEMPOTENT
		innerIxs := ix.InnerIxs()
		innerIxsLen := len(innerIxs)

		if innerIxsLen == 0 {
			ix.SetKnown()
			return nil, nil
		}

		accounts := ix.AccountsAddresses()
		if len(accounts) < 3 {
			return nil, errAccountsTooSmall
		}
		ix.SetKnown()

		from := accounts[0]
		to := accounts[1]

		if innerIxsLen == 4 || innerIxsLen == 6 {
			// create from zero lamports || create with lamports
			// rent index = 1
			createAccountIx := innerIxs[1]
			data := createAccountIx.Data()[4:]
			lamports := binary.LittleEndian.Uint64(data)

			ix.AddEvent(&EventTransfer{
				ProgramAddress: associatedTokenProgramAddress,
				From:           from,
				To:             to,
				Amount:         lamports,
				IsRent:         true,
			})
		}

		owner := accounts[2]
		if owner == walletAddress {
			associatedAccounts := make(map[string]AssociatedAccount, 0)
			associatedAccounts[to] = &AssociatedAccountToken{
				address: to,
				mint:    accounts[2],
			}
			return associatedAccounts, nil
		}
	} else {
		// RECOVER NESTED
		slog.Error("unimplemented associated token instruction (recover nested)", "signature", signature)
	}

	return nil, nil
}
