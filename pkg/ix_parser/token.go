package ixparser

import (
	"encoding/binary"
)

const (
	tokenProgramAddress = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

	ixTokenTransfer        = 3
	ixTokenTransferChecked = 12
	ixTokenMintTo          = 7
	ixTokenMintToChecked   = 14
	ixTokenBurn            = 8
	ixTokenBurnChecked     = 15
	ixTokenCloseAccount    = 9

	ixTokenInitAccount  = 1
	ixTokenInitAccount2 = 16
	ixTokenInitAccount3 = 18
)

func getFromToAccounts(fromIdx, toIdx, l int, accounts []string) (string, string, error) {
	if len(accounts) < l {
		return "", "", errAccountsTooSmall
	}
	from := accounts[fromIdx]
	to := accounts[toIdx]
	return from, to, nil
}

func parseTokenIx(ix ParsableIx) (map[string]AssociatedAccount, error) {
	dataWithDisc := ix.Data()
	if len(dataWithDisc) < 1 {
		return nil, errDataTooSmall
	}
	data := dataWithDisc[1:]

	switch dataWithDisc[0] {
	case ixTokenTransfer:
		from, to, err := getFromToAccounts(0, 1, 2, ix.AccountsAddresses())
		if err != nil {
			return nil, err
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}

		amount := binary.LittleEndian.Uint64(data)
		ix.SetKnown()
		ix.AddEvent(&EventTransfer{
			ProgramAddress: tokenProgramAddress,
			IsRent:         false,
			From:           from,
			To:             to,
			Amount:         amount,
		})
	case ixTokenTransferChecked:
		from, to, err := getFromToAccounts(0, 2, 3, ix.AccountsAddresses())
		if err != nil {
			return nil, err
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		ix.SetKnown()
		ix.AddEvent(&EventTransfer{
			ProgramAddress: tokenProgramAddress,
			IsRent:         false,
			From:           from,
			To:             to,
			Amount:         amount,
		})
	case ixTokenMintToChecked:
		fallthrough
	case ixTokenMintTo:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return nil, errAccountsTooSmall
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		ix.SetKnown()
		ix.AddEvent(&EventMint{
			ProgramAddress: tokenProgramAddress,
			To:             accounts[1],
			Amount:         amount,
		})
	case ixTokenBurnChecked:
		fallthrough
	case ixTokenBurn:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 1 {
			return nil, errAccountsTooSmall
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		ix.SetKnown()
		ix.AddEvent(&EventBurn{
			ProgramAddress: tokenProgramAddress,
			From:           accounts[0],
			Amount:         amount,
		})
	case ixTokenCloseAccount:
		from, to, err := getFromToAccounts(0, 1, 2, ix.AccountsAddresses())
		if err != nil {
			return nil, err
		}
		ix.SetKnown()
		ix.AddEvent(&EventCloseAccount{
			ProgramAddress: tokenProgramAddress,
			From:           from,
			To:             to,
		})
	case ixTokenInitAccount3:
		fallthrough
	case ixTokenInitAccount2:
		fallthrough
	case ixTokenInitAccount:
		accountAddress, mint, err := getFromToAccounts(0, 1, 2, ix.AccountsAddresses())
		if err != nil {
			return nil, err
		}
		accounts := make(map[string]AssociatedAccount)
		accounts[accountAddress] = &AssociatedAccountToken{address: accountAddress, mint: mint}
		ix.SetKnown()
		return accounts, nil
	default:
		ix.SetKnown()
	}

	return nil, nil
}
