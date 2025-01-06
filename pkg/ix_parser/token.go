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

func parseTokenIxAssociatedAccounts(associatedAccounts AssociatedAccounts, ix ParsableIx) error {
	data := ix.Data()
	if len(data) < 1 {
		return errDataTooSmall
	}

	switch data[0] {
	case ixTokenInitAccount3:
		fallthrough
	case ixTokenInitAccount2:
		fallthrough
	case ixTokenInitAccount:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}
		newAccount := accounts[0]
		mint := accounts[1]
		associatedAccounts.Append(&AssociatedAccountToken{address: newAccount, mint: mint})
	}

	return nil
}

func (parser *EventsParser) parseTokenIxEvents(ix ParsableIx) error {
	dataWithDisc := ix.Data()
	if len(dataWithDisc) < 1 {
		return errDataTooSmall
	}
	data := dataWithDisc[1:]

	switch dataWithDisc[0] {
	case ixTokenTransfer:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[1]

		if !parser.isRelated(from, to) {
			return nil
		}
		if len(data) < 8 {
			return errDataTooSmall
		}

		amount := binary.LittleEndian.Uint64(data)
		ix.AddEvent(&EventTransfer{
			ProgramAddress: tokenProgramAddress,
			IsRent:         false,
			From:           from,
			To:             to,
			Amount:         amount,
		})
	case ixTokenTransferChecked:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 3 {
			return errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[2]

		if !parser.isRelated(from, to) {
			return nil
		}
		if len(data) < 8 {
			return errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
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
			return errAccountsTooSmall
		}
		to := accounts[1]
		if !parser.isRelated(to) {
			return nil
		}
		if len(data) < 8 {
			return errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		ix.AddEvent(&EventMint{
			ProgramAddress: tokenProgramAddress,
			To:             to,
			Amount:         amount,
		})
	case ixTokenBurnChecked:
		fallthrough
	case ixTokenBurn:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 1 {
			return errAccountsTooSmall
		}
		from := accounts[0]
		if !parser.isRelated(from) {
			return nil
		}
		if len(data) < 8 {
			return errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		ix.AddEvent(&EventBurn{
			ProgramAddress: tokenProgramAddress,
			From:           from,
			Amount:         amount,
		})
	case ixTokenCloseAccount:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[1]

		if !parser.isRelated(from, to) {
			return nil
		}

		ix.AddEvent(&EventCloseAccount{
			ProgramAddress: tokenProgramAddress,
			From:           from,
			To:             to,
		})
	}

	return nil
}
