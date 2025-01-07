package ixparser

import (
	"encoding/binary"
	"encoding/json"
	dbutils "taxemon/pkg/db_utils"

	"github.com/mr-tron/base58"
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

type AssociatedAccountToken struct {
	address     string
	mint        string
	shouldFetch bool
}

func NewAssociatedAccountToken(address, mint string, shouldFetch bool) *AssociatedAccountToken {
	return &AssociatedAccountToken{
		address, mint, shouldFetch,
	}
}

func (account *AssociatedAccountToken) Address() string {
	return account.address
}

func (account *AssociatedAccountToken) Type() dbutils.AssociatedAccountType {
	return dbutils.AssociatedAccountToken
}

func (account *AssociatedAccountToken) Data() ([]byte, error) {
	d := map[string]interface{}{
		"mint": account.mint,
	}
	return json.Marshal(d)
}

func (account *AssociatedAccountToken) ShouldFetch() bool {
	return account.shouldFetch
}

func parseTokenIxAssociatedAccounts(associatedAccounts AssociatedAccounts, ix ParsableIx, walletAddress string) error {
	data := ix.Data()
	if len(data) < 1 {
		return errDataTooSmall
	}

	var (
		owner, newAccount, mint string
	)

	switch data[0] {
	case ixTokenInitAccount3:
		if len(data) < 33 {
			return errDataTooSmall
		}
		owner = base58.Encode(data[1:])

		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}
		mint = accounts[1]
		owner = accounts[2]
	case ixTokenInitAccount2:
		if len(data) < 33 {
			return errDataTooSmall
		}
		owner = base58.Encode(data[1:])

		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}
		mint = accounts[1]
		owner = accounts[2]
	case ixTokenInitAccount:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 3 {
			return errAccountsTooSmall
		}
		newAccount = accounts[0]
		mint = accounts[1]
		owner = accounts[2]
	default:
		return nil
	}

	if owner == walletAddress {
		associatedAccounts.Append(&AssociatedAccountToken{
			address:     newAccount,
			mint:        mint,
			shouldFetch: true,
		})
	}

	return nil
}

func (parser *EventsParser) parseTokenIxEvents(ix ParsableIx) (EventData, error) {
	dataWithDisc := ix.Data()
	if len(dataWithDisc) < 1 {
		return nil, errDataTooSmall
	}
	data := dataWithDisc[1:]

	switch dataWithDisc[0] {
	case ixTokenTransfer:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return nil, errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[1]

		if !parser.isRelated(from, to) {
			return nil, nil
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}

		amount := binary.LittleEndian.Uint64(data)
		event := &EventTransfer{
			ProgramAddress: tokenProgramAddress,
			IsRent:         false,
			From:           from,
			To:             to,
			Amount:         amount,
		}
		return event, nil
	case ixTokenTransferChecked:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 3 {
			return nil, errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[2]

		if !parser.isRelated(from, to) {
			return nil, nil
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		event := &EventTransfer{
			ProgramAddress: tokenProgramAddress,
			IsRent:         false,
			From:           from,
			To:             to,
			Amount:         amount,
		}
		return event, nil
	case ixTokenMintToChecked:
		fallthrough
	case ixTokenMintTo:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return nil, errAccountsTooSmall
		}
		to := accounts[1]
		if !parser.isRelated(to) {
			return nil, nil
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		event := &EventMint{
			ProgramAddress: tokenProgramAddress,
			To:             to,
			Amount:         amount,
		}
		return event, nil
	case ixTokenBurnChecked:
		fallthrough
	case ixTokenBurn:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 1 {
			return nil, errAccountsTooSmall
		}
		from := accounts[0]
		if !parser.isRelated(from) {
			return nil, nil
		}
		if len(data) < 8 {
			return nil, errDataTooSmall
		}
		amount := binary.LittleEndian.Uint64(data)
		event := &EventBurn{
			ProgramAddress: tokenProgramAddress,
			From:           from,
			Amount:         amount,
		}
		return event, nil
	case ixTokenCloseAccount:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return nil, errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[1]

		if !parser.isRelated(from, to) {
			return nil, nil
		}

		event := &EventCloseAccount{
			ProgramAddress: tokenProgramAddress,
			From:           from,
			To:             to,
		}
		return event, nil
	default:
		return nil, nil
	}
}
