package ixparser

import (
	"encoding/binary"
)

const (
	systemProgramAddress = "11111111111111111111111111111111"

	ixSystemCreateAccount        = 0
	ixSystemTransfer             = 2
	ixSystemCreateWithSeed       = 3
	ixSystemWithdrawNonceAccount = 5
	ixSystemTransferWithSeed     = 11
)

func (parser *EventsParser) parseSystemIxEvents(ix ParsableIx) error {
	dataWithDisc := ix.Data()
	if len(dataWithDisc) < 1 {
		return errDataTooSmall
	}

	data := dataWithDisc[1:]

	switch dataWithDisc[0] {
	case ixSystemCreateAccount:
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
		lamports := binary.LittleEndian.Uint64(data)

		ix.AddEvent(&EventTransfer{
			IsRent: true,
			From:   from,
			To:     to,
			Amount: lamports,
		})
	case ixSystemWithdrawNonceAccount:
		fallthrough
	case ixSystemTransfer:
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
		lamports := binary.LittleEndian.Uint64(data)

		ix.AddEvent(&EventTransfer{
			IsRent: false,
			From:   from,
			To:     to,
			Amount: lamports,
		})
	case ixSystemCreateWithSeed:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[1]

		if !parser.isRelated(from, to) {
			return nil
		}

		if len(data) < 40 {
			return errDataTooSmall
		}
		seedLen := binary.LittleEndian.Uint32(data[32:])
		seedPadding := binary.LittleEndian.Uint32(data[36:])
		if len(data) < 40+int(seedLen+seedPadding) {
			return errDataTooSmall
		}
		lamports := binary.LittleEndian.Uint64(data[40+seedLen+seedPadding:])

		ix.AddEvent(&EventTransfer{
			IsRent: true,
			From:   from,
			To:     to,
			Amount: lamports,
		})
	case ixSystemTransferWithSeed:
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
		lamports := binary.LittleEndian.Uint64(data)

		ix.AddEvent(&EventTransfer{
			IsRent: false,
			From:   from,
			To:     to,
			Amount: lamports,
		})
	}
	return nil
}
