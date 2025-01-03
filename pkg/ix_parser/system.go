package ixparser

import (
	"encoding/binary"
)

const (
	systomProgramAddress = "11111111111111111111111111111111"

	ixSystemCreateAccount        = 0
	ixSystemTransfer             = 2
	ixSystemCreateWithSeed       = 3
	ixSystemWithdrawNonceAccount = 5
	ixSystemTransferWithSeed     = 11
)

func parseSystemIx(ix ParsableIx) error {
	dataWithDisc := ix.Data()
	if len(dataWithDisc) < 1 {
		return errDataTooSmall
	}

	data := dataWithDisc[1:]
	var ev *EventTransfer

	switch dataWithDisc[0] {
	case ixSystemCreateAccount:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}

		from := accounts[0]
		to := accounts[1]

		if len(data) < 8 {
			return errDataTooSmall
		}
		lamports := binary.LittleEndian.Uint64(data)

		ev = &EventTransfer{
			IsRent: true,
			From:   from,
			To:     to,
			Amount: lamports,
		}
	case ixSystemWithdrawNonceAccount:
		fallthrough
	case ixSystemTransfer:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}

		from := accounts[0]
		to := accounts[1]

		if len(data) < 8 {
			return errDataTooSmall
		}
		lamports := binary.LittleEndian.Uint64(data)

		ev = &EventTransfer{
			IsRent: false,
			From:   from,
			To:     to,
			Amount: lamports,
		}
	case ixSystemCreateWithSeed:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 2 {
			return errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[1]

		if len(data) < 40 {
			return errDataTooSmall
		}
		seedLen := binary.LittleEndian.Uint32(data[32:])
		seedPadding := binary.LittleEndian.Uint32(data[36:])
		if len(data) < 40+int(seedLen+seedPadding) {
			return errDataTooSmall
		}
		lamports := binary.LittleEndian.Uint64(data[40+seedLen+seedPadding:])

		ev = &EventTransfer{
			IsRent: true,
			From:   from,
			To:     to,
			Amount: lamports,
		}
	case ixSystemTransferWithSeed:
		accounts := ix.AccountsAddresses()
		if len(accounts) < 3 {
			return errAccountsTooSmall
		}
		from := accounts[0]
		to := accounts[2]

		if len(data) < 8 {
			return errDataTooSmall
		}
		lamports := binary.LittleEndian.Uint64(data)

		ev = &EventTransfer{
			IsRent: false,
			From:   from,
			To:     to,
			Amount: lamports,
		}
	}

	ix.SetKnown()
	if ev != nil {
		ix.AddEvent(ev)
	}

	return nil
}
