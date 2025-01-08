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

const bubblegumProgramAddress = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY"

var (
	ixBubblegumMintV1, _             = hex.DecodeString("9162c076b8937668")
	ixBubblegumMintToCollectionV1, _ = hex.DecodeString("9912b22fc59e560f")
)

type EventCompressedNftMint struct {
	ProgramAddress string `json:"program_address"`
	Name           string
	Symbol         string
	// Address        string
	To string
}

func (_ *EventCompressedNftMint) Type() dbutils.EventType {
	return dbutils.EventTypeMintCNft
}

func (parser *EventsParser) parseBubblegumIx(ix ParsableIx, signature string) (EventData, error) {
	data := ix.Data()
	if len(data) < 8 {
		return nil, errDataTooSmall
	}
	disc := data[:8]

	if slices.Equal(disc, ixBubblegumMintV1) || slices.Equal(disc, ixBubblegumMintToCollectionV1) {
		accounts := ix.AccountsAddresses()
		leafOwner := accounts[1]

		if leafOwner != parser.walletAddress {
			return nil, nil
		}

		// noopIix := ix.InnerIxs()[0]
		// noopData := noopIix.Data()

		// 1 uint8 noop enum prefix 0
		// 2 uint8 noop enum prefix 1
		// 3 uint8 bubblegumEventType 1
		// 4 uint8 version 1
		// 5 uint8 leafSchema prefix
		// if noopData

		data = data[8:]
		offset := uint32(0)

		nameLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if uint32(len(data)) < offset+nameLen {
			return nil, errDataTooSmall
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		if uint32(len(data)) < offset+4 {
			return nil, errDataTooSmall
		}
		symbolLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if uint32(len(data)) < offset+symbolLen {
			return nil, errDataTooSmall
		}
		symbol := string(data[offset : offset+symbolLen])

		event := &EventCompressedNftMint{
			ProgramAddress: bubblegumProgramAddress,
			Name:           name,
			Symbol:         symbol,
			To:             parser.walletAddress,
		}
		ser, _ := json.Marshal(event)
		fmt.Printf("signature %s event %s\n", signature, ser)

		return event, nil
	} else {
		slog.Warn("unknown bubblegum ix", "signature", signature)
	}

	return nil, nil
}
