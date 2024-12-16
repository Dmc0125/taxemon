package rpc

import (
	"encoding/base64"
	"fmt"
	"math"

	"github.com/mr-tron/base58"
)

func deserializeCompactU16(data []byte) (uint16, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("unable to deserialize compactU16: data empty")
	}
	deserialized := uint16(0)
	ln := int(0)

	for i := 0; i < 3; i++ {
		ln += 1
		if i >= len(data) {
			return 0, 0, fmt.Errorf("unable to deserialize compactU16: data too short")
		}
		val := uint32(data[i])

		elemVal := val & 0x7f
		elemDone := (val & 0x80) == 0
		if i == 2 && !elemDone {
			return 0, 0, fmt.Errorf("unable to deserialize compactU16: byte 3 continues")
		}

		shift := 7 * i
		elemVal <<= shift

		newVal := val | elemVal
		if newVal > math.MaxUint16 {
			return 0, 0, fmt.Errorf("unable to deserialize compactU16: newVal overflow")
		}
		deserialized += uint16(newVal)
		if elemDone {
			break
		}
	}

	return deserialized, ln, nil
}

func deserializeInstruction(data []byte) (*TransactionInstructionBase, int, error) {
	if len(data) < 1 {
		return nil, 0, fmt.Errorf("deserialize ix error: data empty")
	}
	programIdIndex := uint8(data[0])
	offset := 1

	accountsIndexesLen, bytesLen, err := deserializeCompactU16(data[offset:])
	if err != nil {
		return nil, 0, err
	}
	offset += bytesLen

	if len(data) < offset+int(accountsIndexesLen) {
		return nil, 0, fmt.Errorf("deserialize ix error: accounts indexes too short")
	}

	accountsIndexes := make([]uint8, accountsIndexesLen)
	for i := range accountsIndexesLen {
		accountsIndexes[i] = data[offset]
		offset += 1
	}

	dataLen, bytesLen, err := deserializeCompactU16(data[offset:])
	if err != nil {
		return nil, 0, err
	}
	offset += bytesLen

	if len(data) < offset+int(dataLen) {
		return nil, 0, fmt.Errorf("deserialize ix error: data too short")
	}

	ixData := make([]byte, dataLen)
	copy(ixData, data[offset:offset+int(dataLen)])
	// ixDataEncoded := base64.StdEncoding.EncodeToString(ixData)
	offset += int(dataLen)

	ix := &TransactionInstructionBase{
		ProgramIdIndex:  programIdIndex,
		AccountsIndexes: accountsIndexes,
		Data:            ixData,
	}
	return ix, offset, nil
}
func deserializeLegacyMessage(data []byte) (*TransactionMessage, int, error) {
	if len(data) < 3 {
		return nil, 0, fmt.Errorf("unable to deserialize msg: data empty")
	}

	offset := 0

	msgHeader := &TransactionMessageHeader{
		NumRequiredSignatures:       uint8(data[offset]),
		NumReadonlySignedAccounts:   uint8(data[offset+1]),
		NumReadonlyUnsignedAccounts: uint8(data[offset+2]),
	}
	offset += 3

	accountKeysLen, bytesLen, err := deserializeCompactU16(data[offset:])
	if err != nil {
		return nil, 0, fmt.Errorf("unable to deserialize msg: %w", err)
	}
	offset += bytesLen

	if len(data) < offset+int(accountKeysLen)*32 {
		return nil, 0, fmt.Errorf("unable to deserialize msg: accounts keys too short")
	}

	accountKeys := make([]string, accountKeysLen)
	for i := range accountKeysLen {
		bytes := data[offset : offset+32]
		accountKeys[i] = base58.Encode(bytes)
		offset += 32
	}

	recentBlockhashOffset := offset
	offset += 32
	if len(data) < offset {
		return nil, 0, fmt.Errorf("unable to deserialize msg: missing blockhash")
	}
	recentBlockhash := base58.Encode(data[recentBlockhashOffset:offset])

	ixsLen, bytesLen, err := deserializeCompactU16(data[offset:])
	if err != nil {
		return nil, 0, fmt.Errorf("unable to deserialize msg: %w", err)
	}
	offset += bytesLen

	instructions := make([]*TransactionInstructionBase, ixsLen)
	for i := range ixsLen {
		ix, bytesLen, err := deserializeInstruction(data[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("unable to deserialize msg: %w", err)
		}
		instructions[i] = ix
		offset += bytesLen
	}

	msg := &TransactionMessage{
		Header:          msgHeader,
		AccountKeys:     accountKeys,
		RecentBlockhash: recentBlockhash,
		Instructions:    instructions,
	}

	return msg, offset, nil
}

func deserializeCompactUint8Array(data []byte) ([]uint8, int, error) {
	offset := 0
	l, bytesLen, err := deserializeCompactU16(data[offset:])
	if err != nil {
		return nil, 0, fmt.Errorf("unable to desrialize compact array: %w", err)
	}
	offset += bytesLen

	if len(data) < offset+int(l) {
		return nil, 0, fmt.Errorf("unable to desrialize compact array: data too short")
	}

	out := make([]uint8, l)
	for i := range l {
		out[i] = data[offset]
		offset += 1
	}

	return out, offset, nil
}

func deserializeV0Message(data []byte) (*TransactionMessage, int, error) {
	if len(data) == 0 {
		return nil, 0, fmt.Errorf("unable to deserialize v0 msg: data empty")
	}

	msg, offset, err := deserializeLegacyMessage(data)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to deserialize v0 msg: %w", err)
	}

	addressTableLookupsLen, bytesLen, err := deserializeCompactU16(data[offset:])
	if err != nil {
		return nil, 0, fmt.Errorf("unable to deserialize v0 msg: %w", err)
	}
	offset += bytesLen

	msg.AddressTableLookups = make([]*TransactionAddressTableLookup, addressTableLookupsLen)
	for i := range addressTableLookupsLen {
		if len(data) < offset+32 {
			return nil, 0, fmt.Errorf("unable to deserialize v0 msg: address lookup tables too short")
		}
		address := base58.Encode(data[offset : offset+32])
		offset += 32

		writableAccountsIndexes, bytesLen, err := deserializeCompactUint8Array(data[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("unable to deserialize v0 msg: %w", err)
		}
		offset += bytesLen
		readonlyAccountsIndexes, bytesLen, err := deserializeCompactUint8Array(data[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("unable to deserialize v0 msg: %w", err)
		}
		offset += bytesLen

		msg.AddressTableLookups[i] = &TransactionAddressTableLookup{
			AccountKey:      address,
			WritableIndexes: writableAccountsIndexes,
			ReadonlyIndexes: readonlyAccountsIndexes,
		}
	}

	return msg, offset, nil
}

func DeserializeTransaction(encoded, encoding string) (*Transaction, error) {
	if encoding != string(EncodingBase64) {
		return nil, fmt.Errorf("deserialize transaction err: unsupported encoding: \"%s\"", encoding)
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("deserialize transaction err: unable to decode transaction")
	}

	offset, signaturesLen, err := deserializeCompactU16(data)
	if err != nil {
		return nil, fmt.Errorf("deserialize transaction err: %w", err)
	}

	signatures := make([]string, signaturesLen)
	for i := range signaturesLen {
		bytes := data[offset : offset+64]
		signatures[i] = base58.Encode(bytes)
		offset += 64
	}

	var msg *TransactionMessage
	messagePrefix := data[offset]

	if messagePrefix>>7 == 1 {
		// 1xxxxxxx
		offset += 1
		version := uint8((messagePrefix << 1) >> 1)
		switch version {
		case 0:
			msg, _, err = deserializeV0Message(data[offset:])
			msg.Version = 0
		default:
			return nil, fmt.Errorf("deserialize transaction err: invalid msg version: %d", version)
		}
	} else {
		msg, _, err = deserializeLegacyMessage(data[offset:])
		msg.Version = 255
	}
	if err != nil {
		return nil, fmt.Errorf("deserialize transaction err: %w", err)
	}

	tx := &Transaction{
		Signatures: signatures,
		Message:    msg,
	}
	return tx, nil
}
