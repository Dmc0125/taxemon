package fetcher

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"taxemon/pkg/assert"
	"taxemon/pkg/dbgen"
	"taxemon/pkg/rpc"
)

func forceRpcRequest[T any](f func() (T, error), limit uint8) (T, error) {
	var (
		res T
		err error
	)
	for range limit {
		res, err = f()
		if err == nil {
			return res, nil
		}
	}
	return res, fmt.Errorf("unable to execute rpc request")
}

type InsertableTransaction struct {
	signature string
	timestamp int64
	slot      int64
	err       bool
	errMsg    string
}

func newInsertableTransaction(tx *rpc.ParsedTransactionResult) *InsertableTransaction {
	itx := &InsertableTransaction{
		signature: tx.Transaction.Signatures[0],
		timestamp: tx.BlockTime,
		slot:      int64(tx.Slot),
		err:       tx.Meta.Err == nil,
	}
	if itx.err {
		errMsg, err := json.Marshal(tx.Meta.Err)
		if err != nil {
			itx.errMsg = string(errMsg)
		}
	}
	return itx
}

type insertedTransaction struct {
	Signature string
	Id        int64
}

func insertTransactions(db dbgen.DBTX, txs []*InsertableTransaction) ([]*insertedTransaction, error) {
	placeholders := make([]string, len(txs))
	args := []interface{}{}

	for _, tx := range txs {
		placeholders = append(placeholders, "(?, ?, ?, ?, ?)")
		args = append(args, tx.signature, tx.timestamp, tx.slot, tx.err, tx.errMsg)
	}

	q := strings.Builder{}
	q.WriteString("INSERT INTO transaction (signature, timestamp, slot, err, err_msg) VALUES ")
	q.WriteString(strings.Join(placeholders, ","))
	q.WriteString(" ON CONFLICT (signature) DO NOTHING RETURNING id;")

	rows, err := db.QueryContext(context.Background(), q.String(), args...)
	if err != nil {
		return nil, err
	}
	insertedTxs := make([]*insertedTransaction, 0)
	for rows.Next() {
		t := new(insertedTransaction)
		if err = rows.Scan(t); err != nil {
			return nil, err
		}
		insertedTxs = append(insertedTxs, t)
	}

	return insertedTxs, nil
}

type insertableTransactionAccounts struct {
	signature string
	addresses []string
}

func newInsertableTransactionAccounts(tx *rpc.ParsedTransactionResult) *insertableTransactionAccounts {
	addresses := slices.AppendSeq(
		slices.Clone(tx.Transaction.Message.AccountKeys),
		slices.Values(tx.Meta.LoadedAddresses.Readonly),
	)
	addresses = slices.AppendSeq(addresses, slices.Values(tx.Meta.LoadedAddresses.Writable))
	return &insertableTransactionAccounts{
		signature: tx.Transaction.Signatures[0],
		addresses: addresses,
	}
}

func insertTransactionAccounts(db dbgen.DBTX, accounts []*insertableTransactionAccounts, insertedTransactions []*insertedTransaction) error {
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	for _, a := range accounts {
		transactionIdx := slices.IndexFunc(insertedTransactions, func(tx *insertedTransaction) bool {
			return tx.Signature == a.signature
		})
		if transactionIdx == -1 {
			return fmt.Errorf("unable to find transaction in inserted transactions")
		}
		txId := insertedTransactions[transactionIdx].Id
		for idx, address := range a.addresses {
			placeholders = append(placeholders, "(?, ?, ?)")
			args = append(args, txId, address, idx)
		}
	}

	q := strings.Builder{}
	q.WriteString("INSERT INTO transaction_account (transaction_id, address, idx) VALUES ")
	q.WriteString(strings.Join(placeholders, ","))
	q.WriteString(";")

	_, err := db.ExecContext(context.Background(), q.String(), args)
	return err
}

type insertableTransactionLogs struct {
	signature string
	logs      [][]byte
}

func newInsertableTransactionLogs(tx *rpc.ParsedTransactionResult) *insertableTransactionLogs {
	parsedMessages := make([][]byte, 0)
	for _, msg := range tx.Meta.LogMessages {
		var startIdx int
		if strings.HasPrefix(msg, "Program log:") {
			startIdx = 13
		} else if strings.HasPrefix(msg, "Program data:") {
			startIdx = 14
		} else {
			continue
		}

		dataEncoded := msg[startIdx:]
		if len(dataEncoded) == 0 {
			continue
		}

		if bytes, err := base64.StdEncoding.DecodeString(dataEncoded); err == nil {
			parsedMessages = append(parsedMessages, bytes)
		}
	}

	return &insertableTransactionLogs{
		signature: tx.Transaction.Signatures[0],
		logs:      parsedMessages,
	}
}

func insertTransactionLogs(db dbgen.DBTX, logs []*insertableTransactionLogs, insertedTransactions []*insertedTransaction) error {
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	for _, l := range logs {
		transactionIdx := slices.IndexFunc(insertedTransactions, func(tx *insertedTransaction) bool {
			return tx.Signature == l.signature
		})
		if transactionIdx == -1 {
			return fmt.Errorf("unable to find transaction in inserted transactions")
		}
		txId := insertedTransactions[transactionIdx].Id
		for idx, log := range l.logs {
			placeholders = append(placeholders, "(?, ?, ?)")
			args = append(args, txId, log, idx)
		}
	}

	q := strings.Builder{}
	q.WriteString("INSERT INTO transaction_log (transaction_id, log, idx) VALUES ")
	q.WriteString(strings.Join(placeholders, ","))
	q.WriteString(";")

	_, err := db.ExecContext(context.Background(), q.String(), args)
	return err
}

type insertableInstructionBase struct {
	program_id_index int64
	accounts_idxs    string
	data             []byte
}

type insertableInstruction struct {
	*insertableInstructionBase
	innerInstructions []*insertableInstructionBase
}

type insertableInstructions struct {
	signature    string
	instructions []*insertableInstruction
}

func newInsertableInstructions(tx *rpc.ParsedTransactionResult) (*insertableInstructions, error) {
	instructions := make([]*insertableInstruction, 0)

	for _, ix := range tx.Transaction.Message.Instructions {
		accountsIdxs, err := json.Marshal(ix.AccountsIndexes)
		if err != nil {
			return nil, err
		}
		instructions = append(instructions, &insertableInstruction{
			insertableInstructionBase: &insertableInstructionBase{
				program_id_index: int64(ix.ProgramIdIndex),
				accounts_idxs:    string(accountsIdxs),
				data:             ix.Data,
			},
			innerInstructions: make([]*insertableInstructionBase, 0),
		})
	}
	for _, innerIxs := range tx.Meta.InnerInstructions {
		ix := instructions[int(innerIxs.IxIndex)]
		for _, innerIx := range innerIxs.Instructions {
			accountsIds, err := json.Marshal(innerIx.Data)
			if err != nil {
				return nil, err
			}
			ix.innerInstructions = append(ix.innerInstructions, &insertableInstructionBase{
				program_id_index: int64(innerIx.ProgramIdIndex),
				accounts_idxs:    string(accountsIds),
				data:             innerIx.Data,
			})
		}
	}

	return &insertableInstructions{
		signature:    tx.Transaction.Signatures[0],
		instructions: instructions,
	}, nil
}

func insertInstructions(db dbgen.DBTX, ixs []*insertableInstructions, insertedTransactions []*insertedTransaction) error {
	ixPlaceholders := make([]string, 0)
	ixArgs := make([]interface{}, 0)
	iixPlaceholders := make([]string, 0)
	iixArgs := make([]interface{}, 0)

	for _, _ix := range ixs {
		txId := slices.IndexFunc(insertedTransactions, func(tx *insertedTransaction) bool {
			return tx.Signature == _ix.signature
		})
		if txId == -1 {
			return fmt.Errorf("unable to find inserted transaction")
		}

		for idx, ix := range _ix.instructions {
			ixPlaceholders = append(ixPlaceholders, "(?, ?, ?, ?, ?)")
			ixArgs = append(ixArgs, txId, idx, ix.program_id_index, ix.accounts_idxs, ix.data)

			for innerIxIdx, iix := range ix.innerInstructions {
				iixPlaceholders = append(iixPlaceholders, "(?, ?, ?, ?, ?, ?)")
				iixArgs = append(iixArgs, txId, idx, innerIxIdx, iix.program_id_index, iix.accounts_idxs, iix.data)
			}
		}
	}

	q := strings.Builder{}
	q.WriteString("INSERT INTO instruction (transaction_id, idx, program_id_idx, accounts_idxs, data) VALUES ")
	q.WriteString(strings.Join(ixPlaceholders, ","))
	q.WriteString(";")

	_, err := db.ExecContext(context.Background(), q.String(), ixArgs...)
	if err != nil {
		return err
	}

	q.Reset()
	q.WriteString("INSERT INTO inner_instruction (transaction_id, ix_idx, idx, program_id_idx, accounts_idxs, data) VALUES ")
	q.WriteString(strings.Join(iixPlaceholders, ","))
	q.WriteString(";")

	_, err = db.ExecContext(context.Background(), q.String(), iixArgs...)
	return err
}

func SyncWallet(
	rpcClient *rpc.Client,
	db *sql.DB,
	q *dbgen.Queries,
	walletAddress string,
) {
	slog.Info("running wallet sync", "walletAddress", walletAddress)
	l := uint64(1000)
	getSignaturesConfig := rpc.GetSignaturesForAddressConfig{
		Limit:      &l,
		Commitment: rpc.CommitmentConfirmed,
	}

	for {
		signaturesResults, err := forceRpcRequest(func() ([]*rpc.SignatureResult, error) {
			return rpcClient.GetSignaturesForAddress(walletAddress, &getSignaturesConfig)
		}, 5)
		assert.NoErr(err, "unable to fetch singatures")
		slog.Info("fetched signatures for wallet", "signatures", len(signaturesResults))

		signatures := make([]string, len(signaturesResults))
		for i, sr := range signaturesResults {
			signatures[i] = sr.Signature
		}
		savedTransactions, err := q.FetchTransactions(context.Background(), signatures)
		// parse associated accounts for saved transactions

		insertableTransactions := make([]*InsertableTransaction, 0)
		insertableTransactionsAccounts := make([]*insertableTransactionAccounts, 0)
		insertableTransactionLogs := make([]*insertableTransactionLogs, 0)
		insertableInstructions := make([]*insertableInstructions, 0)

		for _, sr := range signaturesResults {
			if txIdx := slices.IndexFunc(savedTransactions, func(tx *dbgen.VTransaction) bool {
				return tx.Signature == sr.Signature
			}); txIdx > -1 {
				continue
			}

			slog.Info("fetching tx")
			tx, err := forceRpcRequest(func() (*rpc.ParsedTransactionResult, error) {
				return rpcClient.GetTransaction(sr.Signature, rpc.CommitmentConfirmed)
			}, 5)
			assert.NoErr(err, "unable to get transaction")

			insertableTransactions = append(insertableTransactions, newInsertableTransaction(tx))
			insertableTransactionsAccounts = append(insertableTransactionsAccounts, newInsertableTransactionAccounts(tx))
			insertableTransactionLogs = append(insertableTransactionLogs, newInsertableTransactionLogs(tx))
			iixs, err := newInsertableInstructions(tx)
			assert.NoErr(err, "unable to create insertable instructions")
			insertableInstructions = append(insertableInstructions, iixs)
		}

		tx, err := db.Begin()
		insertedTxs, err := insertTransactions(tx, insertableTransactions)
		assert.NoErr(err, "unable to insert transactions")

		err = insertTransactionAccounts(tx, insertableTransactionsAccounts, insertedTxs)
		assert.NoErr(err, "unable to insert transactions accounts")
		err = insertTransactionLogs(tx, insertableTransactionLogs, insertedTxs)
		assert.NoErr(err, "unable to insert transactions logs")
		err = insertInstructions(tx, insertableInstructions, insertedTxs)
		assert.NoErr(err, "unable to insert transactions instructions")
	}
}
