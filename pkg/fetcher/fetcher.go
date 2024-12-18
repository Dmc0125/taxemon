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

	"golang.org/x/sync/errgroup"
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

type insertableInstructionBase struct {
	program_id_index int64
	accounts_idxs    string
	data             []byte
}

type insertableInstruction struct {
	*insertableInstructionBase
	innerInstructions []*insertableInstructionBase
}

type InsertableTransaction struct {
	signature string
	timestamp int64
	slot      int64
	err       bool
	errMsg    string

	logs      [][]byte
	addresses []string

	ixs []*insertableInstruction
}

func newInsertableTransaction(tx *rpc.ParsedTransactionResult) (*InsertableTransaction, error) {
	itx := &InsertableTransaction{
		signature: tx.Transaction.Signatures[0],
		timestamp: tx.BlockTime,
		slot:      int64(tx.Slot),
		err:       tx.Meta.Err == nil,

		logs:      make([][]byte, 0),
		addresses: make([]string, 0),

		ixs: make([]*insertableInstruction, 0),
	}
	if itx.err {
		errMsg, err := json.Marshal(tx.Meta.Err)
		if err != nil {
			itx.errMsg = string(errMsg)
		}
	}

	itx.addresses = slices.AppendSeq(
		slices.AppendSeq(
			slices.Clone(tx.Transaction.Message.AccountKeys),
			slices.Values(tx.Meta.LoadedAddresses.Readonly),
		),
		slices.Values(tx.Meta.LoadedAddresses.Writable),
	)

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
			itx.logs = append(itx.logs, bytes)
		}
	}

	for _, ix := range tx.Transaction.Message.Instructions {
		accountsIdxs, err := json.Marshal(ix.AccountsIndexes)
		if err != nil {
			return nil, err
		}
		itx.ixs = append(itx.ixs, &insertableInstruction{
			insertableInstructionBase: &insertableInstructionBase{
				program_id_index: int64(ix.ProgramIdIndex),
				accounts_idxs:    string(accountsIdxs),
				data:             ix.Data,
			},
			innerInstructions: make([]*insertableInstructionBase, 0),
		})
	}
	for _, innerIxs := range tx.Meta.InnerInstructions {
		ix := itx.ixs[int(innerIxs.IxIndex)]
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

	return itx, nil
}

type preparedQuery struct {
	queryPrefix  string
	querySuffix  string
	placeholders []string
	args         []interface{}
}

func newPreparedQuery(prefix, suffix string) *preparedQuery {
	return &preparedQuery{
		queryPrefix:  prefix,
		querySuffix:  suffix,
		placeholders: make([]string, 0),
		args:         make([]interface{}, 0),
	}
}

func (pq *preparedQuery) string() string {
	return fmt.Sprintf(
		"%s %s %s;",
		pq.queryPrefix,
		strings.Join(pq.placeholders, ","),
		pq.querySuffix,
	)
}

func prepareInsertTransactionsQuery(txs []*InsertableTransaction) *preparedQuery {
	pq := newPreparedQuery(
		"INSERT INTO transaction (signature, timestamp, slot, err, err_msg) VALUES",
		"ON CONFLICT (signature) DO NOTHING RETURNING id;",
	)
	for _, tx := range txs {
		pq.placeholders = append(pq.placeholders, "(?, ?, ?, ?, ?)")
		pq.args = append(pq.args, tx.signature, tx.timestamp, tx.slot, tx.err, tx.errMsg)
	}
	return pq
}

type insertedTransaction struct {
	Signature string
	Id        int64
}

func insertTransactions(db *sql.DB, txs []*InsertableTransaction) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	txsPreparedQuery := prepareInsertTransactionsQuery(txs)
	rows, err := tx.Query(txsPreparedQuery.string(), txsPreparedQuery.args...)
	if err != nil {
		return err
	}

	insertedTxs := make([]*insertedTransaction, 0)
	for rows.Next() {
		t := new(insertedTransaction)
		if err = rows.Scan(t); err != nil {
			return err
		}
		insertedTxs = append(insertedTxs, t)
	}

	accountsPreparedQuery := newPreparedQuery(
		"INSERT INTO transaction_accounts (transaction_id, address, idx) VALUES",
		"",
	)
	logsPreparedQuery := newPreparedQuery(
		"INSERT INTO transaction_logs (transaction_id, log, idx) VALUES",
		"",
	)
	ixsPreparedQuery := newPreparedQuery(
		"INSERT INTO instruction (transaction_id, idx, program_id_idx, accounts_idxs, data) VALUES",
		"",
	)
	innerIxsPreparedQuery := newPreparedQuery(
		"INSERT INTO inner_instruction (transaction_id, ix_idx, idx, program_id_idx, accounts_idxs, data) VALUES",
		"",
	)

	for _, tx := range txs {
		txIdIdx := slices.IndexFunc(insertedTxs, func(itx *insertedTransaction) bool {
			return itx.Signature == tx.signature
		})
		if txIdIdx == -1 {
			return fmt.Errorf("unable to find inserted tx")
		}
		txId := insertedTxs[txIdIdx].Id

		for accountIdx, address := range tx.addresses {
			accountsPreparedQuery.placeholders = append(accountsPreparedQuery.placeholders, "(?, ?, ?)")
			accountsPreparedQuery.args = append(accountsPreparedQuery.args, txId, address, accountIdx)
		}
		for logIdx, log := range tx.logs {
			logsPreparedQuery.placeholders = append(logsPreparedQuery.placeholders, "(?, ?, ?)")
			logsPreparedQuery.args = append(logsPreparedQuery.args, txId, log, logIdx)
		}
		for ixIdx, ix := range tx.ixs {
			ixsPreparedQuery.placeholders = append(ixsPreparedQuery.placeholders, "(?, ?, ?, ?, ?)")
			ixsPreparedQuery.args = append(ixsPreparedQuery.args, txId, ixIdx, ix.program_id_index, ix.accounts_idxs, ix.data)

			for innerIxIdx, innerIx := range ix.innerInstructions {
				innerIxsPreparedQuery.placeholders = append(innerIxsPreparedQuery.placeholders, "(?, ?, ?, ?, ?, ?)")
				innerIxsPreparedQuery.args = append(
					innerIxsPreparedQuery.args,
					txId,
					ixIdx,
					innerIxIdx,
					innerIx.program_id_index,
					innerIx.accounts_idxs,
					innerIx.data,
				)
			}
		}
	}

	var eg errgroup.Group
	eg.TryGo(func() error {
		_, err := tx.Exec(accountsPreparedQuery.string(), accountsPreparedQuery.args...)
		return err
	})
	eg.TryGo(func() error {
		_, err := tx.Exec(logsPreparedQuery.string(), logsPreparedQuery.args...)
		return err
	})
	eg.TryGo(func() error {
		_, err := tx.Exec(ixsPreparedQuery.string(), ixsPreparedQuery.args...)
		if err != nil {
			return err
		}
		_, err = tx.Exec(innerIxsPreparedQuery.string(), innerIxsPreparedQuery.args...)
		return err
	})
	if err = eg.Wait(); err != nil {
		return err
	}

	err = tx.Commit()
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

			insertableTx, err := newInsertableTransaction(tx)
			assert.NoErr(err, "unable to create insertable tx")
			insertableTransactions = append(insertableTransactions, insertableTx)
		}

		err = insertTransactions(db, insertableTransactions)
		assert.NoErr(err, "unable to insert transactions")
	}
}
