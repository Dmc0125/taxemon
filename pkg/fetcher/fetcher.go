package fetcher

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"strings"
	"taxemon/pkg/assert"
	"taxemon/pkg/dbgen"
	ixparser "taxemon/pkg/ix_parser"
	"taxemon/pkg/rpc"

	"github.com/mr-tron/base58"
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
	return res, err
}

type insertableInstructionBase struct {
	programIdIdx      int64
	programAddress    string
	accountsIndexes   []int
	accountsAddresses []string
	data              []byte
}

func (ix *insertableInstructionBase) ProgramAddress() string {
	return ix.programAddress
}

func (ix *insertableInstructionBase) AccountsAddresses() []string {
	return ix.accountsAddresses
}

func (ix *insertableInstructionBase) Data() []byte {
	return ix.data
}

func newInsertableInstructionBase(
	ix *rpc.TransactionInstructionBase,
	txAddresses []string,
	isInnerIx bool,
) (*insertableInstructionBase, error) {
	accountsAddresses := make([]string, len(ix.AccountsIndexes))
	for i, idx := range ix.AccountsIndexes {
		if int(idx) >= len(txAddresses) {
			return nil, fmt.Errorf("invalid account index: %d len: %d", idx, len(ix.AccountsIndexes))
		}
		accountsAddresses[i] = txAddresses[idx]
	}

	var (
		ixData []byte
		err    error
	)
	if isInnerIx {
		ixData, err = base58.Decode(ix.Data)
	} else {
		ixData, err = base64.StdEncoding.DecodeString(ix.Data)
	}
	if err != nil {
		return nil, err
	}

	iix := insertableInstructionBase{
		programIdIdx:      int64(ix.ProgramIdIndex),
		programAddress:    txAddresses[ix.ProgramIdIndex],
		accountsIndexes:   ix.AccountsIndexes,
		accountsAddresses: accountsAddresses,
		data:              ixData,
	}
	return &iix, nil
}

type insertableInstruction struct {
	*insertableInstructionBase
	innerInstructions []*insertableInstructionBase
	events            []ixparser.Event
}

func (ix *insertableInstruction) InnerIxs() []ixparser.ParsableIxBase {
	// compiler check if insertableInstructionBase implements ixparser.ParsableIxBase
	var _ ixparser.ParsableIxBase = (*insertableInstructionBase)(nil)
	return any(ix.innerInstructions).([]ixparser.ParsableIxBase)
}

func (ix *insertableInstruction) AddEvent(event ixparser.Event) {
	ix.events = append(ix.events, event)
}

type insertableTransaction struct {
	signature string
	timestamp int64
	slot      int64
	err       bool
	errMsg    string

	logs      []string
	addresses []string

	ixs []*insertableInstruction
}

func (tx *insertableTransaction) Instructions() iter.Seq2[int, ixparser.ParsableIx] {
	// compiler check if insertableInstructionBase implements ixparser.ParsableIxBase
	var _ ixparser.ParsableIx = (*insertableInstruction)(nil)
	return slices.All(any(tx.ixs).([]ixparser.ParsableIx))
}

func (tx *insertableTransaction) Logs() iter.Seq2[int, string] {
	return slices.All(tx.logs)
}

func newInsertableTransaction(tx *rpc.ParsedTransactionResult) (*insertableTransaction, error) {
	itx := &insertableTransaction{
		signature: tx.Transaction.Signatures[0],
		timestamp: tx.BlockTime,
		slot:      int64(tx.Slot),
		err:       tx.Meta.Err == nil,

		logs:      make([]string, 0),
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
			hexEncdoded := hex.EncodeToString(bytes)
			itx.logs = append(itx.logs, hexEncdoded)
		}
	}

	for _, ix := range tx.Transaction.Message.Instructions {
		insertableIx, err := newInsertableInstructionBase(ix, itx.addresses, false)
		if err != nil {
			return nil, err
		}
		itx.ixs = append(itx.ixs, &insertableInstruction{
			insertableInstructionBase: insertableIx,
			innerInstructions:         make([]*insertableInstructionBase, 0),
		})
	}
	for _, innerIxs := range tx.Meta.InnerInstructions {
		ix := itx.ixs[int(innerIxs.IxIndex)]
		for _, innerIx := range innerIxs.Instructions {
			insertableInnerIx, err := newInsertableInstructionBase(innerIx, itx.addresses, true)
			if err != nil {
				return nil, err
			}
			ix.innerInstructions = append(
				ix.innerInstructions,
				insertableInnerIx,
			)
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

func prepareInsertTransactionsQuery(txs []*insertableTransaction) *preparedQuery {
	pq := newPreparedQuery(
		"INSERT INTO \"transaction\" (signature, timestamp, slot, err, err_msg) VALUES",
		"ON CONFLICT (signature) DO NOTHING RETURNING signature, id",
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

func insertTransactions(db *sql.DB, txs []*insertableTransaction) error {
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
		if err = rows.Scan(&t.Signature, &t.Id); err != nil {
			return err
		}
		insertedTxs = append(insertedTxs, t)
	}

	accountsPreparedQuery := newPreparedQuery(
		"INSERT INTO transaction_account (transaction_id, address, idx) VALUES",
		"",
	)
	logsPreparedQuery := newPreparedQuery(
		"INSERT INTO transaction_log (transaction_id, log, idx) VALUES",
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
			ixAccountsIdxs, err := json.Marshal(ix.accountsIndexes)
			if err != nil {
				return err
			}
			ixData := hex.EncodeToString(ix.data)

			ixsPreparedQuery.placeholders = append(ixsPreparedQuery.placeholders, "(?, ?, ?, ?, ?)")
			ixsPreparedQuery.args = append(ixsPreparedQuery.args, txId, ixIdx, ix.programIdIdx, string(ixAccountsIdxs), ixData)

			for innerIxIdx, innerIx := range ix.innerInstructions {
				iixAccountsIdxs, err := json.Marshal(innerIx.accountsIndexes)
				if err != nil {
					return err
				}
				innerIxData := hex.EncodeToString(innerIx.data)

				innerIxsPreparedQuery.placeholders = append(innerIxsPreparedQuery.placeholders, "(?, ?, ?, ?, ?, ?)")
				innerIxsPreparedQuery.args = append(
					innerIxsPreparedQuery.args,
					txId,
					ixIdx,
					innerIxIdx,
					innerIx.programIdIdx,
					string(iixAccountsIdxs),
					innerIxData,
				)
			}
		}
	}

	var eg errgroup.Group
	if len(accountsPreparedQuery.args) > 0 {
		eg.TryGo(func() error {
			_, err := tx.Exec(accountsPreparedQuery.string(), accountsPreparedQuery.args...)
			return err
		})
	}
	if len(logsPreparedQuery.args) > 0 {
		eg.TryGo(func() error {
			_, err := tx.Exec(logsPreparedQuery.string(), logsPreparedQuery.args...)
			return err
		})
	}
	eg.TryGo(func() error {
		if len(ixsPreparedQuery.args) > 0 {
			_, err := tx.Exec(ixsPreparedQuery.string(), ixsPreparedQuery.args...)
			if err != nil {
				return err
			}
			if len(innerIxsPreparedQuery.args) > 0 {
				_, err = tx.Exec(innerIxsPreparedQuery.string(), innerIxsPreparedQuery.args...)
				return err
			}
		}
		return nil
	})
	if err = eg.Wait(); err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

type savedInstructionBase struct {
	programAddress string
	accounts       []string
	data           []byte
}

func (ix *savedInstructionBase) ProgramAddress() string {
	return ix.programAddress
}

func (ix *savedInstructionBase) AccountsAddresses() []string {
	return ix.accounts
}

func (ix *savedInstructionBase) Data() []byte {
	return ix.data
}

type savedInstruction struct {
	*savedInstructionBase
	innerInstructions []*savedInstructionBase
}

func (ix *savedInstruction) InnerIxs() []ixparser.ParsableIxBase {
	var _ ixparser.ParsableIxBase = (*savedInstruction)(nil)
	return any(ix.innerInstructions).([]ixparser.ParsableIxBase)
}

func (ix *savedInstruction) AddEvent(_ ixparser.Event) {}

type savedTransaction struct {
	id           int64
	logs         []string
	instructions []*savedInstruction
}

func (tx *savedTransaction) Instructions() iter.Seq2[int, ixparser.ParsableIx] {
	var _ ixparser.ParsableIx = (*savedInstruction)(nil)
	return slices.All(any(tx.instructions).([]ixparser.ParsableIx))
}

func (tx *savedTransaction) Logs() iter.Seq2[int, string] {
	return slices.All(tx.logs)
}

type savedInstructionBaseSerialized struct {
	ProgramAddress string `json:"program_address"`
	AccountsIdxs   string `json:"accounts_idxs"`
	Data           string
}

func deserializeIxAccountsAndData(
	txAccounts []string,
	accountsIdxsSerialized string,
	dataSerialized string,
) ([]string, []byte, error) {
	var accountsIdxs []int
	if err := json.Unmarshal([]byte(accountsIdxsSerialized), &accountsIdxs); err != nil {
		return nil, nil, err
	}
	accountsAddresses := make([]string, len(accountsIdxs))
	for i, idx := range accountsIdxs {
		accountsAddresses[i] = txAccounts[idx]
	}
	ixData, err := hex.DecodeString(dataSerialized)
	if err != nil {
		return nil, nil, err
	}
	return accountsAddresses, ixData, nil
}

func deserializeSavedTransaction(tx *dbgen.VTransaction) (*savedTransaction, error) {
	var txAccounts []string
	if tx.Accounts != nil {
		if err := json.Unmarshal([]byte(tx.Accounts.(string)), &txAccounts); err != nil {
			return nil, err
		}
	}

	var logs []string
	if tx.Logs != nil {
		if err := json.Unmarshal([]byte(tx.Logs.(string)), &logs); err != nil {
			return nil, err
		}
	}

	var ixs []*dbgen.VInstruction
	if err := json.Unmarshal([]byte(tx.Instructions.(string)), &ixs); err != nil {
		return nil, err
	}
	savedIxs := make([]*savedInstruction, len(ixs))
	for ixIdx, ix := range ixs {
		var innerIxs []*dbgen.VInnerInstruction
		if err := json.Unmarshal([]byte(ix.InnerIxs.(string)), &innerIxs); err != nil {
			return nil, err
		}
		savedInnerIxs := make([]*savedInstructionBase, len(innerIxs))
		for i, iix := range innerIxs {
			accountsAddresses, iixData, err := deserializeIxAccountsAndData(txAccounts, iix.AccountsIdxs, iix.Data)
			if err != nil {
				return nil, err
			}
			savedInnerIxs[i] = &savedInstructionBase{
				programAddress: iix.ProgramAddress,
				accounts:       accountsAddresses,
				data:           iixData,
			}
		}

		accountsAddresses, ixData, err := deserializeIxAccountsAndData(txAccounts, ix.AccountsIdxs, ix.Data)
		if err != nil {
			return nil, err
		}
		savedIxs[ixIdx] = &savedInstruction{
			savedInstructionBase: &savedInstructionBase{
				programAddress: ix.ProgramAddress,
				accounts:       accountsAddresses,
				data:           ixData,
			},
			innerInstructions: savedInnerIxs,
		}
	}

	return &savedTransaction{
		id:           tx.ID,
		logs:         logs,
		instructions: savedIxs,
	}, nil
}

func fixTimestampDuplicates(rpcClient *rpc.Client, db *sql.DB, q *dbgen.Queries) error {
	txs, err := q.FetchDuplicateTimestampsTransactions(context.Background())
	if err != nil {
		return err
	}
	if len(txs) == 0 {
		return nil
	}

	slots := make(map[int64][]string)
	for _, tx := range txs {
		_, ok := slots[tx.Slot]
		if !ok {
			slots[tx.Slot] = []string{tx.Signature}
		} else {
			slots[tx.Slot] = append(slots[tx.Slot], tx.Signature)
		}
	}

	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	for slot, slotSignatures := range slots {
		block, err := rpcClient.GetBlock(uint64(slot), rpc.CommitmentConfirmed)
		if err != nil {
			return err
		}
		for _, signature := range slotSignatures {
			blockIndex := slices.IndexFunc(block.Signatures, func(s string) bool {
				return s == signature
			})
			if blockIndex == -1 {
				return fmt.Errorf("unable to find signature on block")
			}
			placeholders = append(placeholders, "(?,?)")
			args = append(args, blockIndex, signature)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("CREATE TEMP TABLE bi_updates (bi INTEGER, sig TEXT)"); err != nil {
		return err
	}

	insertQuery := strings.Builder{}
	insertQuery.WriteString("INSERT INTO bi_updates (bi, sig) VALUES ")
	insertQuery.WriteString(strings.Join(placeholders, ","))
	if _, err = tx.Exec(insertQuery.String(), args...); err != nil {
		return err
	}

	updateQuery := `
		UPDATE
			"transaction"
		SET
			block_index = (
				SELECT
					bi
				FROM
					bi_updates
				WHERE
					sig = signature
			)
		WHERE
			signature IN (SELECT sig FROM bi_updates)
	`
	if _, err = tx.Exec(updateQuery); err != nil {
		return err
	}
	if _, err = tx.Exec("DROP TABLE bi_updates"); err != nil {
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
		assert.NoErr(err, "unable to fetch signatures")
		slog.Info("fetched signatures for wallet", "signatures", len(signaturesResults))

		if len(signaturesResults) == 0 {
			break
		}

		signatures := make([]string, len(signaturesResults))
		for i, sr := range signaturesResults {
			signatures[i] = sr.Signature
		}
		savedTransactions, err := q.FetchTransactions(context.Background(), signatures)
		assert.NoErr(err, "unable to fetch saved transactions")
		for _, tx := range savedTransactions {
			deserializedSavedTx, err := deserializeSavedTransaction(tx)
			assert.NoErr(err, "unable to deserialize saved tx", "tx", tx)
			_ = ixparser.ParseTx(deserializedSavedTx)
		}

		insertableTransactions := make([]*insertableTransaction, 0)
		for _, sr := range signaturesResults {
			if txIdx := slices.IndexFunc(savedTransactions, func(tx *dbgen.VTransaction) bool {
				return tx.Signature == sr.Signature
			}); txIdx > -1 {
				continue
			}

			slog.Info("fetching tx", "signature", sr.Signature)
			tx, err := forceRpcRequest(func() (*rpc.ParsedTransactionResult, error) {
				return rpcClient.GetTransaction(sr.Signature, rpc.CommitmentConfirmed)
			}, 5)
			assert.NoErr(err, "unable to get transaction")

			insertableTx, err := newInsertableTransaction(tx)
			assert.NoErr(err, "unable to create insertable tx")

			_ = ixparser.ParseTx(insertableTx)

			insertableTransactions = append(insertableTransactions, insertableTx)
		}

		if len(insertableTransactions) > 0 {
			err = insertTransactions(db, insertableTransactions)
			assert.NoErr(err, "unable to insert transactions")
		}

		if len(signatures) < int(l) {
			break
		}
		getSignaturesConfig.Before = signatures[0]
	}

	err := fixTimestampDuplicates(rpcClient, db, q)
	assert.NoErr(err, "unbale to fix timestamps duplicates")
}
