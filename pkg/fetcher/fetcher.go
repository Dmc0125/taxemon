package fetcher

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"taxemon/pkg/assert"
	"taxemon/pkg/dbgen"
	ixparser "taxemon/pkg/ix_parser"
	"taxemon/pkg/rpc"
	"time"

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
	isKnown           bool
}

func (ix *insertableInstruction) InnerIxs() []ixparser.ParsableIxBase {
	iixs := make([]ixparser.ParsableIxBase, len(ix.innerInstructions))
	for i, iix := range ix.innerInstructions {
		iixs[i] = iix
	}
	return iixs
}

func (ix *insertableInstruction) AddEvent(event ixparser.Event) {
	ix.events = append(ix.events, event)
}

func (ix *insertableInstruction) SetKnown() {
	ix.isKnown = true
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

func (tx *insertableTransaction) Signature() string {
	return tx.signature
}

func (tx *insertableTransaction) Instructions() iter.Seq2[int, ixparser.ParsableIx] {
	ixs := make([]ixparser.ParsableIx, len(tx.ixs))
	for i, ix := range tx.ixs {
		ixs[i] = ix
	}
	return slices.All(ixs)
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

func insertTransactions(db *sql.DB, txs []*insertableTransaction) ([]*insertedTransaction, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	txsPreparedQuery := prepareInsertTransactionsQuery(txs)
	rows, err := tx.Query(txsPreparedQuery.string(), txsPreparedQuery.args...)
	if err != nil {
		return nil, err
	}

	insertedTxs := make([]*insertedTransaction, 0)
	for rows.Next() {
		t := new(insertedTransaction)
		if err = rows.Scan(&t.Signature, &t.Id); err != nil {
			return nil, err
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
		"INSERT INTO instruction (transaction_id, idx, is_known, program_id_idx, accounts_idxs, data) VALUES",
		"",
	)
	innerIxsPreparedQuery := newPreparedQuery(
		"INSERT INTO inner_instruction (transaction_id, ix_idx, idx, program_id_idx, accounts_idxs, data) VALUES",
		"",
	)
	eventsPreparedQuery := newPreparedQuery(
		"INSERT INTO event (transaction_id, ix_idx, idx, type, data) VALUES",
		"",
	)

	for _, tx := range txs {
		txIdIdx := slices.IndexFunc(insertedTxs, func(itx *insertedTransaction) bool {
			return itx.Signature == tx.signature
		})
		if txIdIdx == -1 {
			return nil, fmt.Errorf("unable to find inserted tx")
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
				return nil, err
			}
			ixData := hex.EncodeToString(ix.data)

			ixsPreparedQuery.placeholders = append(ixsPreparedQuery.placeholders, "(?, ?, ?, ?, ?, ?)")
			ixsPreparedQuery.args = append(ixsPreparedQuery.args, txId, ixIdx, ix.isKnown, ix.programIdIdx, string(ixAccountsIdxs), ixData)

			for innerIxIdx, innerIx := range ix.innerInstructions {
				iixAccountsIdxs, err := json.Marshal(innerIx.accountsIndexes)
				if err != nil {
					return nil, err
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

			for eventIdx, event := range ix.events {
				eventSerialized, err := json.Marshal(event)
				if err != nil {
					return nil, err
				}
				eventsPreparedQuery.placeholders = append(eventsPreparedQuery.placeholders, "(?, ?, ?, ?, ?)")
				eventsPreparedQuery.args = append(eventsPreparedQuery.args, txId, ixIdx, eventIdx, event.Type(), string(eventSerialized))
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
	if len(ixsPreparedQuery.args) > 0 {
		eg.TryGo(func() error {
			if _, err := tx.Exec(ixsPreparedQuery.string(), ixsPreparedQuery.args...); err != nil {
				return err
			}
			if len(innerIxsPreparedQuery.args) > 0 {
				eg.TryGo(func() error {
					_, err = tx.Exec(innerIxsPreparedQuery.string(), innerIxsPreparedQuery.args...)
					return err
				})
			}
			if len(eventsPreparedQuery.args) > 0 {
				eg.TryGo(func() error {
					_, err := tx.Exec(eventsPreparedQuery.string(), eventsPreparedQuery.args...)
					return err
				})
			}
			return nil
		})
	}
	if err = eg.Wait(); err != nil {
		return nil, err
	}

	err = tx.Commit()
	return insertedTxs, err
}

func insertAssociatedAccounts(db dbgen.DBTX, associatedAccounts []ixparser.AssociatedAccount) error {
	q := newPreparedQuery("INSERT INTO associated_account (address, type, data) VALUES", "")
	for _, account := range associatedAccounts {
		data, err := account.Data()
		if err != nil {
			return err
		}

		q.placeholders = append(q.placeholders, "(?, ?, ?)")
		q.args = append(q.args, account.Address(), account.Type(), string(data))
	}
	_, err := db.ExecContext(context.Background(), q.string(), q.args...)
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
	iixs := make([]ixparser.ParsableIxBase, len(ix.innerInstructions))
	for i, iix := range ix.innerInstructions {
		iixs[i] = iix
	}
	return iixs
}

func (ix *savedInstruction) AddEvent(_ ixparser.Event) {}

func (ix *savedInstruction) SetKnown() {}

type savedTransaction struct {
	signature    string
	id           int64
	logs         []string
	instructions []*savedInstruction
}

func (tx *savedTransaction) Signature() string {
	return tx.signature
}

func (tx *savedTransaction) Instructions() iter.Seq2[int, ixparser.ParsableIx] {
	ixs := make([]ixparser.ParsableIx, len(tx.instructions))
	for i, ix := range tx.instructions {
		ixs[i] = ix
	}
	return slices.All(ixs)
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

func DeserializeSavedTransaction(tx *dbgen.VTransaction) (*savedTransaction, error) {
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
		signature:    tx.Signature,
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

type AssociatedAccounts struct {
	currentIter map[string]ixparser.AssociatedAccount
	all         map[string]string
}

func newAssociatedAccounts() *AssociatedAccounts {
	return &AssociatedAccounts{
		currentIter: make(map[string]ixparser.AssociatedAccount),
		all:         make(map[string]string),
	}
}

func (a *AssociatedAccounts) init(q *dbgen.Queries) error {
	associatedAccounts, err := q.FetchAssociatedAccounts(context.Background())
	if err != nil {
		return err
	}
	for _, aa := range associatedAccounts {
		a.all[aa.Address] = aa.LastSignature.String
	}
	return nil
}

func (a *AssociatedAccounts) flush() []ixparser.AssociatedAccount {
	if len(a.currentIter) == 0 {
		return nil
	}

	new := make([]ixparser.AssociatedAccount, 0)
	for address, account := range a.currentIter {
		_, ok := a.all[address]
		if !ok {
			new = append(new, account)
			a.all[address] = ""
		}
	}
	a.currentIter = make(map[string]ixparser.AssociatedAccount)
	return new
}

type Fetcher struct {
	rpcClient *rpc.Client
	q         *dbgen.Queries
	config    *rpc.GetSignaturesForAddressConfig

	associatedAccounts *AssociatedAccounts
	address            string
	latestSignature    string
}

func newFetcher(
	rpcClient *rpc.Client,
	q *dbgen.Queries,
	associatedAccounts *AssociatedAccounts,
	address, lastSignature string,
) *Fetcher {
	l := uint64(1000)
	c := &rpc.GetSignaturesForAddressConfig{
		Limit:      &l,
		Commitment: rpc.CommitmentConfirmed,
	}
	if len(lastSignature) > 0 {
		c.After = lastSignature
	}
	return &Fetcher{
		rpcClient:          rpcClient,
		q:                  q,
		config:             c,
		associatedAccounts: associatedAccounts,
		address:            address,
	}
}

func (f *Fetcher) fetchNext() (bool, []*insertableTransaction) {
	signaturesResults, err := forceRpcRequest(func() ([]*rpc.SignatureResult, error) {
		return f.rpcClient.GetSignaturesForAddress(f.address, f.config)
	}, 5)
	assert.NoErr(err, "unable to fetch signatures")
	slog.Info("fetched signatures for address", "signatures", len(signaturesResults))

	if len(signaturesResults) == 0 {
		return false, nil
	}
	if f.latestSignature == "" {
		f.latestSignature = signaturesResults[0].Signature
	}

	signatures := make([]string, len(signaturesResults))
	for i, sr := range signaturesResults {
		signatures[i] = sr.Signature
	}
	savedTransactions, err := f.q.FetchTransactions(context.Background(), signatures)
	assert.NoErr(err, "unable to fetch saved transactions")

	if f.associatedAccounts != nil {
		slog.Info("should not run")
		for _, tx := range savedTransactions {
			deserializedSavedTx, err := DeserializeSavedTransaction(tx)
			assert.NoErr(err, "unable to deserialize saved tx", "tx", tx)

			currentAssociatedAccounts, err := ixparser.ParseTx(deserializedSavedTx, f.address)
			assert.NoErr(err, "unable to parse tx")
			maps.Insert(f.associatedAccounts.currentIter, maps.All(currentAssociatedAccounts))
		}
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
			return f.rpcClient.GetTransaction(sr.Signature, rpc.CommitmentConfirmed)
		}, 5)
		assert.NoErr(err, "unable to get transaction")

		insertableTx, err := newInsertableTransaction(tx)
		assert.NoErr(err, "unable to create insertable tx")

		currentAssociatedAccounts, err := ixparser.ParseTx(insertableTx, f.address)
		assert.NoErr(err, "unable to parse tx")

		if f.associatedAccounts != nil {
			maps.Insert(f.associatedAccounts.currentIter, maps.All(currentAssociatedAccounts))
		}

		insertableTransactions = append(insertableTransactions, insertableTx)
	}

	f.config.Before = signatures[len(signatures)-1]
	hasNext := len(signaturesResults) == int(*f.config.Limit)

	return hasNext, insertableTransactions
}

func insertTransactionsToWallet(db dbgen.DBTX, walletId int64, txs []*insertedTransaction) error {
	q := newPreparedQuery("INSERT INTO transaction_to_wallet (wallet_id, transaction_id) VALUES", "ON CONFLICT (wallet_id, transaction_id) DO NOTHING")
	for _, tx := range txs {
		q.placeholders = append(q.placeholders, "(?, ?)")
		q.args = append(q.args, walletId, tx.Id)
	}
	_, err := db.ExecContext(context.Background(), q.string(), q.args...)
	return err
}

func syncAddress(
	rpcClient *rpc.Client,
	db *sql.DB,
	q *dbgen.Queries,
	associatedAccounts *AssociatedAccounts,
	walletId int64,
	address string,
	lastSignature string,
) error {
	slog.Info("running address sync", "address", address)
	fetcher := newFetcher(rpcClient, q, associatedAccounts, address, lastSignature)

	for {
		hasNext, insertableTransactions := fetcher.fetchNext()
		if insertableTransactions == nil {
			break
		}

		if len(insertableTransactions) > 0 {
			slog.Info("inserting transactions")
			insertedTxs, err := insertTransactions(db, insertableTransactions)
			if err != nil {
				return err
			}

			tx, err := db.Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			if associatedAccounts != nil {
				insertableAccounts := associatedAccounts.flush()
				if insertableAccounts != nil {
					slog.Info("inserting associated accounts")
					err = insertAssociatedAccounts(tx, insertableAccounts)
					if err != nil {
						return err
					}
				}
			}
			slog.Info("inserting transactions to wallet")
			if err = insertTransactionsToWallet(tx, walletId, insertedTxs); err != nil {
				return err
			}

			err = tx.Commit()
			if err != nil {
				return err
			}
		}

		if !hasNext {
			break
		}
	}

	if associatedAccounts != nil && fetcher.latestSignature != "" {
		slog.Info("setting last signature")
		return q.SetWalletLastSignature(context.Background(), &dbgen.SetWalletLastSignatureParams{
			Signature: sql.NullString{
				Valid:  true,
				String: fetcher.latestSignature,
			},
			Address: address,
		})
	} else if fetcher.latestSignature != "" {
		slog.Info("setting last signature")
		return q.SetAssociatedAccountLastSignature(context.Background(), &dbgen.SetAssociatedAccountLastSignatureParams{
			Signature: sql.NullString{
				Valid:  true,
				String: fetcher.latestSignature,
			},
			Address: address,
		})
	}

	return nil
}

func SyncWallet(
	rpcClient *rpc.Client,
	db *sql.DB,
	q *dbgen.Queries,
	walletId int64,
	walletAddress string,
	lastSigature string,
) error {
	associatedAccounts := newAssociatedAccounts()
	associatedAccounts.init(q)

	slog.Info("syncing wallet", "walletAddress", walletAddress)
	err := syncAddress(rpcClient, db, q, associatedAccounts, walletId, walletAddress, lastSigature)
	if err != nil {
		return err
	}

	for address, lastSignature := range associatedAccounts.all {
		slog.Info("syncing associated account", "address", address)
		err = syncAddress(rpcClient, db, q, nil, walletId, address, lastSignature)
		if err != nil {
			return err
		}
	}

	return fixTimestampDuplicates(rpcClient, db, q)
}

func Start(
	rpcClient *rpc.Client,
	db *sql.DB,
	q *dbgen.Queries,
) error {
	ticker := time.NewTicker(5 * time.Second)

outer:
	for {
		select {
		case <-ticker.C:
			syncRequest, err := q.GetLatestSyncRequest(context.Background())
			if errors.Is(err, sql.ErrNoRows) {
				continue outer
			}
			if err != nil {
				return err
			}

			if err = SyncWallet(
				rpcClient,
				db,
				q,
				syncRequest.WalletID,
				syncRequest.Address,
				syncRequest.LastSignature.String,
			); err != nil {
				return err
			}
		}
	}
}
