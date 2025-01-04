package fetcher

import (
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
	dbutils "taxemon/pkg/db_utils"
	ixparser "taxemon/pkg/ix_parser"
	"taxemon/pkg/rpc"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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
	programIdIdx      int16
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
		programIdIdx:      int16(ix.ProgramIdIndex),
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
	timestamp time.Time
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
		timestamp: time.Unix(tx.BlockTime, 0),
		slot:      int64(tx.Slot),
		err:       tx.Meta.Err != nil,

		logs:      make([]string, 0),
		addresses: make([]string, 0),

		ixs: make([]*insertableInstruction, 0),
	}
	if itx.err {
		if errSerialized, err := json.Marshal(tx.Meta.Err); err == nil {
			itx.errMsg = string(errSerialized)
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

func insertTransactions(db *sqlx.DB, txs []*insertableTransaction) []*dbutils.InsertTransactionsRow {
	tx, err := db.Beginx()
	assert.NoErr(err, "unable to begin tx")

	insertTransactionsParams := make([]*dbutils.InsertTransactionParams, len(txs))
	for i, tx := range txs {
		insertableTx := dbutils.InsertTransactionParams{
			Signature: tx.signature,
			Timestamp: tx.timestamp,
			Slot:      tx.slot,
			Err:       tx.err,
			Accounts:  tx.addresses,
			Logs:      tx.logs,
		}
		if tx.err {
			insertableTx.ErrMsg.Valid = true
			insertableTx.ErrMsg.String = tx.errMsg
		}
		insertTransactionsParams[i] = &insertableTx
	}
	insertedTxs, err := dbutils.InsertTransactions(tx, insertTransactionsParams)
	assert.NoErr(err, "unable to insert transactions")

	insertInstructionsParams := make([]*dbutils.InsertInstructionParams, 0)
	insertInnerInstructionsParams := make([]*dbutils.InsertInnerInstructionParams, 0)
	insertEventsParams := make([]*dbutils.InsertEventParams, 0)

	for _, tx := range txs {
		txIdIdx := slices.IndexFunc(insertedTxs, func(itx *dbutils.InsertTransactionsRow) bool {
			return itx.Signature == tx.signature
		})
		assert.True(txIdIdx > -1, "unable to find tx id idx")
		txId := insertedTxs[txIdIdx].Id

		for i, ix := range tx.ixs {
			insertInstructionsParams = append(insertInstructionsParams, &dbutils.InsertInstructionParams{
				TransactionId: txId,
				Idx:           int32(i),
				IsKnown:       ix.isKnown,
				ProgramIdIdx:  ix.programIdIdx,
				AccountsIdxs:  pq.GenericArray{A: ix.accountsIndexes},
				Data:          hex.EncodeToString(ix.data),
			})

			for j, innerIx := range ix.innerInstructions {
				insertInnerInstructionsParams = append(insertInnerInstructionsParams, &dbutils.InsertInnerInstructionParams{
					TransactionId: txId,
					IxIdx:         int32(i),
					Idx:           int16(j),
					ProgramIdIdx:  innerIx.programIdIdx,
					AccountsIdxs:  pq.GenericArray{A: innerIx.accountsIndexes},
					Data:          hex.EncodeToString(innerIx.data),
				})
			}

			for j, event := range ix.events {
				eventSerialized, err := json.Marshal(event)
				assert.NoErr(err, "unable to marshal event data", "event data", event)
				insertEventsParams = append(insertEventsParams, &dbutils.InsertEventParams{
					TransactionId: txId,
					IxIdx:         int32(i),
					Idx:           int16(j),
					Type:          int16(event.Type()),
					Data:          string(eventSerialized),
				})
			}
		}
	}

	if len(insertInstructionsParams) > 0 {
		err = dbutils.InsertInstructions(tx, insertInstructionsParams)
		assert.NoErr(err, "unable to insert instructions")

		var eg errgroup.Group
		if len(insertInnerInstructionsParams) > 0 {
			eg.TryGo(func() error {
				if err := dbutils.InsertInnerInstructions(tx, insertInnerInstructionsParams); err != nil {
					return fmt.Errorf("unable to insert inner instructions: %w", err)
				}
				return nil
			})
		}
		if len(insertEventsParams) > 0 {
			eg.TryGo(func() error {
				if err := dbutils.InsertEvents(tx, insertEventsParams); err != nil {
					return fmt.Errorf("unable to insert events: %w", err)
				}
				return nil
			})
		}
		err = eg.Wait()
		assert.NoErr(err, "")
	}

	err = tx.Commit()
	assert.NoErr(err, "unable to commit")
	return insertedTxs
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
	id           int32
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

func newSavedInstructionBase(ix *dbutils.SelectTransactionInstructionBase, tx *dbutils.SelectTransactionsRow) *savedInstructionBase {
	assert.True(int(ix.ProgramIdIdx) < len(tx.Accounts), "program id idx overflow", tx.Accounts, ix.ProgramIdIdx)
	accounts := make([]string, len(ix.AccountsIdxs))
	for i, aIdx := range ix.AccountsIdxs {
		assert.True(int(aIdx) < len(tx.Accounts), "account idx overflow", tx.Accounts, aIdx)
		accounts[i] = tx.Accounts[aIdx]
	}
	data, err := hex.DecodeString(ix.Data)
	assert.NoErr(err, "invalid data", "data", ix.Data)
	savedIxBase := &savedInstructionBase{
		programAddress: tx.Accounts[ix.ProgramIdIdx],
		accounts:       accounts,
		data:           data,
	}
	return savedIxBase
}

func DeserializeSavedTransaction(tx *dbutils.SelectTransactionsRow) *savedTransaction {
	var instructions []*dbutils.SelectTransactionInstruction
	err := json.Unmarshal(tx.Instructions, &instructions)
	assert.NoErr(err, "unable to unmarshal tx row instructions", "instructions", tx.Instructions)

	savedIxs := make([]*savedInstruction, len(instructions))
	for i, ix := range instructions {
		savedIx := savedInstruction{
			savedInstructionBase: newSavedInstructionBase(&dbutils.SelectTransactionInstructionBase{
				ProgramIdIdx: ix.ProgramIdIdx,
				AccountsIdxs: ix.AccountsIdxs,
				Data:         ix.Data,
			}, tx),
			innerInstructions: make([]*savedInstructionBase, len(ix.InnerInstructions)),
		}
		for j, iix := range ix.InnerInstructions {
			savedIx.innerInstructions[j] = newSavedInstructionBase(iix, tx)
		}
		savedIxs[i] = &savedIx
	}

	deserializedTx := savedTransaction{
		signature:    tx.Signature,
		id:           tx.Id,
		logs:         tx.Logs,
		instructions: savedIxs,
	}
	return &deserializedTx
}

func fixTimestampDuplicates(rpcClient *rpc.Client, db *sqlx.DB) error {
	txs, err := dbutils.SelectDuplicateTimestampsTransactions(db)
	assert.NoErr(err, "unable to select txs with duplicate timestamps")
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

	signatures := make([]string, 0)
	blockIndexes := make([]int32, 0)

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
			signatures = append(signatures, signature)
			blockIndexes = append(blockIndexes, int32(blockIndex))
		}
	}

	err = dbutils.UpdateTransactionsBlockIndexes(db, signatures, blockIndexes)
	assert.NoErr(err, "unable to update transactions block indexes")
	return nil
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

func (a *AssociatedAccounts) init(db *sqlx.DB) error {
	associatedAccounts, err := dbutils.SelectAssociatedAccounts(db)
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
	db        *sqlx.DB
	config    *rpc.GetSignaturesForAddressConfig

	associatedAccounts *AssociatedAccounts
	address            string
	latestSignature    string
}

func newFetcher(
	rpcClient *rpc.Client,
	db *sqlx.DB,
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
		db:                 db,
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
	savedTransactions, err := dbutils.SelectTransactions(f.db, signatures)
	assert.NoErr(err, "unable to fetch saved transactions")

	if f.associatedAccounts != nil {
		for _, tx := range savedTransactions {
			deserializedSavedTx := DeserializeSavedTransaction(tx)
			assert.NoErr(err, "unable to deserialize saved tx", "tx", tx)

			currentAssociatedAccounts, err := ixparser.ParseTx(deserializedSavedTx, f.address)
			assert.NoErr(err, "unable to parse tx")
			maps.Insert(f.associatedAccounts.currentIter, maps.All(currentAssociatedAccounts))
		}
	}

	insertableTransactions := make([]*insertableTransaction, 0)
	for _, sr := range signaturesResults {
		if txIdx := slices.IndexFunc(savedTransactions, func(tx *dbutils.SelectTransactionsRow) bool {
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

func syncAddress(
	rpcClient *rpc.Client,
	db *sqlx.DB,
	associatedAccounts *AssociatedAccounts,
	walletId int32,
	address string,
	lastSignature string,
) {
	slog.Info("running address sync", "address", address)
	fetcher := newFetcher(rpcClient, db, associatedAccounts, address, lastSignature)

	for {
		hasNext, insertableTransactions := fetcher.fetchNext()
		if insertableTransactions == nil {
			break
		}

		if len(insertableTransactions) > 0 {
			slog.Info("inserting transactions")
			insertedTxs := insertTransactions(db, insertableTransactions)

			var dbtx dbutils.DBTX = db
			if associatedAccounts != nil {
				insertableAccounts := associatedAccounts.flush()
				if insertableAccounts != nil {
					var err error
					dbtx, err = db.Beginx()
					assert.NoErr(err, "unable to begin tx")

					slog.Info("inserting associated accounts")
					params := make([]*dbutils.InsertAssociatedAccountParams, len(insertableAccounts))
					for i, aa := range insertableAccounts {
						data, err := aa.Data()
						assert.NoErr(err, "unable to serialize associated account data")

						params[i] = &dbutils.InsertAssociatedAccountParams{
							Address: aa.Address(),
							Type:    int16(aa.Type()),
							Data:    string(data),
						}
					}
					err = dbutils.InsertAssociatedAccounts(dbtx, params)
					assert.NoErr(err, "unable to insert associated accounts")
				}
			}

			slog.Info("inserting transactions to wallet")
			transactionsIds := make([]int32, len(insertedTxs))
			for i, tx := range insertedTxs {
				transactionsIds[i] = tx.Id
			}
			err := dbutils.InsertTransactionsToWallet(dbtx, walletId, transactionsIds)
			assert.NoErr(err, "unable to insert transactions_to_wallet")

			switch tx := dbtx.(type) {
			case *sqlx.Tx:
				err = tx.Commit()
				assert.NoErr(err, "unable to commit")
			}
		}

		if !hasNext {
			break
		}
	}

	if fetcher.latestSignature == "" {
		return
	}

	err := dbutils.UpdateLastSignature(db, associatedAccounts == nil, address, fetcher.latestSignature)
	assert.NoErr(err, "unable to update wallet or associated account")
}

func SyncWallet(
	rpcClient *rpc.Client,
	db *sqlx.DB,
	walletId int32,
	walletAddress string,
	lastSigature string,
) error {
	associatedAccounts := newAssociatedAccounts()
	associatedAccounts.init(db)

	slog.Info("syncing wallet", "walletAddress", walletAddress)
	syncAddress(rpcClient, db, associatedAccounts, walletId, walletAddress, lastSigature)

	for address, lastSignature := range associatedAccounts.all {
		slog.Info("syncing associated account", "address", address)
		syncAddress(rpcClient, db, nil, walletId, address, lastSignature)
	}

	return fixTimestampDuplicates(rpcClient, db)
}

func Start(
	rpcClient *rpc.Client,
	db *sqlx.DB,
) {
	ticker := time.NewTicker(5 * time.Second)

outer:
	for {
		select {
		case <-ticker.C:
			syncRequest, err := dbutils.GetLatestSyncRequest(db)
			if errors.Is(err, sql.ErrNoRows) {
				continue outer
			}
			assert.NoErr(err, "unable to get latest sync request")

			err = SyncWallet(
				rpcClient,
				db,
				syncRequest.WalletId,
				syncRequest.Address,
				syncRequest.LastSignature.String,
			)
			assert.NoErr(err, "unable to sync wallet", "sync request", syncRequest)

			err = dbutils.UpdateSyncRequestStatus(db, syncRequest.WalletId, dbutils.SyncRequestStatusParsing)
			assert.NoErr(err, "unable to update sync request status")

			slog.Info("wallet syncing finished", "address", syncRequest.Address)
		}
	}
}
