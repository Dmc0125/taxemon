package dbutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type DBTX interface {
	Select(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...any) (sql.Result, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	Get(dest interface{}, query string, args ...interface{}) error
}

type SelectTransactionsRow struct {
	Id           int32
	Signature    string
	Accounts     pq.StringArray
	Logs         pq.StringArray
	Instructions json.RawMessage
	Err          bool
}

func SelectTransactionsFromSignatures(db DBTX, signatures []string) ([]*SelectTransactionsRow, error) {
	result := make([]*SelectTransactionsRow, 0)
	q := `
		select
			t.id,
			t.signature,
			t.accounts,
			t.logs,
			t.err,
			get_instructions(t.id) as instructions
		from
			"transaction" t
		where
			t.signature = any($1)
	`
	err := db.Select(&result, q, pq.StringArray(signatures))
	return result, err
}

type SelectTransactionInstructionBase struct {
	ProgramIdIdx int16   `json:"program_id_idx"`
	AccountsIdxs []int16 `json:"accounts_idxs"`
	Data         string
}

type SelectTransactionInstruction struct {
	*SelectTransactionInstructionBase
	Idx               int32
	InnerInstructions []*SelectTransactionInstructionBase `json:"inner_ixs"`
}

type SelectOrderedTransactionsRow struct {
	*SelectTransactionsRow
	Slot       int64
	BlockIndex sql.NullInt32 `db:"block_index"`
}

func SelectOrderedTransactions(
	db DBTX,
	fromSlot int64,
	fromBlockIndex int32,
	limit int,
) ([]*SelectOrderedTransactionsRow, error) {
	result := make([]*SelectOrderedTransactionsRow, 0)
	q := fmt.Sprintf(`
		select
			t.id,
			t.slot,
			t.block_index,
			t.signature,
			t.accounts,
			t.logs,
			get_instructions(t.id) as instructions
		from
			"transaction" t
		where
			((t.block_index is not null and t.block_index > $2 and t.slot >= $1)
			or t.slot > $1)
			and t.err = false
		order by
			t.slot asc, t.block_index asc
		limit
			%d
	`, limit)
	err := db.Select(&result, q, fromSlot, fromBlockIndex)
	return result, err
}

type InsertTransactionParams struct {
	Signature string
	Timestamp time.Time
	Slot      int64
	Err       bool
	ErrMsg    sql.NullString `db:"err_msg"`
	Accounts  pq.StringArray
	Logs      pq.StringArray
}

type InsertTransactionsRow struct {
	Id        int32
	Signature string
}

func InsertTransactions(db DBTX, params []*InsertTransactionParams) ([]*InsertTransactionsRow, error) {
	q := `
		insert into
			"transaction" (signature, timestamp, slot, err, err_msg, accounts, logs)
		values
			(:signature, :timestamp, :slot, :err, :err_msg, :accounts, :logs)
		on conflict (
			signature
		) do nothing
		returning
			id, signature

	`
	rows, err := db.NamedQuery(q, params)
	if err != nil {
		return nil, err
	}
	result := make([]*InsertTransactionsRow, 0)
	for rows.Next() {
		r := new(InsertTransactionsRow)
		if err = rows.Scan(&r.Id, &r.Signature); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, err
}

type InsertInstructionParams struct {
	TransactionId int32 `db:"transaction_id"`
	Idx           int32
	ProgramIdIdx  int16           `db:"program_id_idx"`
	AccountsIdxs  pq.GenericArray `db:"accounts_idxs"`
	Data          string
}

func InsertInstructions(db DBTX, params []*InsertInstructionParams) error {
	q := `
		insert into
			instruction (transaction_id, idx, program_id_idx, accounts_idxs, data)
		values
			(:transaction_id, :idx, :program_id_idx, :accounts_idxs, :data)
	`
	_, err := db.NamedExec(q, params)
	return err
}

type InsertInnerInstructionParams struct {
	TransactionId int32 `db:"transaction_id"`
	IxIdx         int32 `db:"ix_idx"`
	Idx           int32
	ProgramIdIdx  int16           `db:"program_id_idx"`
	AccountsIdxs  pq.GenericArray `db:"accounts_idxs"`
	Data          string
}

func InsertInnerInstructions(db DBTX, params []*InsertInnerInstructionParams) error {
	q := `
		insert into
			inner_instruction (transaction_id, ix_idx, idx, program_id_idx, accounts_idxs, data)
		values
			(:transaction_id, :ix_idx, :idx, :program_id_idx, :accounts_idxs, :data)
	`
	_, err := db.NamedExec(q, params)
	return err
}

type InsertEventParams struct {
	TransactionId int32 `db:"transaction_id"`
	IxIdx         int32 `db:"ix_idx"`
	Idx           int16
	Type          EventType
	Data          string
}

func InsertEvents(db DBTX, params []*InsertEventParams) error {
	q := `
		insert into
			event (transaction_id, ix_idx, idx, type, data)
		values
			(:transaction_id, :ix_idx, :idx, :type, :data)
		on conflict (transaction_id, ix_idx, idx)
		do update set
			idx = EXCLUDED.idx,
			type = EXCLUDED.type,
			data = EXCLUDED.data
	`
	_, err := db.NamedExec(q, params)
	return err
}

func InsertTransactionsToWallet(db DBTX, walletId int32, transactionsIds []int32) error {
	params := make([]map[string]interface{}, 0)
	for _, txId := range transactionsIds {
		params = append(params, map[string]interface{}{
			"wallet_id":      walletId,
			"transaction_id": txId,
		})
	}
	q := `
		insert into
			transaction_to_wallet (wallet_id, transaction_id)
		values
			(:wallet_id, :transaction_id)
		on conflict (wallet_id, transaction_id) do nothing
	`
	_, err := db.NamedExec(q, params)
	return err
}

type AssociatedAccountType string

const (
	AssociatedAccountToken    AssociatedAccountType = "token"
	AssociatedAccountJupLimit AssociatedAccountType = "jup_limit"
)

type InsertAssociatedAccountParams struct {
	WalletId    int32 `db:"wallet_id"`
	Address     string
	Type        AssociatedAccountType
	Data        sql.NullString
	ShouldFetch bool `db:"should_fetch"`
}

func InsertAssociatedAccounts(db DBTX, params []*InsertAssociatedAccountParams) error {
	q := `
		insert into
			associated_account (wallet_id, address, type, data, should_fetch)
		values
			(:wallet_id, :address, :type, :data, :should_fetch)
	`
	_, err := db.NamedExec(q, params)
	return err
}

type GetLatestSyncRequestRow struct {
	WalletId      int32 `db:"wallet_id"`
	Address       string
	LastSignature sql.NullString `db:"last_signature"`
}

func GetLatestSyncRequest(db DBTX) (*GetLatestSyncRequestRow, error) {
	result := new(GetLatestSyncRequestRow)
	q := `
		SELECT
		    sync_request.wallet_id,
		    wallet.address,
		    wallet.last_signature
		FROM
		    sync_request
		    JOIN wallet ON wallet.id = sync_request.wallet_id
		WHERE
			sync_request.status = 'fetching'
		ORDER BY
		    sync_request.created_at
		LIMIT
		    1
	`
	err := db.Get(result, q)
	return result, err
}

type SelectDuplicateTimestampsTransactionsRow struct {
	Slot      int64
	Signature string
}

func SelectDuplicateTimestampsTransactions(db DBTX) ([]*SelectDuplicateTimestampsTransactionsRow, error) {
	result := make([]*SelectDuplicateTimestampsTransactionsRow, 0)
	q := `
		SELECT
		    t1.slot,
		    t1.signature
		FROM
		    "transaction" t1
		    LEFT JOIN "transaction" t2 ON t2.slot = t1.slot
		    AND t2.signature != t1.signature
		WHERE
		    t1.block_index IS NULL
		    AND t2.block_index IS NULL
		    AND t2.id IS NOT NULL
		GROUP BY
		    t1.slot,
		    t1.signature
	`
	err := db.Select(&result, q)
	return result, err
}

func UpdateTransactionsBlockIndexes(db DBTX, signatures []string, blockIndexes []int32) error {
	q := `
		update
			"transaction" t
		set
			block_index = u.bi
		from (
			select
				unnest($1::VARCHAR(255)[]) as sig,
			 	unnest($2::INTEGER[]) as bi
		) as u
		where t.signature = u.sig
	`
	_, err := db.Exec(q, pq.StringArray(signatures), pq.Int32Array(blockIndexes))
	return err
}

type SelectAssociatedAccountsRow struct {
	Address       string
	Type          AssociatedAccountType
	Data          sql.NullString
	LastSignature sql.NullString `db:"last_signature"`
	ShouldFetch   bool           `db:"should_fetch"`
}

func SelectAssociatedAccounts(db DBTX, walletId int32) ([]*SelectAssociatedAccountsRow, error) {
	result := make([]*SelectAssociatedAccountsRow, 0)
	q := `
		SELECT
		    address, type, data, last_signature, should_fetch
		FROM
		    associated_account
		WHERE
			wallet_id = $1
	`
	err := db.Select(&result, q, walletId)
	return result, err
}

func UpdateLastSignature(db DBTX, isAssociatedAccount bool, address, lastSignature string) error {
	q := strings.Builder{}
	if !isAssociatedAccount {
		q.WriteString("update wallet")
	} else {
		q.WriteString("update associated_account")
	}
	q.WriteString(" set last_signature = $1 where address = $2")

	_, err := db.Exec(q.String(), lastSignature, address)
	return err
}

type SyncRequestStatus string

const (
	SyncRequestStatusFetching SyncRequestStatus = "fetching"
	SyncRequestStatusParsing  SyncRequestStatus = "parsing"
)

func UpdateSyncRequestStatus(db DBTX, walletId int32, status SyncRequestStatus) error {
	q := `
		update
			sync_request
		set
			status = $1
		where
			wallet_id = $2
	`
	_, err := db.Exec(q, status, walletId)
	return err
}

type SelectEventsRow struct {
	Signature string
	Id        int32
	IxIdx     int32 `db:"ix_idx"`
	Type      EventType
	Data      string
}

// TODO: FEES
func SelectEvents(db DBTX, offset int) ([]*SelectEventsRow, error) {
	result := make([]*SelectEventsRow, 0)
	q := `
		select
			e.id, e.type, e.data, e.ix_idx, t.signature
		from
			event e
			join "transaction" t on t.id = e.transaction_id
		order by
			t.slot asc, t.block_index asc, e.ix_idx asc, e.idx asc
		limit
			500
		offset $1
	`
	err := db.Select(&result, q, offset)
	return result, err
}

type SelectWalletsRow struct {
	Id      int32
	Address string
}

func SelectWallets(db DBTX) ([]*SelectWalletsRow, error) {
	result := make([]*SelectWalletsRow, 0)
	q := `
		select
			address,
			id
		from
			wallet
	`
	err := db.Select(&result, q)
	return result, err
}
