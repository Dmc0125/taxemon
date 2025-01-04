package dbutils

import (
	"database/sql"
	"encoding/json"
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

type SelectTransactionInstructionBase struct {
	ProgramIdIdx int16   `json:"program_id_idx"`
	AccountsIdxs []int16 `json:"accounts_idxs"`
	Data         string
}

type SelectTransactionInstruction struct {
	*SelectTransactionInstructionBase
	InnerInstructions []*SelectTransactionInstructionBase `json:"inner_ixs"`
}

type SelectTransactionsRow struct {
	Id           int32
	Signature    string
	Accounts     pq.StringArray
	Logs         pq.StringArray
	Instructions json.RawMessage
}

func SelectTransactions(db DBTX, signatures []string) ([]*SelectTransactionsRow, error) {
	result := make([]*SelectTransactionsRow, 0)
	q := `
		select
			t.id, t.signature, t.accounts, t.logs, t.instructions
		from
			"v_transaction" t
		where
			t.signature = any($1)
	`
	err := db.Select(&result, q, pq.StringArray(signatures))
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
		returning id, signature
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
	IsKnown       bool            `db:"is_known"`
	ProgramIdIdx  int16           `db:"program_id_idx"`
	AccountsIdxs  pq.GenericArray `db:"accounts_idxs"`
	Data          string
}

func InsertInstructions(db DBTX, params []*InsertInstructionParams) error {
	q := `
		insert into
			instruction (transaction_id, idx, is_known, program_id_idx, accounts_idxs, data)
		values
			(:transaction_id, :idx, :is_known, :program_id_idx, :accounts_idxs, :data)
	`
	_, err := db.NamedExec(q, params)
	return err
}

type InsertInnerInstructionParams struct {
	TransactionId int32 `db:"transaction_id"`
	IxIdx         int32 `db:"ix_idx"`
	Idx           int16
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
	Type          int16
	Data          string
}

func InsertEvents(db DBTX, params []*InsertEventParams) error {
	q := `
		insert into
			event (transaction_id, ix_idx, idx, type, data)
		values
			(:transaction_id, :ix_idx, :idx, :type, :data)
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
	`
	_, err := db.NamedExec(q, params)
	return err
}

type InsertAssociatedAccountParams struct {
	Address string
	Type    int16
	Data    string
}

func InsertAssociatedAccounts(db DBTX, params []*InsertAssociatedAccountParams) error {
	q := `
		insert into
			associated_account (address, type, data)
		values
			(:address, :type, :data)
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
		    AND t2.timestamp = t1.timestamp
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
	LastSignature sql.NullString `db:"last_signature"`
}

func SelectAssociatedAccounts(db DBTX) ([]*SelectAssociatedAccountsRow, error) {
	result := make([]*SelectAssociatedAccountsRow, 0)
	q := `
		SELECT
		    address,
		    last_signature
		FROM
		    associated_account
	`
	err := db.Select(&result, q)
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