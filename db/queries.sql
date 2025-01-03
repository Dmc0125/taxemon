-- name: FetchTransactions :many
SELECT
    *
FROM
    v_transaction t
WHERE
    t.signature in (sqlc.slice ('signatures'));

-- name: FetchDuplicateTimestampsTransactions :many
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
    t1.signature;

-- name: FetchAssociatedAccounts :many
SELECT
    address,
    last_signature
FROM
    associated_account;

-- name: FetchUnknownTransactions :many
SELECT DISTINCT
    v.*
FROM
    v_transaction v
    JOIN instruction i ON i.transaction_id = v.id
WHERE
    i.is_known = true;

-- name: InsertWallet :one
INSERT INTO
    wallet (address, label)
VALUES
    (sqlc.arg ('address'), sqlc.arg ('label')) RETURNING id;

-- name: InsertSyncRequest :exec
INSERT INTO
    sync_request (wallet_id, created_at)
VALUES
    (sqlc.arg ('wallet_id'), sqlc.arg ('created_at'));

-- name: SetWalletLastSignature :exec
UPDATE wallet
SET
    last_signature = sqlc.arg ('signature')
WHERE
    address = sqlc.arg ('address');

-- name: SetAssociatedAccountLastSignature :exec
UPDATE associated_account
SET
    last_signature = sqlc.arg ('signature')
WHERE
    address = sqlc.arg ('address');

-- name: GetLatestSyncRequest :one
SELECT
    sync_request.wallet_id,
    wallet.address,
    wallet.last_signature
FROM
    sync_request
    JOIN wallet ON wallet.id = sync_request.wallet_id
ORDER BY
    sync_request.created_at
LIMIT
    1;
