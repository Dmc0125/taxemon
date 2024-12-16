-- name: FetchTransactions :many
SELECT
    *
FROM
    v_transaction tx
WHERE
    tx.signature IN (sqlc.slice ('signatures'));
