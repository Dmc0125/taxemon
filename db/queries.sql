-- name: FetchTransactions :many
SELECT
    *
FROM
    v_transaction t
WHERE
    t.signature in (sqlc.slice ('signatures'))
