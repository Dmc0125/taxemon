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
