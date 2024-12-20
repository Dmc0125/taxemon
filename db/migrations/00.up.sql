CREATE TABLE "transaction" (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    signature TEXT NOT NULL UNIQUE,
    -- unix seconds
    timestamp INTEGER NOT NULL,
    slot INTEGER NOT NULL,
    block_index INTEGER,
    --
    err BOOLEAN NOT NULL DEFAULT false,
    err_msg TEXT
);

CREATE TABLE transaction_account (
    transaction_id INTEGER NOT NULL,
    address TEXT NOT NULL,
    idx INTEGER NOT NULL,
    --
    PRIMARY KEY (address, idx, transaction_id),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE
);

CREATE TABLE transaction_log (
    transaction_id INTEGER NOT NULL,
    idx INTEGER NOT NULL,
    log TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE
);

CREATE TABLE instruction (
    transaction_id INTEGER NOT NULL,
    --
    idx INTEGER NOT NULL,
    program_id_idx INTEGER NOT NULL,
    -- accounts ids stored as json array
    accounts_idxs TEXT NOT NULL,
    data TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE
);

CREATE TABLE inner_instruction (
    transaction_id INTEGER NOT NULL,
    ix_idx INTEGER NOT NULL,
    --
    idx INTEGER NOT NULL,
    program_id_idx INTEGER NOT NULL,
    -- accounts ids stored as json array
    accounts_idxs TEXT NOT NULL,
    data TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, ix_idx, idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE,
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (transaction_id, idx) ON DELETE CASCADE
);

CREATE TABLE event (
    transaction_id INTEGER NOT NULL,
    ix_idx INTEGER NOT NULL,
    --
    -- 0 -> transfer
    type INTEGER NOT NULL,
    -- event data stored as json string bytes
    data BLOB NOT NULL,
    --
    PRIMARY KEY (transaction_id, ix_idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id),
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (tranaction_id, ix_idx) ON DELETE CASCADE
);

CREATE VIEW v_transaction_accounts AS
SELECT
    transaction_id,
    json_group_array (address) as addresses
FROM
    transaction_account
GROUP BY
    transaction_id;

CREATE VIEW v_transaction_logs AS
SELECT
    transaction_id,
    json_group_array (log) as logs
FROM
    transaction_log
GROUP BY
    transaction_id;

CREATE VIEW v_instructions AS
SELECT
    ix.transaction_id,
    COALESCE(
        json_group_array (
            CASE
                WHEN ix.transaction_id IS NOT NULL THEN json_object (
                    'program_address',
                    ta.address,
                    'accounts_idxs',
                    ix.accounts_idxs,
                    'data',
                    ix.data
                )
            END
        ),
        '[]'
    ) AS ixs
FROM
    instruction ix
    JOIN transaction_account ta ON ta.transaction_id = ix.transaction_id
    AND ta.idx = ix.program_id_idx
GROUP BY
    ix.transaction_id
ORDER BY
    ix.idx;

CREATE VIEW v_inner_instructions AS
SELECT
    iix.transaction_id,
    iix.ix_idx,
    COALESCE(
        json_group_array (
            CASE
                WHEN iix.transaction_id IS NOT NULL THEN json_object (
                    'program_address',
                    ta.address,
                    'accounts_idxs',
                    iix.accounts_idxs,
                    'data',
                    iix.data
                )
            END
        ),
        '[]'
    ) AS iixs
FROM
    inner_instruction iix
    JOIN transaction_account ta ON ta.transaction_id = iix.transaction_id
    AND ta.idx = iix.program_id_idx
GROUP BY
    iix.transaction_id,
    iix.ix_idx
ORDER BY
    iix.idx;

CREATE VIEW v_transaction AS
SELECT
    t.signature,
    t.id,
    ta.addresses AS accounts,
    tl.logs AS logs,
    ixs.ixs AS instructions,
    iixs.iixs AS inner_instructions
FROM
    "transaction" t
    LEFT JOIN v_transaction_accounts ta ON ta.transaction_id = t.id
    LEFT JOIN v_transaction_logs tl ON tl.transaction_id = t.id
    LEFT JOIN v_instructions ixs ON ixs.transaction_id = t.id
    LEFT JOIN v_inner_instructions iixs ON iixs.transaction_id = t.id;
