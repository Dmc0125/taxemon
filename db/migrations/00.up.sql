CREATE TABLE transaction (
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
    FOREIGN KEY (transaction_id) REFERENCES transaction (id) ON DELETE CASCADE
);

CREATE TABLE transaction_log (
    transaction_id INTEGER NOT NULL,
    idx INTEGER NOT NULL,
    log TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, idx),
    FOREIGN KEY (transaction_id) REFERENCES transaction (id) ON DELETE CASCADE
);

CREATE TABLE instruction (
    transaction_id INTEGER NOT NULL,
    --
    idx INTEGER NOT NULL,
    program_id_idx INTEGER NOT NULL,
    -- accounts ids stored as json array
    accounts_idxs TEXT,
    data BLOB,
    --
    PRIMARY KEY (transaction_id, idx),
    FOREIGN KEY (transaction_id) REFERENCES transaction (id) ON DELETE CASCADE
);

CREATE TABLE inner_instruction (
    transaction_id INTEGER NOT NULL,
    ix_idx INTEGER NOT NULL,
    --
    idx INTEGER NOT NULL,
    program_id_idx INTEGER NOT NULL,
    -- accounts ids stored as json array
    accounts_idxs TEXT,
    data BLOB,
    --
    PRIMARY KEY (transaction_id, ix_idx, idx),
    FOREIGN KEY (transaction_id) REFERENCES transaction (id) ON DELETE CASCADE,
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (transaction_id, id) ON DELETE CASCADE
);

CREATE VIEW v_inner_instruction AS
SELECT
    ta.address,
    iix.accounts_ids,
    iix.data
FROM
    inner_instruction iix
    JOIN transaction_account ta ON ta.transaction_id = iix.transaction_id
    AND ta.idx = iix.program_id_idx;

CREATE VIEW v_instruction AS
SELECT
    ta.address,
    ix.transaction_id,
    ix.accounts_ids,
    ix.data,
    COALESCE(
        json_group_array (
            CASE
                WHEN iix.transaction_id IS NOT NULL THEN json_object (
                    'program_id',
                    iix.program_id,
                    'accounts_ids',
                    iix.accounts_ids,
                    'data',
                    iix.data
                )
            END
        ),
        '[]'
    ) as inner_instructions
FROM
    instruction ix
    LEFT JOIN v_inner_instruction iix ON ix.transaction_id = iix.transaction_id
    AND ix.idx = iix.ix_idx
    JOIN transaction_account ta ON ta.transaction_id = ix.transaction_id
    AND ta.idx = ix.program_id_idx;

CREATE VIEW v_transaction AS
SELECT
    t.signature,
    t.id,
    json_group_array (ta.address) as accounts,
    COALESCE(
        (
            SELECT
                json_group_array (log)
            FROM
                transaction_log tl
            WHERE
                tl.transaction_id = t.id
            ORDER BY
                tl.idx
        ),
        '[]'
    ) AS logs,
    COALESCE(
        json_group_array (
            CASE
                WHEN ix.transaction_id IS NOT NULL THEN json_object (
                    'program_id',
                    ix.program_id,
                    'accounts_ids',
                    ix.accounts_ids,
                    'data',
                    ix.data,
                    'inner_ixs',
                    ix.inner_instructions
                )
            END
        ),
        '[]'
    ) AS instruction
FROM
    transaction t
    LEFT JOIN transaction_account ta ON ta.transaction_id = t.id
    LEFT JOIN v_instruction ix ON ix.transaction_id = t.id;
