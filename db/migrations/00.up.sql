CREATE TABLE wallet (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL UNIQUE,
    label TEXT,
    last_signature TEXT
);

CREATE TABLE sync_request (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    wallet_id INTEGER NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (wallet_id) REFERENCES wallet (id) ON DELETE CASCADE
);

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

CREATE INDEX transaction_signature ON "transaction" (signature);

CREATE TABLE transaction_to_wallet (
    wallet_id INTEGER NOT NULL,
    transaction_id INTEGER NOT NULL,
    --
    PRIMARY KEY (wallet_id, transaction_id),
    FOREIGN KEY (wallet_id) REFERENCES wallet (id) ON DELETE CASCADE,
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE
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
    is_known BOOLEAN NOT NULL DEFAULT false,
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
    idx INTEGER NOT NULL,
    --
    -- 0 -> transfer
    -- 1 -> mint
    -- 2 -> burn
    -- 3 -> close account
    type INTEGER NOT NULL,
    -- event data stored as json string bytes
    data TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, ix_idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id),
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (tranaction_id, ix_idx) ON DELETE CASCADE
);

CREATE TABLE associated_account (
    address TEXT NOT NULL,
    last_signature TEXT,
    --
    -- 0 -> token account
    type INTEGER NOT NULL,
    data TEXT,
    --
    PRIMARY KEY (address)
);

CREATE VIEW v_transaction_accounts AS
SELECT
    transaction_id,
    json_group_array (address) as addresses
FROM
    transaction_account
GROUP BY
    transaction_id
ORDER BY
    idx ASC;

CREATE VIEW v_transaction_logs AS
SELECT
    transaction_id,
    json_group_array (log) as logs
FROM
    transaction_log
GROUP BY
    transaction_id;

CREATE VIEW v_inner_instruction AS
SELECT
    iix.transaction_id,
    iix.ix_idx,
    ta.address AS program_address,
    iix.accounts_idxs,
    iix.data
FROM
    inner_instruction iix
    JOIN transaction_account ta ON ta.transaction_id = iix.transaction_id
    AND ta.idx = iix.program_id_idx
ORDER BY
    iix.idx ASC;

CREATE VIEW v_instruction AS
SELECT
    ix.transaction_id,
    ix.is_known,
    ix.idx,
    ta.address AS program_address,
    ix.accounts_idxs,
    ix.data,
    COALESCE(
        (
            SELECT
                json_group_array (
                    json_object (
                        'program_address',
                        iix.program_address,
                        'accounts_idxs',
                        iix.accounts_idxs,
                        'data',
                        iix.data
                    )
                )
            FROM
                v_inner_instruction iix
            WHERE
                iix.transaction_id = ix.transaction_id
                AND iix.ix_idx = ix.idx
        ),
        '[]'
    ) AS inner_ixs
FROM
    instruction ix
    JOIN transaction_account ta ON ta.transaction_id = ix.transaction_id
    AND ta.idx = ix.program_id_idx
GROUP BY
    ix.transaction_id,
    ix.idx;

CREATE VIEW v_transaction AS
SELECT
    t.signature,
    t.id,
    ta.addresses AS accounts,
    tl.logs AS logs,
    COALESCE(
        (
            SELECT
                json_group_array (
                    json_object (
                        'program_address',
                        ix.program_address,
                        'accounts_idxs',
                        ix.accounts_idxs,
                        'data',
                        ix.data,
                        'inner_ixs',
                        ix.inner_ixs
                    )
                )
            FROM
                v_instruction ix
            WHERE
                ix.transaction_id = t.id
            ORDER BY
                ix.idx ASC
        ),
        '[]'
    ) AS instructions
FROM
    "transaction" t
    LEFT JOIN v_transaction_accounts ta ON ta.transaction_id = t.id
    LEFT JOIN v_transaction_logs tl ON tl.transaction_id = t.id
GROUP BY
    t.id;
