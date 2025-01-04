CREATE TABLE wallet (
    id SERIAL NOT NULL PRIMARY KEY,
    address VARCHAR(255) NOT NULL UNIQUE,
    label VARCHAR(100),
    last_signature VARCHAR(255)
);

CREATE TYPE sync_request_status AS ENUM (
    -- fetching and parsing txs
    'fetching',
    -- parsing events into taxable events
    'parsing'
);

CREATE TABLE sync_request (
    id SERIAL NOT NULL PRIMARY KEY,
    wallet_id INTEGER NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status sync_request_status DEFAULT 'fetching',
    FOREIGN KEY (wallet_id) REFERENCES wallet (id) ON DELETE CASCADE
);

CREATE TABLE "transaction" (
    id SERIAL NOT NULL PRIMARY KEY,
    signature VARCHAR(255) NOT NULL UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL,
    slot BIGINT NOT NULL,
    block_index INTEGER,
    --
    accounts VARCHAR(255)[] NOT NULL,
    logs VARCHAR[] NOT NULL,

    err BOOLEAN NOT NULL DEFAULT false,
    err_msg JSONB
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

CREATE TABLE instruction (
    transaction_id INTEGER NOT NULL,
    --
    idx INTEGER NOT NULL,
    is_known BOOLEAN NOT NULL DEFAULT false,
    program_id_idx SMALLINT NOT NULL,
    accounts_idxs SMALLINT[] NOT NULL,
    data TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE
);

CREATE TABLE inner_instruction (
    transaction_id INTEGER NOT NULL,
    ix_idx INTEGER NOT NULL,
    --
    idx SMALLINT NOT NULL,
    program_id_idx SMALLINT NOT NULL,
    accounts_idxs SMALLINT[] NOT NULL,
    data TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, ix_idx, idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE,
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (transaction_id, idx) ON DELETE CASCADE
);

CREATE TABLE event (
    transaction_id INTEGER NOT NULL,
    ix_idx INTEGER NOT NULL,
    idx SMALLINT NOT NULL,
    --
    -- 0 -> transfer
    -- 1 -> mint
    -- 2 -> burn
    -- 3 -> close account
    type SMALLINT NOT NULL,
    -- event data stored as json string bytes
    data JSONB NOT NULL,
    --
    PRIMARY KEY (transaction_id, ix_idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id),
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (transaction_id, idx) ON DELETE CASCADE
);

CREATE TABLE associated_account (
    address VARCHAR(255) NOT NULL,
    last_signature VARCHAR(255),
    --
    -- 0 -> token account
    type SMALLINT NOT NULL,
    data JSONB,
    --
    PRIMARY KEY (address)
);

CREATE OR REPLACE FUNCTION get_inner_instructions(tx_id INTEGER, instruction_idx INTEGER)
RETURNS jsonb AS $$
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'program_id_idx', iix.program_id_idx,
                'accounts_idxs', to_jsonb(iix.accounts_idxs),
                'data', iix.data
            ) ORDER BY iix.idx
        ),
        '[]'::jsonb
    )
    FROM inner_instruction iix
    WHERE iix.transaction_id = tx_id
    AND iix.ix_idx = instruction_idx;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_instructions(tx_id INTEGER)
RETURNS jsonb AS $$
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'program_id_idx', ix.program_id_idx,
                'accounts_idxs', to_jsonb(ix.accounts_idxs),
                'data', ix.data,
                'inner_ixs', get_inner_instructions(ix.transaction_id, ix.idx)
            ) ORDER BY ix.idx
        ),
        '[]'::jsonb
    )
    FROM instruction ix
    WHERE ix.transaction_id = tx_id;
$$ LANGUAGE SQL STABLE;

CREATE VIEW v_transaction AS
SELECT
    t.signature,
    t.id,
    t.accounts,
    t.logs,
    get_instructions(t.id) AS instructions
FROM
    "transaction" t;
