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
    idx INTEGER NOT NULL,
    program_id_idx SMALLINT NOT NULL,
    accounts_idxs SMALLINT[] NOT NULL,
    data TEXT NOT NULL,
    --
    PRIMARY KEY (transaction_id, ix_idx, idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id) ON DELETE CASCADE,
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (transaction_id, idx) ON DELETE CASCADE
);

CREATE TYPE associated_account_type AS ENUM('token', 'jup_limit');

CREATE TABLE associated_account (
    address VARCHAR(255) NOT NULL,
    wallet_id INTEGER NOT NULL,
    last_signature VARCHAR(255),
    should_fetch BOOL NOT NULL,
    type associated_account_type NOT NULL,
    data JSONB,
    --
    PRIMARY KEY (address),
    FOREIGN KEY (wallet_id) REFERENCES wallet (id)
);
