CREATE TABLE event (
    id SERIAL PRIMARY KEY,
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
    UNIQUE (transaction_id, ix_idx),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id),
    FOREIGN KEY (transaction_id, ix_idx) REFERENCES instruction (transaction_id, idx) ON DELETE CASCADE
);

CREATE TABLE known_instruction (
    id SERIAL UNIQUE,
    program_address VARCHAR(255) NOT NULL,
    discriminator VARCHAR(20) NOT NULL,
    disc_len SMALLINT NOT NULL,
    PRIMARY KEY (program_address, discriminator)
);

CREATE OR REPLACE FUNCTION get_inner_instructions(tx_id INTEGER, instruction_idx INTEGER)
RETURNS jsonb AS $$
    SELECT
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'program_id_idx', iix.program_id_idx,
                    'accounts_idxs', to_jsonb(iix.accounts_idxs),
                    'data', iix.data
                ) ORDER BY iix.idx ASC
            ),
            '[]'::jsonb
        )
    FROM
        inner_instruction iix
    WHERE
        iix.transaction_id = tx_id
    AND
        iix.ix_idx = instruction_idx;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_instructions(tx_id INTEGER)
RETURNS jsonb AS $$
    SELECT
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'idx', ix.idx,
                    'program_id_idx', ix.program_id_idx,
                    'accounts_idxs', to_jsonb(ix.accounts_idxs),
                    'data', ix.data,
                    'inner_ixs', get_inner_instructions(ix.transaction_id, ix.idx)
                ) ORDER BY ix.idx ASC
            ),
            '[]'::jsonb
        )
    FROM
        instruction ix
    WHERE
        ix.transaction_id = tx_id
        AND (
            unknown_only = false
            OR NOT EXISTS (
                SELECT
                    1
                FROM
                    event e
                WHERE
                    e.transaction_id = tx_id
                    AND e.ix_idx = ix.idx
            )
        )
$$ LANGUAGE SQL STABLE;

-- CREATE TYPE get_inner_instructions_r AS (
--     program_id_idx SMALLINT,
--     accounts_idxs SMALLINT[],
--     data TEXT
-- );

-- CREATE OR REPLACE FUNCTION get_inner_instructions(tx_id INTEGER, instruction_idx INTEGER)
-- RETURNS get_inner_instructions_r[] AS $$
--     SELECT
--         COALESCE(
--             ARRAY_AGG(
--                 ROW(
--                     iix.program_id_idx,
--                     iix.accounts_idxs,
--                     iix.data
--                 )::get_inner_instructions_r
--             ),
--             ARRAY[]::get_inner_instructions_r[]
--         )
--     FROM
--         inner_instruction iix
--     WHERE
--         iix.transaction_id = tx_id
--     AND
--         iix.ix_idx = instruction_idx;
-- $$ LANGUAGE SQL STABLE;

-- CREATE TYPE get_instructions_r AS (
--     idx INTEGER,
--     program_id_idx SMALLINT,
--     accounts_idxs SMALLINT[],
--     data TEXT,
--     inner_ixs get_inner_instructions_r[]
-- );

-- CREATE OR REPLACE FUNCTION get_instructions(tx_id INTEGER, unknown_only BOOLEAN DEFAULT false)
-- RETURNS SETOF get_instructions_r AS $$
--     SELECT
--         ix.idx,
--         ix.program_id_idx,
--         ix.accounts_idxs,
--         ix.data,
--         get_inner_instructions(tx_id, ix.idx) as inner_ixs
--     FROM
--         instruction ix
--     WHERE
--         ix.transaction_id = tx_id
--         AND (
--             unknown_only = false
--             OR NOT EXISTS (
--                 SELECT
--                     1
--                 FROM
--                     event e
--                 WHERE
--                     e.transaction_id = tx_id
--                     AND e.ix_idx = ix.idx
--             )
--         )
-- $$ LANGUAGE SQL STABLE;
