PRAGMA foreign_keys = ON;

-- Key Manager
CREATE TABLE key_manager_states
(
    id          INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    branch_seed TEXT                              NOT NULL,
    `index`     BIGINT                            NOT NULL,
    is_active   BOOLEAN                           NOT NULL,
    created_at  DATETIME                          NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME                          NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX key_manager_states_uniq_branch_seed_index on key_manager_states (branch_seed, `index`);

-- Config

CREATE TABLE config
(
    id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    key          TEXT                              NOT NULL,
    value        TEXT                              NOT NULL,
    is_encrypted BOOLEAN                           NOT NULL,
    created_at   DATETIME                          NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME                          NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX config_uniq_key on config (key);

-- Transaction
CREATE TABLE transactions
(
    id                        INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    hash                      TEXT     NOT NULL,
    network                   INTEGER  NOT NULL,
    instructions              TEXT     NOT NULL,
    fee_instructions          TEXT     NOT NULL,
    inputs                    TEXT     NOT NULL,
    signatures                TEXT     NOT NULL,
    seal_signature            TEXT     NOT NULL,
    is_seal_signer_authorized BOOLEAN  NOT NULL,
    result                    TEXT     NULL,
    qcs                       TEXT     NULL,
    final_fee                 BIGINT   NULL,
    status                    TEXT     NOT NULL,
    dry_run                   BOOLEAN  NOT NULL,
    min_epoch                 BIGINT   NULL,
    max_epoch                 BIGINT   NULL,
    executed_time_ms          bigint   NULL,
    finalized_time_ms         bigint   NULL,
    required_substates        text     NOT NULL default '[]',
    new_account_info          text     NULL,
    created_at                DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX transactions_uniq_hash ON transactions (hash);
CREATE INDEX transactions_idx_status ON transactions (status);

-- Substates
CREATE TABLE substates
(
    id                   INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    module_name          TEXT     NULL,
    address              TEXT     NOT NULL,
    parent_address       TEXT     NULL,
    referenced_substates TEXT     NOT NULL,
    version              INTEGER  NOT NULL,
    transaction_hash     TEXT     NOT NULL,
    template_address     TEXT     NULL,
    created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX substates_idx_transaction_hash ON substates (transaction_hash);
CREATE UNIQUE INDEX substates_uniq_address ON substates (address);

-- Accounts
CREATE TABLE accounts
(
    id              INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    name            TEXT     NULL,
    address         TEXT     NOT NULL,
    owner_key_index BIGINT   NOT NULL,
    is_default      BOOLEAN  NOT NULL DEFAULT 0,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX accounts_uniq_address ON accounts (address);
CREATE UNIQUE INDEX accounts_uniq_name ON accounts (name) WHERE name IS NOT NULL;

-- Vaults
CREATE TABLE vaults
(
    id                      INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    account_id              INTEGER  NOT NULL REFERENCES accounts (id),
    address                 TEXT     NOT NULL,
    resource_address        TEXT     NOT NULL,
    resource_type           TEXT     NOT NULL,
    revealed_balance        BIGINT   NOT NULL DEFAULT 0,
    confidential_balance    BIGINT   NOT NULL DEFAULT 0,
    locked_revealed_balance BIGINT   NOT NULL DEFAULT 0,
    token_symbol            TEXT     NULL,
    created_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX vaults_uniq_address ON vaults (address);

-- Outputs
CREATE TABLE outputs
(
    id                          INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    account_id                  INTEGER  NOT NULL REFERENCES accounts (id),
    vault_id                    INTEGER  NOT NULL REFERENCES vaults (id),
    commitment                  TEXT     NOT NULL,
    value                       BIGINT   NOT NULL,
    sender_public_nonce         TEXT     NULL,
    encryption_secret_key_index BIGINT   NOT NULL,
    public_asset_tag            TEXT     NULL,
    -- Status can be "Unspent", "Spent", "Locked", "LockedUnconfirmed", "Invalid"
    status                      TEXT     NOT NULL,
    locked_at                   DATETIME NULL,
    locked_by_proof             INTEGER  NULL,
    encrypted_data              blob     NOT NULL DEFAULT '',
    created_at                  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX outputs_uniq_commitment ON outputs (commitment);
CREATE INDEX outputs_idx_account_status ON outputs (account_id, status);

-- Proofs
CREATE TABLE proofs
(
    id                     INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    account_id             INTEGER  NOT NULL REFERENCES accounts (id),
    vault_id               INTEGER  NOT NULL REFERENCES vaults (id),
    transaction_hash       TEXT     NULL,
    locked_revealed_amount BIGINT   NOT NULL DEFAULT 0,
    created_at             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Auth token, we don't store the auth token, the token in this table is the jwt token that is granted when user accepts the auth login request.
CREATE TABLE auth_status
(
    id           INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    user_decided BOOLEAN                           NOT NULL,
    granted      BOOLEAN                           NOT NULL,
    token        TEXT                              NULL,
    revoked      BOOLEAN                           NOT NULL DEFAULT FALSE
);

-- NFTs
CREATE TABLE non_fungible_tokens
(
    id           INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    vault_id     INTEGER  NOT NULL REFERENCES vaults (id),
    nft_id       TEXT     NOT NULL,
    resource_id  text     NOT NULL,
    data         TEXT     NOT NULL,
    mutable_data TEXT     NOT NULL,
    is_burned    BOOLEAN  NOT NULL,
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX nfts_uniq_address ON non_fungible_tokens (nft_id);
