// @generated automatically by Diesel CLI.

diesel::table! {
    accounts (id) {
        id -> Integer,
        name -> Nullable<Text>,
        address -> Text,
        owner_key_index -> BigInt,
        is_default -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    auth_status (id) {
        id -> Integer,
        user_decided -> Bool,
        granted -> Bool,
        token -> Nullable<Text>,
        revoked -> Bool,
    }
}

diesel::table! {
    config (id) {
        id -> Integer,
        key -> Text,
        value -> Text,
        is_encrypted -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    key_manager_states (id) {
        id -> Integer,
        branch_seed -> Text,
        index -> BigInt,
        is_active -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    non_fungible_tokens (id) {
        id -> Integer,
        vault_id -> Integer,
        nft_id -> Text,
        resource_id -> Text,
        data -> Text,
        mutable_data -> Text,
        is_burned -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    outputs (id) {
        id -> Integer,
        account_id -> Integer,
        vault_id -> Integer,
        commitment -> Text,
        value -> BigInt,
        sender_public_nonce -> Nullable<Text>,
        encryption_secret_key_index -> BigInt,
        public_asset_tag -> Nullable<Text>,
        status -> Text,
        locked_at -> Nullable<Timestamp>,
        locked_by_proof -> Nullable<Integer>,
        encrypted_data -> Binary,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    proofs (id) {
        id -> Integer,
        account_id -> Integer,
        vault_id -> Integer,
        transaction_hash -> Nullable<Text>,
        locked_revealed_amount -> BigInt,
        created_at -> Timestamp,
    }
}

diesel::table! {
    substates (id) {
        id -> Integer,
        module_name -> Nullable<Text>,
        address -> Text,
        parent_address -> Nullable<Text>,
        referenced_substates -> Text,
        version -> Integer,
        transaction_hash -> Text,
        template_address -> Nullable<Text>,
        created_at -> Timestamp,
    }
}

diesel::table! {
    transactions (id) {
        id -> Integer,
        hash -> Text,
        network -> Integer,
        instructions -> Text,
        fee_instructions -> Text,
        inputs -> Text,
        signatures -> Text,
        seal_signature -> Text,
        is_seal_signer_authorized -> Bool,
        result -> Nullable<Text>,
        qcs -> Nullable<Text>,
        final_fee -> Nullable<BigInt>,
        status -> Text,
        dry_run -> Bool,
        min_epoch -> Nullable<BigInt>,
        max_epoch -> Nullable<BigInt>,
        executed_time_ms -> Nullable<BigInt>,
        finalized_time_ms -> Nullable<BigInt>,
        required_substates -> Text,
        new_account_info -> Nullable<Text>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    vaults (id) {
        id -> Integer,
        account_id -> Integer,
        address -> Text,
        resource_address -> Text,
        resource_type -> Text,
        revealed_balance -> BigInt,
        confidential_balance -> BigInt,
        locked_revealed_balance -> BigInt,
        token_symbol -> Nullable<Text>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::joinable!(non_fungible_tokens -> vaults (vault_id));
diesel::joinable!(outputs -> accounts (account_id));
diesel::joinable!(outputs -> vaults (vault_id));
diesel::joinable!(proofs -> accounts (account_id));
diesel::joinable!(proofs -> vaults (vault_id));
diesel::joinable!(vaults -> accounts (account_id));

diesel::allow_tables_to_appear_in_same_query!(
    accounts,
    auth_status,
    config,
    key_manager_states,
    non_fungible_tokens,
    outputs,
    proofs,
    substates,
    transactions,
    vaults,
);
