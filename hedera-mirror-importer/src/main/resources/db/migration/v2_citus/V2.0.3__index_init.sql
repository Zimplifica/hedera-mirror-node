-------------------
-- Add constraints and indexes to tables
-------------------

-- use sequential, avoid error "The index name ... on a shard is too long and could lead to deadlocks when executed ..."
set local citus.multi_shard_modify_mode to 'sequential';

-- account_balance
alter table account_balance
    add constraint acc_bal_pk primary key (consensus_timestamp, account_id);
create index if not exists acc_bal_account__timestamp
    on account_balance (account_id desc, consensus_timestamp desc);

-- account_balance_file
alter table account_balance_file
    add constraint acc_bal_fl__pk primary key (consensus_timestamp, node_account_id);
create unique index if not exists acc_bal_fl__name
    on account_balance_file (name, consensus_timestamp desc, node_account_id);

-- assessed_custom_fee
create index if not exists ass_cust_fee__timestamp
    on assessed_custom_fee (consensus_timestamp);

-- address_book
alter table address_book
    add constraint addr_bk__pk primary key (start_consensus_timestamp, file_id);

-- address_book_entry
alter table address_book_entry
    add constraint addr_bk_entry__pk primary key (consensus_timestamp, node_id, node_account_id);

-- address_book_service_endpoint
alter table address_book_service_endpoint
    add constraint addr_bk_serv_endpt__pk primary key (consensus_timestamp, node_id, ip_address_v4, port);

-- contract_result
create index if not exists contr_res__consensus
    on contract_result (consensus_timestamp desc);

-- crypto_transfer
create index if not exists crypt_trans__timestamp
    on crypto_transfer (consensus_timestamp, transaction_payer_account_id);
create index if not exists crypt_trans__id_timestamp
    on crypto_transfer (entity_id, consensus_timestamp, transaction_payer_account_id)
    where entity_id != 98;
-- id corresponding to treasury address 0.0.98

-- custom_fee
create index if not exists cust_fee__token_timestamp
    on custom_fee (token_id desc, created_timestamp desc);

-- entity
alter table entity
    add constraint entity__pk primary key (id);
-- Enforce lowercase hex representation by constraint rather than making indexes on lower(ed25519).
alter table entity
    add constraint c__entity__lower_ed25519
        check (public_key = lower(public_key));
create index if not exists entity__id_type
    on entity (id, type);
create index if not exists entity__pkey
    on entity (public_key) where public_key is not null;
create unique index if not exists entity__shard_realm_num
    on entity (shard, realm, num, id);
-- have to add id when creating unique indexes due to partitioning

-- event_file
alter table event_file
    add constraint event_file__pk primary key (consensus_end, node_account_id);
create unique index if not exists event_file__hash
    on event_file (hash, consensus_end, node_account_id);

-- file_data
alter table file_data
    add constraint file_data__pk primary key (consensus_timestamp, entity_id);

-- live_hash
-- add entity_id to index when added as column
alter table live_hash
    add constraint live_hash__pk primary key (consensus_timestamp);

-- nft
alter table nft
    add constraint nft__pk primary key (token_id, serial_number, created_timestamp);

-- nft_transfer
create unique index if not exists nft_trans__timestamp_token_id_serial
    on nft_transfer (consensus_timestamp desc, token_id desc, serial_number desc, transaction_payer_account_id);

-- non_fee_transfer
create index if not exists non_fee_trans__timestamp
    on non_fee_transfer (consensus_timestamp, transaction_payer_account_id);

-- record_file
alter table record_file
    add constraint record_file__pk primary key (consensus_end, node_account_id);
create unique index if not exists record_file__index
    on record_file (index, node_account_id, consensus_end);
create unique index if not exists record_file__hash
    on record_file (hash, node_account_id, consensus_end);
create index if not exists record_file__prev_hash
    on record_file (prev_hash);

-- schedule
alter table schedule
    add constraint schedule__pk primary key (consensus_timestamp, schedule_id);
create unique index if not exists schedule__schedule_id
    on schedule (schedule_id desc, consensus_timestamp desc);
create index if not exists schedule__creator_account_id
    on schedule (creator_account_id desc);

-- t_entity_types
alter table t_entity_types
    add constraint t_entity_types__pk primary key (id);

-- t_transaction_results
alter table t_transaction_results
    add constraint transac_res__pk primary key (proto_id);
create unique index if not exists transac_res_name
    on t_transaction_results (result);

-- t_transaction_types
alter table t_transaction_types
    add constraint transact_types__pk primary key (proto_id);
create unique index if not exists transac_types_name
    on t_transaction_types (name);

-- token
alter table token
    add constraint token__pk primary key (created_timestamp, token_id);
create unique index if not exists token__id_timestamp
    on token (token_id, created_timestamp);

-- token_account
alter table token_account
    add constraint token_account__pk primary key (account_id, token_id, modified_timestamp);
create unique index if not exists token_acc__token_acc_timestamp
    on token_account (token_id, account_id, modified_timestamp);

-- token_balance
alter table token_balance
    add constraint token_balance__pk primary key (consensus_timestamp, account_id, token_id);

-- token_transfer
create index if not exists token_transf__token_acc_timestamp
    on token_transfer (consensus_timestamp desc, token_id desc, account_id desc, transaction_payer_account_id);
create index if not exists token_transf__acc_timestamp
    on token_transfer (account_id, consensus_timestamp desc, transaction_payer_account_id);

-- topic_message
alter table if exists topic_message
    add constraint topic_msg__pk primary key (consensus_timestamp, entity_id);
create index if not exists topic_msg__realm_num_timestamp
    on topic_message (realm_num, topic_num, consensus_timestamp);
create unique index if not exists topic_msg__topic_num_realm_num_seqnum
    on topic_message (realm_num, topic_num, sequence_number, entity_id, consensus_timestamp);

-- transaction
alter table if exists transaction
    add constraint transaction__pk primary key (consensus_ns, payer_account_id);
create index if not exists transaction__trans_id
    on transaction (valid_start_ns, payer_account_id);
create index if not exists trans__payer_acc_id
    on transaction (payer_account_id);
create index if not exists transaction_type
    on transaction (type, consensus_ns desc);

-- transaction_signature
create index if not exists trans_sig__id
    on transaction_signature (entity_id desc, consensus_timestamp desc);

create unique index if not exists trans_sig__timestamp_pub_key_pref
    on transaction_signature (consensus_timestamp desc, public_key_prefix, entity_id);

-- revert to default
set local citus.multi_shard_modify_mode to 'parallel';
