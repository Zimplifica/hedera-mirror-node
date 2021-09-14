-------------------
-- Restore Backup tables use efficient COPY process to CSV's
-------------------

-- copy from backups into distributed tables and temp table where migration is needed
\copy account_balance (consensus_timestamp, balance, account_id) from account_balance.csv csv;

\copy account_balance_file (bytes, consensus_timestamp, count, file_hash, load_start, load_end, name, node_account_id) from account_balance_file.csv csv;

\copy address_book (start_consensus_timestamp, end_consensus_timestamp, file_id, node_count, file_data) from address_book.csv csv;

\copy address_book_entry (consensus_timestamp, description, memo, public_key, node_id, node_account_id, node_cert_hash, stake) from address_book_entry.csv csv;

\copy address_book_service_endpoint (consensus_timestamp, ip_address_v4, node_id, port) from address_book_service_endpoint.csv csv;

\copy contract_result (function_parameters, gas_supplied, call_result, gas_used, consensus_timestamp) from contract_result.csv csv;

\copy custom_fee (amount, amount_denominator, collector_account_id, created_timestamp, denominating_token_id, maximum_amount, minimum_amount, token_id) from custom_fee.csv csv;

\copy file_data (file_data, consensus_timestamp, entity_id, transaction_type) from file_data.csv csv;

\copy live_hash (livehash, consensus_timestamp) from live_hash.csv csv;

\copy nft (account_id, created_timestamp, deleted, modified_timestamp, metadata, serial_number, token_id) from nft.csv csv;

\copy record_file (bytes, consensus_start, consensus_end, count, digest_algorithm, file_hash, hapi_version_major, hapi_version_minor, hapi_version_patch, hash, "index", load_start, load_end, name, node_account_id, prev_hash, version) from record_file.csv csv;

\copy schedule (consensus_timestamp, creator_account_id, executed_timestamp, payer_account_id, schedule_id, transaction_body) from schedule.csv csv;

\copy entity (auto_renew_account_id, auto_renew_period, created_timestamp, deleted, expiration_timestamp, id, key, memo, modified_timestamp, num, proxy_account_id, public_key, realm, shard, submit_key, type) from entity.csv csv;

\copy token (token_id, created_timestamp, decimals, fee_schedule_key, fee_schedule_key_ed25519_hex, freeze_default, freeze_key, freeze_key_ed25519_hex, initial_supply, kyc_key, kyc_key_ed25519_hex, max_supply, modified_timestamp, name, supply_key, supply_key_ed25519_hex, supply_type, symbol, total_supply, treasury_account_id, type, wipe_key, wipe_key_ed25519_hex) from token.csv csv;

\copy token_account (account_id, associated, created_timestamp, freeze_status, kyc_status, modified_timestamp, token_id) from token_account.csv csv;

\copy token_balance (consensus_timestamp, account_id, balance, token_id) from token_balance.csv csv;

alter table transaction
    alter column entity_id drop not null,
    alter column scheduled drop not null;
\copy transaction (consensus_ns, type, result, payer_account_id, valid_start_ns, valid_duration_seconds, node_account_id, entity_id, initial_balance, max_fee, charged_tx_fee, memo, scheduled, transaction_hash, transaction_bytes) from transaction.csv csv;

\copy transaction_signature (consensus_timestamp, public_key_prefix, entity_id, signature) from transaction_signature.csv csv;

-- handle null scheduled and entity_id values from backup
update transaction
set scheduled = case when scheduled is null then false else scheduled end,
    entity_id = case when entity_id is null then 0 else entity_id end
where scheduled is null or entity_id is null;

alter table transaction
    alter column entity_id set not null,
    alter column scheduled set not null;


-- create temp, copy and migrate to distributed table for tables that have a mismatch between schema versions
--- assessed_custom_fee
create table if not exists assessed_custom_fee_temp
(
    amount                       bigint not null,
    collector_account_id         bigint not null,
    consensus_timestamp          bigint not null,
    token_id                     bigint
);

\copy assessed_custom_fee_temp (amount, collector_account_id, consensus_timestamp, token_id) from assessed_custom_fee.csv csv;

--- crypto_transfer
create table if not exists crypto_transfer_temp
(
    entity_id                    bigint not null,
    consensus_timestamp          bigint not null,
    amount                       bigint not null
);

\copy crypto_transfer_temp (entity_id, consensus_timestamp, amount) from crypto_transfer.csv csv;

--- nft_transfer
create table if not exists nft_transfer_temp
(
    consensus_timestamp          bigint not null,
    receiver_account_id          bigint,
    sender_account_id            bigint,
    serial_number                bigint not null,
    token_id                     bigint not null
);

\copy nft_transfer_temp (consensus_timestamp, receiver_account_id, sender_account_id, serial_number, token_id) from nft_transfer.csv csv

--- non_fee_transfer
create table if not exists non_fee_transfer_temp
(
    entity_id                    bigint not null,
    consensus_timestamp          bigint not null,
    amount                       bigint not null
);

\copy non_fee_transfer_temp (entity_id, consensus_timestamp, amount) from non_fee_transfer.csv csv;

--- token_transfer
create table if not exists token_transfer_temp
(
    token_id                     bigint not null,
    account_id                   bigint not null,
    consensus_timestamp          bigint not null,
    amount                       bigint not null
);

\copy token_transfer_temp (token_id, account_id, consensus_timestamp, amount) from token_transfer.csv csv;

--- topic_message
create table if not exists topic_message_temp
(
    consensus_timestamp   bigint   not null,
    entity_id             bigint   null,
    realm_num             smallint not null,
    topic_num             integer  not null,
    message               bytea    not null,
    running_hash          bytea    not null,
    sequence_number       bigint   not null,
    running_hash_version  smallint not null,
    chunk_num             integer,
    chunk_total           integer,
    payer_account_id      bigint,
    valid_start_timestamp bigint
);
\copy topic_message_temp (consensus_timestamp, realm_num, topic_num, message, running_hash, sequence_number, running_hash_version, chunk_num, chunk_total, payer_account_id, valid_start_timestamp) from topic_message.csv csv;
update topic_message_temp
    set entity_id = topic_num;

insert into topic_message
select *
from topic_message_temp;

drop table if exists topic_message_temp;

-- populate transaction_payer_account_id distribution column on transfer tables
SELECT NOW();
select 'assessed_custom_fee insert';
insert into assessed_custom_fee
select acft.amount, acft.collector_account_id, acft.consensus_timestamp, acft.token_id, t.payer_account_id as transaction_payer_account_id
from assessed_custom_fee_temp acft
join transaction t on acft.consensus_timestamp = t.consensus_ns;

SELECT NOW();
select 'crypto_transfer insert';
insert into crypto_transfer
select cft.entity_id, cft.consensus_timestamp, cft.amount, t.payer_account_id as transaction_payer_account_id
from crypto_transfer_temp cft
join transaction t on cft.consensus_timestamp = t.consensus_ns;

SELECT NOW();
select 'non_fee_transfer insert';
insert into non_fee_transfer
select nft.entity_id, nft.consensus_timestamp, nft.amount, t.payer_account_id as transaction_payer_account_id
from non_fee_transfer_temp nft
join transaction t on nft.consensus_timestamp = t.consensus_ns;

SELECT NOW();
select 'nft_transfer insert';
insert into nft_transfer
select nft.consensus_timestamp, nft.receiver_account_id, nft.sender_account_id, nft.serial_number, nft.token_id, t.payer_account_id as transaction_payer_account_id
from nft_transfer_temp nft
join transaction t on nft.consensus_timestamp = t.consensus_ns;

SELECT NOW();
select 'token_transfer insert';
insert into token_transfer
select ttt.token_id, ttt.account_id, ttt.consensus_timestamp, ttt.amount, t.payer_account_id as transaction_payer_account_id
from token_transfer_temp ttt
join transaction t on ttt.consensus_timestamp = t.consensus_ns;
