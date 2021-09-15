-------------------
-- Add partitioning logic to tables
-------------------

-- partman extension automation of partitioning
-- create role partman with login;
-- grant all on schema ${mirror-schema} to partman;
-- grant all on all tables in schema ${mirror-schema} to partman;
-- grant execute on all functions in schema ${mirror-schema} to partman;
-- grant execute on all procedures in schema ${mirror-schema} to partman; -- PG11+ only
-- grant all on schema ${mirror-schema} to partman;

-- timestamp constants for year starts and ends
-- \set endOf2019Ns 1577836799000000000
-- \set endOf2020Ns 1609459199000000000
-- \set endOf2021Ns 1640995199000000000

-- can't automatically partition for now as nanoseconds are not supported by p_interval custom, also they change per year
-- select partman.create_parent('account_balance', 'consensus_timestamp', 'native', '', p_start_partition := '0');

-- account_balance
create table acc_bal_2019 partition of account_balance
    for values from (0) to (1577836799000000000);
create table acc_bal_2020 partition of account_balance
    for values from (1577836799000000000) to (1609459199000000000);
create table acc_bal_2021 partition of account_balance
    for values from (1609459199000000000) to (1640995199000000000);

-- account_balance_file
create table acc_bal_file_2019 partition of account_balance_file
    for values from (0) to (1577836799000000000);
create table acc_bal_file_2020 partition of account_balance_file
    for values from (1577836799000000000) to (1609459199000000000);
create table acc_bal_file_2021 partition of account_balance_file
    for values from (1609459199000000000) to (1640995199000000000);

-- crypto_transfer
create table crypto_transfer_2019 partition of crypto_transfer
    for values from (0) to (1577836799000000000);
create table crypto_transfer_2020 partition of crypto_transfer
    for values from (1577836799000000000) to (1609459199000000000);
create table crypto_transfer_2021 partition of crypto_transfer
    for values from (1609459199000000000) to (1640995199000000000);

-- entity
-- should we partition to get ahead of the curve?
-- create table entity_2019 partition of entity
--     for values from (0) to (1577836799000000000);
-- create table entity_2020 partition of entity
--     for values from (1577836799000000000) to (1609459199000000000);
-- create table entity_2021 partition of entity
--     for values from (1609459199000000000) to (1640995199000000000);

-- partial data issue exists since timestamps are null in those cases. Partitioning by id instead?
-- Should we do by 1k, 10k, 100k or 1M?
create table entity_0 partition of entity
    for values from (0) to (100000);
create table entity_1 partition of entity
    for values from (100000) to (200000);
create table entity_2 partition of entity
    for values from (200000) to (300000);
create table entity_3 partition of entity
    for values from (300000) to (3000000000);
create table entity_4 partition of entity
    for values from (3000000000) to (999999999999);

-- nft_transfer
create table nft_transfer_2019 partition of nft_transfer
    for values from (0) to (1577836799000000000);
create table nft_transfer_2020 partition of nft_transfer
    for values from (1577836799000000000) to (1609459199000000000);
create table nft_transfer_2021 partition of nft_transfer
    for values from (1609459199000000000) to (1640995199000000000);

-- non_fee_transfer
create table non_fee_transfer_2019 partition of non_fee_transfer
    for values from (0) to (1577836799000000000);
create table non_fee_transfer_2020 partition of non_fee_transfer
    for values from (1577836799000000000) to (1609459199000000000);
create table non_fee_transfer_2021 partition of non_fee_transfer
    for values from (1609459199000000000) to (1640995199000000000);

-- record_file
create table rec_file_2019 partition of record_file
    for values from (0) to (1577836799000000000);
create table rec_file_2020 partition of record_file
    for values from (1577836799000000000) to (1609459199000000000);
create table rec_file_2021 partition of record_file
    for values from (1609459199000000000) to (1640995199000000000);

-- token_balance
create table token_bal_2019 partition of token_balance
    for values from (0) to (1577836799000000000);
create table token_bal_2020 partition of token_balance
    for values from (1577836799000000000) to (1609459199000000000);
create table token_bal_2021 partition of token_balance
    for values from (1609459199000000000) to (1640995199000000000);

-- topic_message
create table topic_msg_2019 partition of topic_message
    for values from (0) to (1577836799000000000);
create table topic_msg_2020 partition of topic_message
    for values from (1577836799000000000) to (1609459199000000000);
create table topic_msg_2021 partition of topic_message
    for values from (1609459199000000000) to (1640995199000000000);

-- token_transfer
create table token_transfer_2019 partition of token_transfer
    for values from (0) to (1577836799000000000);
create table token_transfer_2020 partition of token_transfer
    for values from (1577836799000000000) to (1609459199000000000);
create table token_transfer_2021 partition of token_transfer
    for values from (1609459199000000000) to (1640995199000000000);

-- transaction
create table transaction_2019 partition of transaction
    for values from (0) to (1577836799000000000);
create table transaction_2020 partition of transaction
    for values from (1577836799000000000) to (1609459199000000000);
create table transaction_2021 partition of transaction
    for values from (1609459199000000000) to (1640995199000000000);






