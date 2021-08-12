-------------------
-- Create distributed tables for tables that have mostly insert logic
-- Create reference tables for tables used only to describe with rare insert/updates
-------------------

-- entity
-- main distributed table. Most if not all other table will attempt to co-locate with entity shards on worker nodes
select create_distributed_table('entity', 'id');

-- assessed_custom_fee
select create_distributed_table('assessed_custom_fee', 'token_id', colocate_with => 'entity');

-- account_balance
select create_distributed_table('account_balance', 'account_id', colocate_with => 'entity');

-- account_balance_file
select create_distributed_table('account_balance_file', 'node_account_id', colocate_with => 'entity');

-- address_book - as is distributed query won't work for address book. Need to add file_id to address_book_entry
-- select create_distributed_table('address_book', 'file_id', colocate_with => 'entity');

-- address_book_entry
-- select create_distributed_table('address_book_entry', 'node_account_id', colocate_with => 'entity');

-- address_book_service_endpoint
-- select create_distributed_table('address_book_service_endpoint', 'node_id', colocate_with => 'entity');

-- contract_result, leave as local table until contract_result is updated with entity_id etc
-- select create_distributed_table('contract_result', 'entity_id', colocate_with => 'entity');

-- crypto_transfer
select create_distributed_table('crypto_transfer', 'entity_id', colocate_with => 'entity');

-- custom_fee
select create_distributed_table('custom_fee', 'token_id', colocate_with => 'entity');

-- event_file
select create_distributed_table('event_file', 'node_account_id', colocate_with => 'entity');

-- file_data
select create_distributed_table('file_data', 'entity_id', colocate_with => 'entity');

-- live_hash, leave as local table until live_hash is updated with entity_id etc
-- select create_distributed_table('live_hash', 'entity_id', colocate_with => 'entity');

-- nft
select create_distributed_table('nft', 'token_id', colocate_with => 'entity');

-- nft_transfer
select create_distributed_table('nft_transfer', 'token_id', colocate_with => 'entity');

-- non_fee_transfer
select create_distributed_table('non_fee_transfer', 'entity_id', colocate_with => 'entity');

-- record_file
select create_distributed_table('record_file', 'node_account_id', colocate_with => 'entity');

-- schedule
select create_distributed_table('schedule', 'schedule_id', colocate_with => 'entity');

-- t_entity_types serves only as a reference table and rarely gets updated, will be fully located on each node
select create_reference_table('t_entity_types');

-- t_transaction_results serves only as a reference table and rarely gets updated, will be fully located on each node
select create_reference_table('t_transaction_results');

-- t_transaction_types serves only as a reference table and rarely gets updated, will be fully located on each node
select create_reference_table('t_transaction_types');

-- token
select create_distributed_table('token', 'token_id', colocate_with => 'entity');

-- token_account
select create_distributed_table('token_account', 'token_id', colocate_with => 'entity');

-- token_balance
select create_distributed_table('token_balance', 'account_id', colocate_with => 'entity');

-- token_transfer
select create_distributed_table('token_transfer', 'token_id', colocate_with => 'entity');

-- topic_message
select create_distributed_table('topic_message', 'entity_id', colocate_with => 'entity');

-- transaction
-- unclear the right distribution column to use for transactions.
-- entity_id and payer_account_id were considered but full outer joins with non distribution columns aren't supported with citus
-- for now leave as growing list on coordinator node with old partitions being removed for hot path
--select create_distributed_table('transaction', 'payer_account_id', colocate_with => 'entity');

-- transaction_signature
select create_distributed_table('transaction_signature', 'entity_id', colocate_with => 'entity');
