-------------------
-- cron schedule autovacuum if necessary
-------------------

vacuum account_balance;
vacuum crypto_transfer;
vacuum record_file;
vacuum token_balance;
vacuum token_transfer;
vacuum transaction;
