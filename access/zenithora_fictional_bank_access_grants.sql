GRANT USE CATALOG ON CATALOG zenithora_fictional_bank TO data_engineering_team;

GRANT USE SCHEMA ON SCHEMA zenithora_fictional_bank.cards TO data_engineering_team;
GRANT USE SCHEMA ON SCHEMA zenithora_fictional_bank.core_banking TO data_engineering_team;
GRANT USE SCHEMA ON SCHEMA zenithora_fictional_bank.credit TO data_engineering_team;
GRANT USE SCHEMA ON SCHEMA zenithora_fictional_bank.master TO data_engineering_team;
GRANT USE SCHEMA ON SCHEMA zenithora_fictional_bank.payments TO data_engineering_team;
GRANT USE SCHEMA ON SCHEMA zenithora_fictional_bank.reference TO data_engineering_team;

GRANT SELECT ON TABLE zenithora_fictional_bank.cards.cards TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.cards.card_transactions TO data_engineering_team;

GRANT SELECT ON TABLE zenithora_fictional_bank.core_banking.accounts TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.core_banking.daily_balances TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.core_banking.transactions TO data_engineering_team;

GRANT SELECT ON TABLE zenithora_fictional_bank.credit.credit_scores TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.credit.loan_payments TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.credit.loans TO data_engineering_team;

GRANT SELECT ON TABLE zenithora_fictional_bank.master.complaints TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.master.customers TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.master.employees TO data_engineering_team;

GRANT SELECT ON TABLE zenithora_fictional_bank.payments.imps_transactions TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.payments.upi_transactions TO data_engineering_team;

GRANT SELECT ON TABLE zenithora_fictional_bank.reference.atm_locations TO data_engineering_team;
GRANT SELECT ON TABLE zenithora_fictional_bank.reference.branches TO data_engineering_team;

SHOW GRANTS TO data_engineering_team;