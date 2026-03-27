from _config_pipeline import *
from utilities.utils import * 
########## Customers ##########

# ─── Règles de validation DLT ───────────────────────────────────────────
VALID_RULES = {
    "valid_customer_id":   "customer_id IS NOT NULL",
    "valid_customer_name": "name IS NOT NULL",
    "valid_dob":           "dob IS NOT NULL",
    "valid_city":          "city IS NOT NULL",
    "valid_join_date":     "join_date IS NOT NULL",
    "valid_email":         r"email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$'",
    "valid_phone":         "phone_number IS NOT NULL",
    "valid_channel":       "preferred_channel IS NOT NULL AND preferred_channel IN ('ONLINE', 'MOBILE', 'BRANCH', 'ATM')",
    "valid_occupation":    "occupation IS NOT NULL",
    "valid_income_range":  "income_range IS NOT NULL AND income_range IN ('HIGH', 'MEDIUM', 'LOW', 'VERY_HIGH')",
    "valid_risk_segment":  "risk_segment IS NOT NULL AND risk_segment IN ('LOW', 'MEDIUM', 'HIGH', 'UNKNOW')",
    "valid_gender":        "gender IS NOT NULL",
    "valid_status":        "status IS NOT NULL",
    "valid_phone_number_lenght": "phone_number_length = 13"
    
}


# ─── Table principale ────────────────────────────────────────────────────
@dlt.table(
   name="bank_bronze_customers_ingestion_cleaned",
   comment="Contains the cleaned data from landing data",
)
@dlt.expect_or_fail("valid_customer_id",   VALID_RULES["valid_customer_id"])
@dlt.expect_or_drop("valid_customer_name", VALID_RULES["valid_customer_name"])
@dlt.expect_or_drop("valid_dob",           VALID_RULES["valid_dob"])
@dlt.expect_or_drop("valid_city",          VALID_RULES["valid_city"])
@dlt.expect_or_drop("valid_join_date",     VALID_RULES["valid_join_date"])
@dlt.expect_or_drop("valid_email",         VALID_RULES["valid_email"])
@dlt.expect_or_drop("valid_phone",         VALID_RULES["valid_phone"])
@dlt.expect_or_drop("valid_channel",       VALID_RULES["valid_channel"])
@dlt.expect_or_drop("valid_occupation",    VALID_RULES["valid_occupation"])
@dlt.expect_or_drop("valid_income_range",  VALID_RULES["valid_income_range"])
@dlt.expect_or_drop("valid_risk_segment",  VALID_RULES["valid_risk_segment"])
@dlt.expect("valid_gender", VALID_RULES["valid_gender"])
@dlt.expect("valid_status", VALID_RULES["valid_status"])      
@dlt.expect_or_drop("valid_phone_number_lenght", VALID_RULES["valid_phone_number_lenght"])

def bank_bronze_customers_ingestion_cleaned():
    return  clean_customers_df(dlt.read_stream("bank_landing_customers_incremental"))
   
  
# ─── Table quarantaine ───────────────────────────────────────────────────
@dlt.table(
   name="bank_bronze_customers_quarantine",
   comment="Rows that failed at least one validation rule",
)
def bank_bronze_customers_quarantine():
   df = clean_customers_df(dlt.read_stream("bank_landing_customers_incremental"))
   return quarantaine_table(df, VALID_RULES)
   



################# Accounts Transactions ##########   

VALID_RULES_ACCOUNTS = {
    "valid_account_id":   "account_id IS NOT NULL",
    "valid_customer_id":  "customer_id IS NOT NULL",
    "valid_account_type": "account_type IS NOT NULL",
    "valid_balance":      "balance IS NOT NULL",
    "valid_txn_id":       "txn_id IS NOT NULL",
    "valid_txn_date":     "txn_date IS NOT NULL",
    "valid_txn_amount":   "txn_amount IS NOT NULL",
    "valid_txn_channel":  "txn_channel IS NOT NULL",
    "valid_txn_type":     "txn_type IS NOT NULL AND txn_type IN ('DEBIT', 'CREDIT')",
    "valid_transaction":  "(txn_type = 'DEBIT' AND txn_amount < 0) OR (txn_type = 'CREDIT' AND txn_amount > 0)"
}


@dlt.table(
   name="bank_bronze_accounts_transactions_ingestion_cleaned",
   comment="Contains the cleaned data from landing data",
)


@dlt.expect_or_fail("valid_account_id",   VALID_RULES_ACCOUNTS["valid_account_id"])
@dlt.expect_or_drop("valid_customer_id",  VALID_RULES_ACCOUNTS["valid_customer_id"])
@dlt.expect_or_drop("valid_account_type", VALID_RULES_ACCOUNTS["valid_account_type"])
@dlt.expect_or_drop("valid_balance",      VALID_RULES_ACCOUNTS["valid_balance"])
@dlt.expect_or_drop("valid_txn_id",       VALID_RULES_ACCOUNTS["valid_txn_id"])
@dlt.expect_or_drop("valid_txn_date",     VALID_RULES_ACCOUNTS["valid_txn_date"])
@dlt.expect_or_drop("valid_txn_amount",   VALID_RULES_ACCOUNTS["valid_txn_amount"])
@dlt.expect_or_drop("valid_txn_channel",  VALID_RULES_ACCOUNTS["valid_txn_channel"])
@dlt.expect_or_drop("valid_txn_type",     VALID_RULES_ACCOUNTS["valid_txn_type"])
@dlt.expect_or_drop("valid_transaction",  VALID_RULES_ACCOUNTS["valid_transaction"])


def bank_bronze_accounts_transactions_ingestion_cleaned():
    return clean_accounts_transactions_df(dlt.read_stream("bank_landing_accounts_transactions_incremental"))


@dlt.table(
   name="bank_bronze_accounts_transactions_quarantine",
   comment="Rows that failed at least one validation rule",
)

def bank_bronze_accounts_transactions_quarantine():
   df = clean_accounts_transactions_df(dlt.read_stream("bank_landing_accounts_transactions_incremental"))
   return quarantaine_table(df, VALID_RULES_ACCOUNTS)
