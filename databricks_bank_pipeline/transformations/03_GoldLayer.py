from _config_pipeline import *

###### JOINED BETWEEN TRANSACTION ACCOUNT AND CUSTOMER TABLE #### 
#### Utilisation d'une vue matérialisée pour optimiser les performances (dlt.read) ### 
@dlt.table(
    name = f"{GOLD_ZONE}.bank_gold_cust_acc_trns_mv",
    comment = "Customer account transaction materialized view"
)

def bank_gold_cust_acc_trns_mv():
  customers_table = dlt.read(f"{SILVER_ZONE}.bank_silver_customers_scd1")
  txns_table = dlt.read(f"{SILVER_ZONE}.bank_silver_transaction_accounts_scd2").where(col('__END_AT').isNull())
  
  joined = customers_table.join(
                                txns_table, 
                                on = "customer_id",
                                how="inner"
                                )
  return joined


#######  AGGREGATION for the trades #######
@dlt.table(
    name = f"{GOLD_ZONE}.bank_gold_cust_acc_trns_agg",
    comment = "Gold Layer aggregation of customers + account transactions"
)

def bank_gold_cust_acc_trns_agg():
    df = dlt.read(f"{GOLD_ZONE}.bank_gold_cust_acc_trns_mv")

    return(
        
        df.groupBy("customer_id", "name", "gender", "city", "status", "income_range","risk_segment","customer_age","tenure_months","tenure_years")
          .agg(
              countDistinct("account_id").alias("account_count"),
              count("*").alias("txn_count"),
              round(
                  sum(when(
                        col("txn_type") == "CREDIT", col("txn_amount")
                      ).otherwise(lit(0.0))
                  )
                  ,2).alias("total_credit"),
              round(
                  sum(when(
                        col("txn_type") == "DEBIT", col("txn_amount")
                       ).otherwise(lit(0.0))
                  )
                  ,2).alias("total_debit"),
              round(avg(col("txn_amount")),2).alias("avg_txn_amount"),
              min("txn_date").alias("first_txn_date"),
              max("txn_date").alias("last_txn_date"),
              countDistinct("txn_channel").alias("channels_used")
          )         
    )
     