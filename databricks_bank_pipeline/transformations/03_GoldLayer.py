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
