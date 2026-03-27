from _config_pipeline import *
from utilities.utils import * 


@dlt.table(
    name=f"{SILVER_ZONE}.bank_silver_customers_transformed",
    comment="Transformed customers table"
)

def bank_silver_customers_transformed():
    df = spark.readStream.table("bank_bronze_customers_ingestion_cleaned")
    return (
        df
        .withColumn("customer_age", 
                        when(
                            col("dob").isNotNull(),
                            floor(months_between(current_date(), col("dob")) / 12)
                        ).otherwise(lit(None))
                    )
        .withColumn("tenure_days", 
                        when(
                            col("join_date").isNotNull(),
                            datediff(current_date(), col("join_date"))
                        ).otherwise(lit(None))
                    )
        .withColumn("tenure_months",
                        when(
                            col("join_date").isNotNull(),
                            round(months_between(current_date(), col("join_date")))
                        )
                     )
        .withColumn("tenure_years", 
                        when(
                            col("join_date").isNotNull(),
                            round(months_between(current_date(), col("join_date")) / 12)
                        ).otherwise(lit(None))
                    )
        .withColumn("dof_out_of_range_date_flag",(col("dob") < lit ("1900-01-01")) | (col("dob") > current_date()))
        .withColumn("transformation_date", current_timestamp())

    )


######## DATA TRANSFORMED : OK - SCD 1 - CUSTOMERS

dlt.create_streaming_table(
    name = f"{SILVER_ZONE}.bank_silver_customers_scd1",
    comment = "Silver customers table - SCD 1"
)

# nouveau apply_changes() - Ecrasement des données
dlt.create_auto_cdc_flow(
    target = f"{SILVER_ZONE}.bank_silver_customers_scd1",
    source = f"{SILVER_ZONE}.bank_silver_customers_transformed",
    keys = ["customer_id"],
    sequence_by = col("transformation_date"),
    except_column_list=["transformation_date","inserted_at"],
    stored_as_scd_type = 1
)

################################ Silver Transaction Accounts 

@dlt.table(
    name = f"{SILVER_ZONE}.bank_silver_transaction_accounts_transformed",
    comment = "Silver Transaction Accounts"
)


def bank_silver_transaction_accounts_transformed():
    df = spark.readStream.table("bank_bronze_accounts_transactions_ingestion_cleaned")
    return(
        df
        # filtre digital ou physique
        .withColumn("channel_type",
                        when(
                            (col("txn_channel") == "ATM") | (col("txn_channel") == "BRANCH"),
                            lit("PHYSICAL") 
                        ).otherwise("DIGITAL")
                    )
        # txn année, mois, jour
        .withColumn("txn_year", year(col("txn_date")))
        .withColumn("txn_month", month(col("txn_date")))
        .withColumn("txn_day", dayofmonth(col("txn_date")))
        # direction transaction
        .withColumn("txn_direction",
                        when(
                            col("txn_type") == 'DEBIT',
                            lit("OUT")
                        ).otherwise("IN")
                    )
        # date de transformation
        .withColumn("transformation_date", current_timestamp())
    )


######### DATA TRANSFORMED : OK - SCD 2 - TRANSACTION ACCOUNTS

dlt.create_streaming_table(
    name = f"{SILVER_ZONE}.bank_silver_transaction_accounts_scd2",
    comment = "Silver Transaction Accounts - SCD 2"
)

dlt.create_auto_cdc_flow(
    target = f"{SILVER_ZONE}.bank_silver_transaction_accounts_scd2",
    source = f"{SILVER_ZONE}.bank_silver_transaction_accounts_transformed",
    keys = ["txn_id"],
    sequence_by = col("transformation_date"),
    except_column_list=["transformation_date","inserted_at"],
    stored_as_scd_type = 2
)


    