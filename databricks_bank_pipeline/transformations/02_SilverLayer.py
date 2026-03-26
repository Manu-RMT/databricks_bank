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


################################ Silver Transaction Accounts 

@dlt.table(
    name =
)