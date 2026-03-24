from _config_pipeline import *

########## Customers #########

@dlt.table(
    name= "bank_bronze_customers_ingestion_cleaned",
    comment="Contains the cleaned data from landing data",
)

# Expectations SI TRUE ON GARDE SINON ...
@dlt.expect_or_fail("valid_cusomer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_dob", " dob is NOT NULL")
@dlt.expect_or_drop("valid_city", "city IS NOT NULL")
@dlt.expect_or_drop("valid_join_date", "join_date IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9][a-zA-Z0-9._-]*@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$'")
@dlt.expect_or_drop("valid_phone", "phone_number IS NOT NULL")
@dlt.expect_or_drop("valid_channel", "preferred_channel IS NOT NULL")
@dlt.expect_or_drop("valid_occupation", "occupation IS NOT NULL")
@dlt.expect_or_drop("valid_income_range", "income_range IS NOT NULL")
@dlt.expect_or_drop("valid_risk_segment", "risk_segment IS NOT NULL")
@dlt.expect("valid_gender", "gender IS NOT NULL")
@dlt.expect("status", "status IS NOT NULL")

def bank_bronze_customers_ingestion_cleaned():
    df = dlt.read_stream("bank_landing_customers_incremental")

    # Cleaning Data
    df = df.withColumn("name",upper(col("name"))) \
           .withColumn("email",lower(col("email"))) \
           .withColumn("city",upper(col("city"))) \
           .withColumn("preferred_channel",upper(col("preferred_channel"))) \
           .withColumn("income_range",upper(col("income_range"))) \
           .withColumn("risk_segment",upper(col("risk_segment")))

    df = df.withColumn("gender", when(col("gender") == "M", "Male")
                                  .when(col("gender") == "F", "Female")
                                  .otherwise("UNKNOW"))
    df = df.withColumn("status", when(col("status").isNull() | (trim(col("status")) == ""), lit("UNKNOW"))
                                  .otherwise(col("status")))
    
    df = df.withColumn("phone_number", trim(col("phone_number"))) 
    df = df.withColumn("phone_number", regexp_replace(col("phone_number"), r'[^0-9\+]', '')) 
    ## filter
    df = df.filter(col("phone_number").rlike(r"^\+44\d{10}$"))
    df = df.filter(col("preferred_channel").isin("ONLINE","MOBILE","BRANCH","ATM"))
    df = df.filter(col("income_range").isin("HIGH","MEDIUM","LOW","VERY_HIGH"))
    df = df.filter(col("risk_segment").isin("LOW","MEDIUM","HIGH","UNKNOW"))

    return df

