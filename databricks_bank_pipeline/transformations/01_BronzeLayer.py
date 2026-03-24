from _config_pipeline import *

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


def clean_df(df):
   """Transformations de nettoyage communes aux deux tables."""
   return (df
       # Normalisation casse
       .withColumn("name",              upper(col("name")))
       .withColumn("email",             lower(col("email")))
       .withColumn("city",              upper(col("city")))
       .withColumn("preferred_channel", upper(col("preferred_channel")))
       .withColumn("income_range",      upper(col("income_range")))
       .withColumn("risk_segment",      upper(col("risk_segment")))
       # Standardisation genre
       .withColumn("gender", when(col("gender") == "M", "Male")
                             .when(col("gender") == "F", "Female")
                             .otherwise("UNKNOW"))
       # Status vide ou NULL → UNKNOW
       .withColumn("status", when(col("status").isNull() | (trim(col("status")) == ""), lit("UNKNOW"))
                             .otherwise(col("status")))
       # Nettoyage téléphone : supprime tout sauf chiffres et "+"
       .withColumn("phone_number", regexp_replace(trim(col("phone_number")), r'[^0-9\+]', ''))
       .withColumn("phone_number", col("phone_number").cast("string"))
       .withColumn("phone_number_length", length(col("phone_number")))
   )



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
    return  clean_df(dlt.read_stream("bank_landing_customers_incremental"))
   
  
# ─── Table quarantaine ───────────────────────────────────────────────────
@dlt.table(
   name="bank_bronze_customers_quarantine",
   comment="Rows that failed at least one validation rule",
)
def bank_bronze_customers_quarantine():
   df = clean_df(dlt.read_stream("bank_landing_customers_incremental"))
   
   failed_condition = " OR ".join([f"NOT ({v})" for v in VALID_RULES.values()])
   
   return (df
       .filter(expr(failed_condition))
       .withColumn("failed_rules",
           array_compact(array(*[
               when(~expr(condition), lit(rule_name))
               for rule_name, condition in VALID_RULES.items()
           ]))
       )
       .withColumn("created_at", current_timestamp())
   )
