from pyspark.sql.functions import udf
from _config_pipeline import *

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None

#
# Nettoage des données clients (to BronzeLayer)
#
def clean_customers_df(df):
   """Transformations de nettoyage communes aux deux tables."""
   return (df
       # Normalisation casse
       .withColumn("name",              upper(col("name")))
       .withColumn("email",             lower(col("email")))
       .withColumn("city",              upper(col("city")))
       .withColumn("preferred_channel", upper(col("preferred_channel")))
       .withColumn("income_range",      upper(col("income_range")))
       .withColumn("risk_segment",      upper(col("risk_segment")))
       .withColumn("status",            upper(col("status")))
      
       # Standardisation genre
       .withColumn("gender", when(col("gender") == "M", "MALE")
                             .when(col("gender") == "F", "FEMALE")
                             .otherwise("UNKNOW"))
       # Status vide ou NULL → UNKNOW
       .withColumn("status", when(col("status").isNull() | (trim(col("status")) == ""), lit("UNKNOW"))
                             .otherwise(col("status")))
       # Nettoyage téléphone : supprime tout sauf chiffres et "+"
       .withColumn("phone_number", regexp_replace(trim(col("phone_number")), r'[^0-9\+]', ''))
       .withColumn("phone_number", col("phone_number").cast("string"))
       .withColumn("phone_number_length", length(col("phone_number")))
       .withColumn("inserted_at", current_timestamp())
       
   )


#
# Nettoyage des données comptes et transactions (to BronzeLayer)
#
def clean_accounts_transactions_df(df):
    return(df
    
    .withColumn("txn_channel", upper(col("txn_channel")))
    .withColumn("account_type", upper(col("account_type")))
    .withColumn("txn_type", upper(col("txn_type")))
    
    # Txn Type
    .withColumn("txn_type", when(col("txn_type") == "DEBITT", "DEBIT")
                            .when(col("txn_type") == "CREDIIT", "CREDIT")
                            .otherwise(col("txn_type")))
    # Txn_channel
    .withColumn("txn_channel", when(col("txn_channel").isNull() | (trim(col("txn_channel")) == ""), lit("UNKNOW"))
                             .otherwise(col("txn_channel")))
    
    .withColumn("inserted_at", current_timestamp())
    
    )


#
# Table de quarantaine des données incorrectes (to BronzeLayer)
#
def quarantaine_table(df, VALID_RULES):
   """Création de la table de quarantaine."""
   failed_condition = " OR ".join([f"NOT ({v})" for v in VALID_RULES.values()])
   
   return (df
       .filter(expr(failed_condition))
       .withColumn("failed_rules",
           array_compact(array(*[
               when(~expr(condition), lit(rule_name))
               for rule_name, condition in VALID_RULES.items()
           ]))
       )
   )
