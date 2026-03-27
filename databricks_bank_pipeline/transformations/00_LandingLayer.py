from _config_pipeline import *

####### Customers Data Ingestion ########

@dlt.table(
   name="bank_landing_customers_incremental",
   comment="Landing table for raw customers data ingested incrementally via Auto Loader"
)
def bank_landing_customers_incremental():
   # Chemins spécifiques à l'entité 'customers', définis localement
   # pour éviter tout écrasement par les variables du bloc suivant
   data_name = "customers"
   raw_source_path = f"{VOLUME_SOURCE_PATH}/{data_name}/"
   bronze_schema_autoload_path = f"{BRONZE_METADATA}schema_tracking/{data_name}/"

   # Schéma explicite pour éviter l'inférence automatique (plus fiable en production)
   customer_schema = StructType([
       StructField("customer_id", LongType(), False),       # Clé primaire, non nullable
       StructField("name", StringType(), True),
       StructField("dob", DateType(), True),                # Date de naissance
       StructField("gender", StringType(), True),
       StructField("city", StringType(), True),
       StructField("join_date", DateType(), True),          # Date d'adhésion
       StructField("status", StringType(), True),           # Ex: active, inactive
       StructField("email", StringType(), True),
       StructField("phone_number", StringType(), True),
       StructField("preferred_channel", StringType(), True),# Canal préféré (web, mobile...)
       StructField("occupation", StringType(), True),
       StructField("income_range", StringType(), True),     # Tranche de revenus
       StructField("risk_segment", StringType(), True)      # Segment de risque client
   ])

   return (
       spark.readStream.format("cloudFiles")       # Auto Loader pour ingestion incrémentale
       .option("cloudFiles.format", "csv")         # Format source : CSV
       .option("header", "true")                   # La 1ère ligne contient les en-têtes
       .schema(customer_schema)                    # Schéma imposé (pas d'inférence)
       .option("cloudFiles.schemaLocation", bronze_schema_autoload_path)  # Suivi du schéma entre runs
       .option("cloudFiles.includeExistingFiles", "true")  # Inclut les fichiers déjà présents au 1er run
       .load(raw_source_path)                      # Chargement depuis le volume source
   )


########### Account & Transactions Data Ingestion #######

@dlt.table(
   name="bank_landing_accounts_transactions_incremental",
   comment="Landing table for raw accounts and transactions data ingested incrementally via Auto Loader"
)
def bank_landing_accounts_transactions_incremental():
   # Chemins spécifiques à l'entité 'accounts', définis localement
   # pour éviter tout conflit avec les variables du bloc customers
   data_name = "accounts"
   raw_source_path = f"{VOLUME_SOURCE_PATH}/{data_name}/"
   bronze_schema_autoload_path = f"{BRONZE_METADATA}schema_tracking/{data_name}/"

   # Schéma mixte : données compte + données transaction dénormalisées dans le même fichier
   account_schema = StructType([
       StructField("account_id", LongType(), False),        # Clé primaire du compte
       StructField("customer_id", LongType(), False),       # Clé étrangère vers customers
       StructField("account_type", StringType(), True),     # Ex: savings, checking
       StructField("balance", DoubleType(), True),          # Solde du compte
       StructField("txn_id", LongType(), True),             # Identifiant de transaction
       StructField("txn_date", DateType(), True),           # Date de la transaction
       StructField("txn_type", StringType(), True),         # Ex: debit, credit
       StructField("txn_amount", DoubleType(), True),       # Montant de la transaction
       StructField("txn_channel", StringType(), True)       # Canal utilisé (ATM, online...)
   ])

   return (
       spark.readStream.format("cloudFiles")       # Auto Loader pour ingestion incrémentale
       .option("cloudFiles.format", "csv")         # Format source : CSV
       .option("header", "true")                   # La 1ère ligne contient les en-têtes
       .schema(account_schema)                     # Schéma imposé (pas d'inférence)
       .option("cloudFiles.includeExistingFiles", "true")  # Inclut les fichiers déjà présents au 1er run
       .option("cloudFiles.schemaLocation", bronze_schema_autoload_path)  # Suivi du schéma entre runs
       .load(raw_source_path)                      # Chargement depuis le volume source
   )

 