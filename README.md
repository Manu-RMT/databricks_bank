# 🏦 Bank Customer & Transactions – End‑to‑End Lakehouse Pipeline  
Pipeline complet de traitement de données bancaires construit sur **Databricks**, suivant une architecture **Lakehouse** moderne (Bronze → Silver → Gold), incluant ingestion, qualité, transformations, SCD, agrégations et visualisation finale.

---

## 🚀 Objectifs du projet
- Construire un pipeline **fiable, scalable et maintenable** pour des données bancaires (clients & transactions).
- Démontrer une architecture **Delta Live Tables (DLT)** avec :
 - Ingestion incrémentale
 - Nettoyage & qualité (Expectations)
 - Quarantaine automatique
 - Modélisation Silver (SCD1 / SCD2)
 - Tables Gold prêtes pour l’analytics
- Produire un **dashboard bancaire** complet (segmentation, risques, KPIs financiers).

---

## 🏗️ Architecture Lakehouse

### **1. Bronze – Ingestion & Qualité**
| Table | Description | Type |
|-------|-------------|------|
| `bank_landing_*` | Ingestion incrémentale brute | Streaming |
| `bank_bronze_*_ingestion_cleaned` | Nettoyage + Expectations | Streaming |
| `bank_bronze_*_quarantine` | Données rejetées | Streaming |

**Fonctionnalités clés :**
- Expectations DLT 
- Détection d’anomalies → redirection automatique en quarantaine
- Suivi des lignes droppées / unmet expectations

---

### **2. Silver – Modélisation & Historisation**
| Table | Description | Type |
|-------|-------------|------|
| `bank_silver_customers_scd1` | Historisation SCD1 | Streaming |
| `bank_silver_transaction_accounts_scd2` | Historisation SCD2 | Streaming |
| `bank_silver_*_transformed` | Tables nettoyées & enrichies | Streaming |

**Points forts :**
- Gestion des changements lents (SCD1 & SCD2)
- Normalisation & typage
- Jointures clients ↔ transactions

---

### **3. Gold – Tables analytiques**
| Table | Description | Type |
|-------|-------------|------|
| `bank_gold_cust_acc_trns_agg` | Agrégations clients / comptes / transactions | Materialized View |
| `bank_gold_cust_acc_trns_mv` | Vue analytique finale | Materialized View |

**Utilisation :**
- Dashboard BI
- KPIs financiers
- Segmentation & scoring

---

## 🔄 Pipeline DLT

Le pipeline **`task_databricks_bank_pipeline`** orchestre l’ensemble du flux :

- Exécution 100% verte ✔️  
- Dépendances gérées automatiquement  
- Monitoring complet : durée, lignes output, upserts, warnings, failed rows  

Le graphe DLT montre clairement la progression :  
**Landing → Bronze → Silver → Gold → Dashboard**

---

## 📊 Dashboard Analytics

Un dashboard bancaire complet a été construit à partir des tables Gold :

### **KPIs principaux**
- Total Customers : **102**
- Active / Inactive
- Total Credit : **11.01M**
- Total Debit : **-15.89k**
- Âge moyen : **43 ans**

### **Visualisations**
- Répartition par genre  
- Segmentation par risque (Low / Medium / High)  
- Évolution du crédit & débit par année  
- Table détaillée des clients (genre, ville, statut, revenu)

---

## 🧰 Technologies utilisées
- **Databricks** (Delta Live Tables, Workflows, Catalog)
- **Delta Lake**
- **PySpark / SQL**
- **Materialized Views**
- **Power BI / Databricks SQL Dashboard**
- **Lakehouse Architecture**

---

## 🧪 Data Quality – Expectations
Chaque table Bronze applique des règles de qualité :
- Validité des types
- Présence des colonnes obligatoires
- Valeurs non nulles
- Formats email / dates
- Ranges (âge, montants)
- Intégrité référentielle

Les lignes invalides sont automatiquement envoyées en **quarantaine**.

 
## Résultat 

<img width="1149" height="380" alt="Capture d&#39;écran 2026-03-27 161145" src="https://github.com/user-attachments/assets/3d4a0dc2-d1c2-4416-908c-fc9c24671205" />

---

<img width="1079" height="412" alt="Capture d&#39;écran 2026-03-27 144753" src="https://github.com/user-attachments/assets/f1cd9f7f-c69c-4836-a9b6-fdbe88b22b39" />

---

<img width="1675" height="602" alt="Capture d&#39;écran 2026-03-27 145914" src="https://github.com/user-attachments/assets/39c410d3-a2e9-4c31-9eeb-deb4faf08e51" />

---

 <img width="1717" height="863" alt="Capture d&#39;écran 2026-03-27 150046" src="https://github.com/user-attachments/assets/2be4aa42-12c4-4bff-9147-528c0a70ec2e" />
