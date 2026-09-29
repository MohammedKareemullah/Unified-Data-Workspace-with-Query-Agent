# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview actual field names
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC SELECT * FROM udw_procurement.bronze.sap_journal_entries_custom LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.bronze.sap_journal_entries_custom

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE udw_procurement.silver.budget;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Gold Budget vs Actuals
# MAGIC -- Budget from Silver.budget (CSV flat file)
# MAGIC -- Actuals from Silver.journal_entries (ACDOCA custom OData)
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.budget_vs_actuals
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/budget_vs_actuals/'
# MAGIC AS
# MAGIC WITH je_actuals AS (
# MAGIC   -- Aggregate journal entry actuals by cost center,
# MAGIC   -- GL account and fiscal period
# MAGIC   SELECT
# MAGIC     CostCenter                                      AS cost_center,
# MAGIC     GLAccount                                       AS gl_account,
# MAGIC     FiscalYear                                      AS fiscal_year,
# MAGIC     CAST(FiscalPeriod AS INT)                       AS fiscal_period_num,
# MAGIC     TO_DATE(CONCAT(
# MAGIC       FiscalYear, '-',
# MAGIC       LPAD(CAST(FiscalPeriod AS INT), 2, '0'), '-01'
# MAGIC     ), 'yyyy-MM-dd')                                AS fiscal_period_date,
# MAGIC     CompanyCode                                     AS company_code,
# MAGIC     TransactionCurrency                             AS currency,
# MAGIC     -- Signed amount: S=debit (cost), H=credit (reversal)
# MAGIC     ROUND(SUM(
# MAGIC       CASE DebitCreditCode
# MAGIC         WHEN 'S' THEN CAST(AmountInTransactionCurrency AS DOUBLE)
# MAGIC         WHEN 'H' THEN -CAST(AmountInTransactionCurrency AS DOUBLE)
# MAGIC         ELSE CAST(AmountInTransactionCurrency AS DOUBLE)
# MAGIC       END
# MAGIC     ), 2)                                           AS actual_amount_local,
# MAGIC     COUNT(DISTINCT AccountingDocument)              AS posting_count,
# MAGIC     COUNT(DISTINCT Supplier)                        AS distinct_vendors
# MAGIC   FROM udw_procurement.bronze.sap_journal_entries_custom
# MAGIC   WHERE CostCenter IS NOT NULL
# MAGIC     AND CostCenter != ''
# MAGIC     AND GLAccount IS NOT NULL
# MAGIC     AND AccountingDocument IS NOT NULL
# MAGIC     AND FiscalPeriod IS NOT NULL
# MAGIC     AND CAST(FiscalPeriod AS INT) BETWEEN 1 AND 12
# MAGIC   GROUP BY
# MAGIC     CostCenter, GLAccount, FiscalYear,
# MAGIC     FiscalPeriod, CompanyCode, TransactionCurrency
# MAGIC ),
# MAGIC -- Normalize actuals to USD using FX rates
# MAGIC fx AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN target_currency = 'INR'
# MAGIC         THEN exchange_rate END)                     AS inr_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'EUR'
# MAGIC         THEN exchange_rate END)                     AS eur_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'SAR'
# MAGIC         THEN exchange_rate END)                     AS sar_per_usd
# MAGIC   FROM udw_procurement.bronze.fx_rates
# MAGIC   WHERE base_currency = 'USD'
# MAGIC ),
# MAGIC actuals_usd AS (
# MAGIC   SELECT
# MAGIC     a.*,
# MAGIC     ROUND(
# MAGIC       CASE UPPER(a.currency)
# MAGIC         WHEN 'USD' THEN a.actual_amount_local
# MAGIC         WHEN 'INR' THEN a.actual_amount_local / fx.inr_per_usd
# MAGIC         WHEN 'EUR' THEN a.actual_amount_local / fx.eur_per_usd
# MAGIC         WHEN 'SAR' THEN a.actual_amount_local / fx.sar_per_usd
# MAGIC         ELSE a.actual_amount_local / fx.inr_per_usd
# MAGIC       END, 2
# MAGIC     )                                               AS actual_amount_usd
# MAGIC   FROM je_actuals a
# MAGIC   CROSS JOIN fx
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   -- Budget dimensions
# MAGIC   b.cost_center,
# MAGIC   b.cost_center_name,
# MAGIC   b.gl_account,
# MAGIC   b.gl_description,
# MAGIC   b.fiscal_period                                   AS fiscal_period_str,
# MAGIC   b.fiscal_year,
# MAGIC   b.fiscal_month,
# MAGIC   b.currency                                        AS budget_currency,
# MAGIC   b.plant,
# MAGIC   b.company_code,
# MAGIC
# MAGIC   -- Budget amounts
# MAGIC   ROUND(b.budget_amount, 2)                         AS budget_amount_local,
# MAGIC   ROUND(b.budget_amount_usd, 2)                     AS budget_amount_usd,
# MAGIC
# MAGIC   -- Actual amounts
# MAGIC   COALESCE(a.actual_amount_local, 0)                AS actual_amount_local,
# MAGIC   COALESCE(a.actual_amount_usd, 0)                  AS actual_amount_usd,
# MAGIC   COALESCE(a.posting_count, 0)                      AS posting_count,
# MAGIC   COALESCE(a.distinct_vendors, 0)                   AS distinct_vendors,
# MAGIC   a.currency                                        AS actual_currency,
# MAGIC
# MAGIC   -- Variance (positive = overspend, negative = underspend)
# MAGIC   ROUND(
# MAGIC     COALESCE(a.actual_amount_usd, 0) -
# MAGIC     b.budget_amount_usd, 2
# MAGIC   )                                                 AS variance_usd,
# MAGIC
# MAGIC   -- Variance percentage
# MAGIC   CASE
# MAGIC     WHEN b.budget_amount_usd != 0
# MAGIC     THEN ROUND(
# MAGIC       (COALESCE(a.actual_amount_usd, 0) -
# MAGIC        b.budget_amount_usd) /
# MAGIC       ABS(b.budget_amount_usd) * 100, 2
# MAGIC     )
# MAGIC     ELSE NULL
# MAGIC   END                                               AS variance_pct,
# MAGIC
# MAGIC   -- Utilization percentage
# MAGIC   CASE
# MAGIC     WHEN b.budget_amount_usd != 0
# MAGIC     THEN ROUND(
# MAGIC       COALESCE(a.actual_amount_usd, 0) /
# MAGIC       ABS(b.budget_amount_usd) * 100, 2
# MAGIC     )
# MAGIC     ELSE NULL
# MAGIC   END                                               AS utilization_pct,
# MAGIC   
# MAGIC   CASE
# MAGIC     WHEN COALESCE(a.actual_amount_usd, 0) >
# MAGIC b.budget_amount_usd * 1.10                THEN 'OVER BUDGET'
# MAGIC     WHEN COALESCE(a.actual_amount_usd, 0) >
# MAGIC          b.budget_amount_usd * 0.50                THEN 'SIGNIFICANTLY UNDER'
# MAGIC     WHEN COALESCE(a.actual_amount_usd, 0) >
# MAGIC          b.budget_amount_usd * 0.80                THEN 'UNDER BUDGET'
# MAGIC     WHEN COALESCE(a.actual_amount_usd, 0) = 0
# MAGIC          AND b.budget_amount_usd > 0               THEN 'NO ACTUALS YET'
# MAGIC     ELSE                                                'ON TRACK'
# MAGIC   END                                               AS budget_status,
# MAGIC
# MAGIC   -- Data source flag for transparency
# MAGIC   CASE
# MAGIC     WHEN a.actual_amount_usd IS NOT NULL
# MAGIC     THEN 'SAP_ACDOCA'
# MAGIC     ELSE 'NO_ACTUALS'
# MAGIC   END                                               AS actual_data_source,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS run_date
# MAGIC
# MAGIC FROM udw_procurement.silver.budget b
# MAGIC LEFT JOIN actuals_usd a
# MAGIC   ON  b.cost_center  = a.cost_center
# MAGIC   AND b.gl_account   = a.gl_account
# MAGIC   AND b.fiscal_period = a.fiscal_period_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 2 — Validate
# MAGIC SELECT
# MAGIC   budget_status,
# MAGIC   actual_data_source,
# MAGIC   fiscal_year,
# MAGIC   COUNT(DISTINCT cost_center)           AS cost_centers,
# MAGIC   COUNT(*)                              AS period_line_items,
# MAGIC   ROUND(SUM(budget_amount_usd), 2)      AS total_budget_usd,
# MAGIC   ROUND(SUM(actual_amount_usd), 2)      AS total_actual_usd,
# MAGIC   ROUND(SUM(variance_usd), 2)           AS total_variance_usd,
# MAGIC   ROUND(AVG(utilization_pct), 1)        AS avg_utilization_pct
# MAGIC FROM udw_procurement.gold.budget_vs_actuals
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY fiscal_year DESC, total_variance_usd DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- %sql
# MAGIC -- -- CELL 3 — Cost center summary for dashboard
# MAGIC -- SELECT
# MAGIC --   cost_center,
# MAGIC --   cost_center_name,
# MAGIC --   fiscal_year,
# MAGIC --   ROUND(SUM(budget_amount_usd), 2)      AS total_budget_usd,
# MAGIC --   ROUND(SUM(actual_amount_usd), 2)      AS total_actual_usd,
# MAGIC --   ROUND(SUM(variance_usd), 2)           AS total_variance_usd,
# MAGIC --   ROUND(AVG(utilization_pct), 1)        AS avg_utilization_pct,
# MAGIC --   SUM(posting_count)                    AS total_postings,
# MAGIC --   budget_status
# MAGIC -- FROM udw_procurement.gold.budget_vs_actuals
# MAGIC -- GROUP BY 1, 2, 3, 9
# MAGIC --
# MAGIC -- ORDER BY ABS(SUM(variance_usd)) DESC;
# MAGIC
# MAGIC SELECT 
# MAGIC actual_amount_usd,
# MAGIC actual_amount_local
# MAGIC from udw_procurement.gold.budget_vs_actuals;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE udw_procurement.gold.budget_vs_actuals;

# COMMAND ----------

