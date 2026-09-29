# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview budget CSV structure
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC SELECT * FROM udw_procurement.bronze.budget LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Create Silver Budget
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.budget
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/budget/'
# MAGIC AS
# MAGIC WITH fx AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN target_currency = 'INR'
# MAGIC         THEN exchange_rate END)                     AS inr_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'EUR'
# MAGIC         THEN exchange_rate END)                     AS eur_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'SAR'
# MAGIC         THEN exchange_rate END)                     AS sar_per_usd
# MAGIC   FROM udw_procurement.bronze.fx_rates
# MAGIC   WHERE base_currency = 'USD'
# MAGIC )
# MAGIC SELECT
# MAGIC   b.cost_center,
# MAGIC   b.cost_center_name,
# MAGIC   b.gl_account,
# MAGIC   b.gl_description,
# MAGIC   b.fiscal_period,
# MAGIC   YEAR(TO_DATE(b.fiscal_period, 'yyyy-MM'))         AS fiscal_year,
# MAGIC   MONTH(TO_DATE(b.fiscal_period, 'yyyy-MM'))        AS fiscal_month,
# MAGIC   CAST(b.budget_amount AS DOUBLE)                   AS budget_amount,
# MAGIC   b.currency,
# MAGIC   -- Normalize to USD
# MAGIC   ROUND(
# MAGIC     CASE UPPER(b.currency)
# MAGIC       WHEN 'USD' THEN CAST(b.budget_amount AS DOUBLE)
# MAGIC       WHEN 'INR' THEN CAST(b.budget_amount AS DOUBLE) / fx.inr_per_usd
# MAGIC       WHEN 'EUR' THEN CAST(b.budget_amount AS DOUBLE) / fx.eur_per_usd
# MAGIC       WHEN 'SAR' THEN CAST(b.budget_amount AS DOUBLE) / fx.sar_per_usd
# MAGIC       ELSE CAST(b.budget_amount AS DOUBLE) / fx.inr_per_usd
# MAGIC     END, 2
# MAGIC   )                                                 AS budget_amount_usd,
# MAGIC   b.plant,
# MAGIC   b.company_code,
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'FLAT_FILE'                                       AS source_system
# MAGIC FROM udw_procurement.bronze.budget b
# MAGIC CROSS JOIN fx
# MAGIC WHERE b.cost_center IS NOT NULL
# MAGIC   AND b.fiscal_period IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 3 — Validate
# MAGIC SELECT
# MAGIC   fiscal_year,
# MAGIC   fiscal_month,
# MAGIC   currency,
# MAGIC   COUNT(DISTINCT cost_center)           AS cost_centers,
# MAGIC   COUNT(DISTINCT gl_account)            AS gl_accounts,
# MAGIC   ROUND(SUM(budget_amount), 0)          AS total_budget_local,
# MAGIC   ROUND(SUM(budget_amount_usd), 0)      AS total_budget_usd
# MAGIC FROM udw_procurement.silver.budget
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1, 2;