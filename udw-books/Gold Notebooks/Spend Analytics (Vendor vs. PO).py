# Databricks notebook source
# MAGIC %sql 
# MAGIC DESCRIBE udw_procurement.bronze.fx_rates;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(vendor_id) from udw_procurement.silver.purchase_orders WHERE vendor_id = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Gold: Spend Analytics
# MAGIC -- UC-02: Vendor spend aggregated by month, category, currency
# MAGIC -- Agent use: "What is our total spend with Tata Steel?"
# MAGIC --            "Which vendors represent 80% of our spend?"
# MAGIC --            "Show me spend trend by material group last 6 months"
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC -- Get FX rates for normalization
# MAGIC WITH fx AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN target_currency = 'INR' THEN exchange_rate END) AS inr_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'EUR' THEN exchange_rate END) AS eur_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'SAR' THEN exchange_rate END) AS sar_per_usd
# MAGIC   FROM udw_procurement.bronze.fx_rates
# MAGIC   WHERE base_currency = 'USD'
# MAGIC )
# MAGIC SELECT
# MAGIC   p.vendor_id,
# MAGIC   COALESCE(v.vendor_name, p.vendor_id)              AS vendor_name,
# MAGIC   COALESCE(v.vendor_category, 'Unknown')            AS vendor_category,
# MAGIC   COALESCE(v.country, p.supplier_country)           AS vendor_country,
# MAGIC   COALESCE(v.payment_terms, p.payment_terms)        AS payment_terms,
# MAGIC   COALESCE(v.incoterms, p.incoterms)                AS incoterms,
# MAGIC   p.material_group,
# MAGIC   p.company_code,
# MAGIC   p.purchasing_org,
# MAGIC   p.plant,
# MAGIC   p.currency                                        AS original_currency,
# MAGIC   DATE_TRUNC('MONTH', p.po_creation_date)           AS spend_month,
# MAGIC   YEAR(p.po_creation_date)                          AS spend_year,
# MAGIC   MONTH(p.po_creation_date)                         AS spend_month_num,
# MAGIC   -- Aggregations
# MAGIC   COUNT(DISTINCT p.po_number)                       AS po_count,
# MAGIC   COUNT(p.po_item)                                  AS line_item_count,
# MAGIC   ROUND(SUM(p.net_amount), 2)                       AS total_spend_original_currency,
# MAGIC   -- Normalize to USD
# MAGIC   ROUND(SUM(
# MAGIC     CASE p.currency
# MAGIC       WHEN 'USD' THEN p.net_amount
# MAGIC       WHEN 'INR' THEN p.net_amount / fx.inr_per_usd
# MAGIC       WHEN 'EUR' THEN p.net_amount / fx.eur_per_usd
# MAGIC       WHEN 'SAR' THEN p.net_amount / fx.sar_per_usd
# MAGIC       ELSE p.net_amount / fx.inr_per_usd
# MAGIC     END
# MAGIC   ), 2)                                             AS total_spend_usd,
# MAGIC   ROUND(AVG(
# MAGIC     CASE p.currency
# MAGIC       WHEN 'USD' THEN p.net_amount
# MAGIC       WHEN 'INR' THEN p.net_amount / fx.inr_per_usd
# MAGIC       WHEN 'EUR' THEN p.net_amount / fx.eur_per_usd
# MAGIC       WHEN 'SAR' THEN p.net_amount / fx.sar_per_usd
# MAGIC       ELSE p.net_amount / fx.inr_per_usd
# MAGIC     END
# MAGIC   ), 2)                                             AS avg_po_value_usd,
# MAGIC   -- Fulfillment metrics
# MAGIC   SUM(CASE WHEN p.is_fully_received THEN 1 ELSE 0 END) AS fully_received_pos,
# MAGIC   SUM(CASE WHEN p.is_fully_invoiced THEN 1 ELSE 0 END) AS fully_invoiced_pos,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN p.is_fully_received THEN 1 ELSE 0 END) * 100.0 /
# MAGIC     COUNT(DISTINCT p.po_number), 2
# MAGIC   )                                                 AS on_time_delivery_pct,
# MAGIC   -- Delivery status counts
# MAGIC   SUM(CASE WHEN p.delivery_status = 'COMPLETED'
# MAGIC       THEN 1 ELSE 0 END)                            AS completed_line_items,
# MAGIC   SUM(CASE WHEN p.delivery_status = 'OVERDUE'
# MAGIC       THEN 1 ELSE 0 END)                            AS overdue_line_items,
# MAGIC   SUM(CASE WHEN p.delivery_status = 'PENDING'
# MAGIC       THEN 1 ELSE 0 END)                            AS pending_line_items,
# MAGIC   CURRENT_DATE()                                    AS run_date
# MAGIC FROM udw_procurement.silver.purchase_orders p
# MAGIC CROSS JOIN fx
# MAGIC LEFT JOIN udw_procurement.silver.vendors v
# MAGIC   ON p.vendor_id = v.vendor_id
# MAGIC WHERE p.po_creation_date IS NOT NULL and p.vendor_id != ''
# MAGIC GROUP BY
# MAGIC   p.vendor_id, v.vendor_name, v.vendor_category,
# MAGIC   v.country, p.supplier_country, v.payment_terms,
# MAGIC   p.payment_terms, v.incoterms, p.incoterms,
# MAGIC   p.material_group, p.company_code, p.purchasing_org,
# MAGIC   p.plant, p.currency,
# MAGIC   DATE_TRUNC('MONTH', p.po_creation_date),
# MAGIC   YEAR(p.po_creation_date), MONTH(p.po_creation_date),
# MAGIC   fx.inr_per_usd, fx.eur_per_usd, fx.sar_per_usd;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Save as Gold Delta table
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.spend_analytics
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/spend_analytics/'
# MAGIC AS
# MAGIC WITH fx AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN target_currency = 'INR' THEN exchange_rate END) AS inr_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'EUR' THEN exchange_rate END) AS eur_per_usd,
# MAGIC     MAX(CASE WHEN target_currency = 'SAR' THEN exchange_rate END) AS sar_per_usd
# MAGIC   FROM udw_procurement.bronze.fx_rates
# MAGIC   WHERE base_currency = 'USD'
# MAGIC )
# MAGIC SELECT
# MAGIC   p.vendor_id,
# MAGIC   COALESCE(v.vendor_name, p.vendor_id)              AS vendor_name,
# MAGIC   COALESCE(v.vendor_category, 'Unknown')            AS vendor_category,
# MAGIC   COALESCE(v.country, p.supplier_country)           AS vendor_country,
# MAGIC   COALESCE(v.payment_terms, p.payment_terms)        AS payment_terms,
# MAGIC   p.material_group,
# MAGIC   p.company_code,
# MAGIC   p.purchasing_org,
# MAGIC   p.plant,
# MAGIC   p.currency                                        AS original_currency,
# MAGIC   DATE_TRUNC('MONTH', p.po_creation_date)           AS spend_month,
# MAGIC   YEAR(p.po_creation_date)                          AS spend_year,
# MAGIC   MONTH(p.po_creation_date)                         AS spend_month_num,
# MAGIC   COUNT(DISTINCT p.po_number)                       AS po_count,
# MAGIC   COUNT(p.po_item)                                  AS line_item_count,
# MAGIC   ROUND(SUM(p.net_amount), 2)                       AS total_spend_original_currency,
# MAGIC   ROUND(SUM(
# MAGIC     CASE p.currency
# MAGIC       WHEN 'USD' THEN p.net_amount
# MAGIC       WHEN 'INR' THEN p.net_amount / fx.inr_per_usd
# MAGIC       WHEN 'EUR' THEN p.net_amount / fx.eur_per_usd
# MAGIC       WHEN 'SAR' THEN p.net_amount / fx.sar_per_usd
# MAGIC       ELSE p.net_amount / fx.inr_per_usd
# MAGIC     END
# MAGIC   ), 2)                                             AS total_spend_usd,
# MAGIC   ROUND(AVG(
# MAGIC     CASE p.currency
# MAGIC       WHEN 'USD' THEN p.net_amount
# MAGIC       WHEN 'INR' THEN p.net_amount / fx.inr_per_usd
# MAGIC       WHEN 'EUR' THEN p.net_amount / fx.eur_per_usd
# MAGIC       WHEN 'SAR' THEN p.net_amount / fx.sar_per_usd
# MAGIC       ELSE p.net_amount / fx.inr_per_usd
# MAGIC     END
# MAGIC   ), 2)                                             AS avg_po_value_usd,
# MAGIC   SUM(CASE WHEN p.is_fully_received
# MAGIC       THEN 1 ELSE 0 END)                            AS fully_received_pos,
# MAGIC   SUM(CASE WHEN p.is_fully_invoiced
# MAGIC       THEN 1 ELSE 0 END)                            AS fully_invoiced_pos,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN p.is_fully_received
# MAGIC         THEN 1 ELSE 0 END) * 100.0 /
# MAGIC     COUNT(DISTINCT p.po_number), 2
# MAGIC   )                                                 AS on_time_delivery_pct,
# MAGIC   SUM(CASE WHEN p.delivery_status = 'COMPLETED'
# MAGIC       THEN 1 ELSE 0 END)                            AS completed_items,
# MAGIC   SUM(CASE WHEN p.delivery_status = 'OVERDUE'
# MAGIC       THEN 1 ELSE 0 END)                            AS overdue_items,
# MAGIC   SUM(CASE WHEN p.delivery_status = 'PENDING'
# MAGIC       THEN 1 ELSE 0 END)                            AS pending_items,
# MAGIC   CURRENT_DATE()                                    AS run_date
# MAGIC FROM udw_procurement.silver.purchase_orders p
# MAGIC CROSS JOIN fx
# MAGIC LEFT JOIN udw_procurement.silver.vendors v ON p.vendor_id = v.vendor_id
# MAGIC WHERE p.po_creation_date IS NOT NULL and p.vendor_id != ''
# MAGIC GROUP BY
# MAGIC   p.vendor_id, v.vendor_name, v.vendor_category,
# MAGIC   v.country, p.supplier_country, v.payment_terms,
# MAGIC   p.payment_terms, p.material_group, p.company_code,
# MAGIC   p.purchasing_org, p.plant, p.currency,
# MAGIC   DATE_TRUNC('MONTH', p.po_creation_date),
# MAGIC   YEAR(p.po_creation_date), MONTH(p.po_creation_date),
# MAGIC   fx.inr_per_usd, fx.eur_per_usd, fx.sar_per_usd;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select total_spend_usd from udw_procurement.gold.spend_analytics

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- it groups by the 1st, 2nd, 3rd and 4th column in this table
# MAGIC  SELECT
# MAGIC     vendor_id,
# MAGIC     vendor_name,
# MAGIC     vendor_category,
# MAGIC     vendor_country,
# MAGIC     SUM(total_spend_usd)    AS total_spend_usd,
# MAGIC     SUM(po_count)           AS total_pos,
# MAGIC     SUM(line_item_count)    AS total_line_items,
# MAGIC     AVG(on_time_delivery_pct) AS avg_on_time_pct
# MAGIC   FROM udw_procurement.gold.spend_analytics
# MAGIC   GROUP BY 1, 2, 3, 4 
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Gold: Pareto Spend Analysis
# MAGIC -- Bedrock agent use: "Which vendors should I focus on?"
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.spend_pareto
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/spend_pareto/'
# MAGIC AS
# MAGIC WITH vendor_totals AS (
# MAGIC   SELECT
# MAGIC     vendor_id,
# MAGIC     vendor_name,
# MAGIC     vendor_category,
# MAGIC     vendor_country,
# MAGIC     SUM(total_spend_usd)    AS total_spend_usd,
# MAGIC     SUM(po_count)           AS total_pos,
# MAGIC     SUM(line_item_count)    AS total_line_items,
# MAGIC     AVG(on_time_delivery_pct) AS avg_on_time_pct
# MAGIC   FROM udw_procurement.gold.spend_analytics
# MAGIC   GROUP BY 1, 2, 3, 4
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *,
# MAGIC     SUM(total_spend_usd) OVER ()                AS grand_total_spend,
# MAGIC     RANK() OVER (ORDER BY total_spend_usd DESC) AS spend_rank
# MAGIC   FROM vendor_totals
# MAGIC )
# MAGIC SELECT
# MAGIC   vendor_id,
# MAGIC   vendor_name,
# MAGIC   vendor_category,
# MAGIC   vendor_country,
# MAGIC   spend_rank,
# MAGIC   ROUND(total_spend_usd, 2)                     AS total_spend_usd,
# MAGIC   total_pos,
# MAGIC   total_line_items,
# MAGIC   ROUND(avg_on_time_pct, 2)                     AS avg_on_time_delivery_pct,
# MAGIC   ROUND(total_spend_usd / grand_total_spend * 100, 2) AS spend_pct_of_total,
# MAGIC   ROUND(
# MAGIC     SUM(total_spend_usd) OVER (
# MAGIC       ORDER BY total_spend_usd DESC
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) / grand_total_spend * 100, 2
# MAGIC   )                                             AS cumulative_spend_pct,
# MAGIC   CASE
# MAGIC     WHEN SUM(total_spend_usd) OVER (
# MAGIC       ORDER BY total_spend_usd DESC
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) / grand_total_spend <= 0.80
# MAGIC     THEN 'TOP 80%'
# MAGIC     ELSE 'TAIL SPEND'
# MAGIC   END                                           AS spend_tier,
# MAGIC   CURRENT_DATE()                                AS run_date
# MAGIC FROM ranked
# MAGIC ORDER BY spend_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 4 — Validate
# MAGIC SELECT
# MAGIC   spend_tier,
# MAGIC   COUNT(*) AS vendor_count,
# MAGIC   ROUND(SUM(total_spend_usd), 2) AS total_spend_usd,
# MAGIC   ROUND(MAX(cumulative_spend_pct), 2) AS max_cumulative_pct
# MAGIC FROM udw_procurement.gold.spend_pareto
# MAGIC GROUP BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE table udw_procurement.gold.spend_analytics;