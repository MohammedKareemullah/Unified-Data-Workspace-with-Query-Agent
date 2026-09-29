# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT po_to_scheduled_days FROM udw_procurement.silver.purchase_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Gold: PO Cycle Time
# MAGIC -- UC-01: Procurement cycle time analysis
# MAGIC -- Agent use: "Which vendors have the longest delivery times?"
# MAGIC --            "What is the average PO to delivery days for steel?"
# MAGIC --            "Show me overdue POs by plant"
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.po_cycle_time
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/po_cycle_time/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   p.vendor_id,
# MAGIC   COALESCE(v.vendor_name, p.vendor_id)              AS vendor_name,
# MAGIC   COALESCE(v.vendor_category, 'Unknown')            AS vendor_category,
# MAGIC   COALESCE(v.country, p.supplier_country)           AS vendor_country,
# MAGIC   p.material_group,
# MAGIC   p.company_code,
# MAGIC   p.plant,
# MAGIC   p.purchasing_org,
# MAGIC   p.payment_terms,
# MAGIC   -- Cycle time metrics
# MAGIC   COUNT(DISTINCT p.po_number)                       AS po_count,
# MAGIC   COUNT(p.po_item)                                  AS line_item_count,
# MAGIC   ROUND(AVG(p.po_to_scheduled_days), 1)             AS avg_po_to_delivery_days,
# MAGIC   ROUND(MIN(p.po_to_scheduled_days), 0)             AS min_cycle_days,
# MAGIC   ROUND(MAX(p.po_to_scheduled_days), 0)             AS max_cycle_days,
# MAGIC   ROUND(PERCENTILE(p.po_to_scheduled_days, 0.5), 1) AS median_cycle_days,
# MAGIC   ROUND(PERCENTILE(p.po_to_scheduled_days, 0.90), 1) AS p90_cycle_days,
# MAGIC   -- Delivery status breakdown
# MAGIC   SUM(CASE WHEN p.delivery_status = 'COMPLETED'
# MAGIC       THEN 1 ELSE 0 END)                            AS completed_items,
# MAGIC   SUM(CASE WHEN p.delivery_status = 'OVERDUE'
# MAGIC       THEN 1 ELSE 0 END)                            AS overdue_items,
# MAGIC   SUM(CASE WHEN p.delivery_status = 'PENDING'
# MAGIC       THEN 1 ELSE 0 END)                            AS pending_items,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN p.delivery_status = 'OVERDUE'
# MAGIC         THEN 1 ELSE 0 END) * 100.0 /
# MAGIC     COUNT(p.po_item), 2
# MAGIC   )                                                 AS overdue_rate_pct,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN p.is_fully_received
# MAGIC         THEN 1 ELSE 0 END) * 100.0 /
# MAGIC     COUNT(DISTINCT p.po_number), 2
# MAGIC   )                                                 AS on_time_delivery_pct,
# MAGIC   -- Cycle time buckets
# MAGIC   SUM(CASE WHEN p.po_to_scheduled_days <= 7
# MAGIC       THEN 1 ELSE 0 END)                            AS within_7_days,
# MAGIC   SUM(CASE WHEN p.po_to_scheduled_days BETWEEN 8 AND 14
# MAGIC       THEN 1 ELSE 0 END)                            AS within_8_14_days,
# MAGIC   SUM(CASE WHEN p.po_to_scheduled_days BETWEEN 15 AND 30
# MAGIC       THEN 1 ELSE 0 END)                            AS within_15_30_days,
# MAGIC   SUM(CASE WHEN p.po_to_scheduled_days > 30
# MAGIC       THEN 1 ELSE 0 END)                            AS over_30_days,
# MAGIC   -- Performance classification
# MAGIC   CASE
# MAGIC     WHEN ROUND(AVG(p.po_to_scheduled_days), 1) <= 7  THEN 'FAST'
# MAGIC     WHEN ROUND(AVG(p.po_to_scheduled_days), 1) <= 14 THEN 'STANDARD'
# MAGIC     WHEN ROUND(AVG(p.po_to_scheduled_days), 1) <= 30 THEN 'SLOW'
# MAGIC     ELSE 'CRITICAL'
# MAGIC   END                                               AS cycle_time_rating,
# MAGIC   CURRENT_DATE()                                    AS run_date
# MAGIC FROM udw_procurement.silver.purchase_orders p
# MAGIC LEFT JOIN udw_procurement.silver.vendors v ON p.vendor_id = v.vendor_id
# MAGIC WHERE p.po_to_scheduled_days IS NOT NULL
# MAGIC GROUP BY
# MAGIC   p.vendor_id, v.vendor_name, v.vendor_category,
# MAGIC   v.country, p.supplier_country, p.material_group,
# MAGIC   p.company_code, p.plant, p.purchasing_org,
# MAGIC   p.payment_terms;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 2 — Validate and preview
# MAGIC SELECT
# MAGIC   cycle_time_rating,
# MAGIC   COUNT(*) AS vendor_combinations,
# MAGIC   ROUND(AVG(avg_po_to_delivery_days), 1) AS avg_days,
# MAGIC   SUM(po_count) AS total_pos,
# MAGIC   ROUND(AVG(on_time_delivery_pct), 2) AS avg_on_time_pct,
# MAGIC   SUM(overdue_items) AS total_overdue
# MAGIC FROM udw_procurement.gold.po_cycle_time
# MAGIC GROUP BY 1
# MAGIC ORDER BY avg_days;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE udw_procurement.gold.po_cycle_time;