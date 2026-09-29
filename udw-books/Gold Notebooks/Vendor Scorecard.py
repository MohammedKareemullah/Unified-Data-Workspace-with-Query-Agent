# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT is_fully_invoiced FROM udw_procurement.silver.purchase_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT po_number ) as distinct_po,
# MAGIC     COUNT(po_number) as all_po
# MAGIC FROM udw_procurement.silver.purchase_orders;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(CASE WHEN is_fully_invoiced
# MAGIC           THEN 1 ELSE 0 END)  FROM udw_procurement.silver.purchase_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Gold: Vendor Scorecard
# MAGIC -- UC-04: Composite vendor performance score
# MAGIC -- Agent use: "Who are my best performing vendors?"
# MAGIC --            "Which vendors have quality issues?"
# MAGIC --            "Compare Siemens vs Bosch on delivery performance"
# MAGIC --            "Show me vendor risk assessment"
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.vendor_scorecard
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/vendor_scorecard/'
# MAGIC AS
# MAGIC WITH -- PO performance metrics per vendor
# MAGIC po_metrics AS (
# MAGIC   SELECT
# MAGIC     vendor_id,
# MAGIC     COUNT(DISTINCT po_number)                       AS total_pos,
# MAGIC     COUNT(po_item)                                  AS total_line_items,
# MAGIC     ROUND(AVG(po_to_scheduled_days), 1)             AS avg_delivery_days,
# MAGIC     ROUND(PERCENTILE(po_to_scheduled_days, 0.5), 1) AS median_delivery_days,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN is_fully_received
# MAGIC           THEN 1 ELSE 0 END) * 100.0 /
# MAGIC       COUNT(DISTINCT po_number), 2
# MAGIC     )                                               AS on_time_delivery_pct,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN is_fully_invoiced
# MAGIC           THEN 1 ELSE 0 END) * 100.0 /
# MAGIC       COUNT(DISTINCT po_number), 2
# MAGIC     )                                               AS invoice_accuracy_pct,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN delivery_status = 'OVERDUE'
# MAGIC           THEN 1 ELSE 0 END) * 100.0 /
# MAGIC       COUNT(po_item), 2
# MAGIC     )                                               AS overdue_rate_pct,
# MAGIC     SUM(net_amount)                                 AS total_po_value,
# MAGIC     MAX(currency)                                   AS currency,
# MAGIC     COUNT(DISTINCT plant)                           AS plants_supplied
# MAGIC   FROM udw_procurement.silver.purchase_orders
# MAGIC   GROUP BY vendor_id
# MAGIC ),
# MAGIC -- Quality metrics per vendor
# MAGIC quality_metrics AS (
# MAGIC   SELECT
# MAGIC     vendor_id,
# MAGIC     COUNT(*)                                        AS total_inspection_lots,
# MAGIC     ROUND(AVG(defect_rate_pct), 4)                  AS avg_defect_rate_pct,
# MAGIC     ROUND(AVG(scrap_rate_pct), 4)                   AS avg_scrap_rate_pct,
# MAGIC     ROUND(AVG(return_rate_pct), 4)                  AS avg_return_rate_pct,
# MAGIC     ROUND(AVG(quality_score), 1)                    AS avg_quality_score,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN is_accepted
# MAGIC           THEN 1 ELSE 0 END) * 100.0 /
# MAGIC       COUNT(*), 2
# MAGIC     )                                               AS lot_acceptance_rate_pct,
# MAGIC     MAX(quality_grade)                              AS best_quality_grade,
# MAGIC     SUM(CASE WHEN quality_grade = 'EXCELLENT'
# MAGIC         THEN 1 ELSE 0 END)                          AS excellent_lots,
# MAGIC     SUM(CASE WHEN quality_grade = 'POOR'
# MAGIC         THEN 1 ELSE 0 END)                          AS poor_lots
# MAGIC   FROM udw_procurement.silver.inspection_lots
# MAGIC   WHERE vendor_id IS NOT NULL
# MAGIC   GROUP BY vendor_id
# MAGIC ),
# MAGIC -- Logistics metrics per vendor
# MAGIC logistics_metrics AS (
# MAGIC   SELECT
# MAGIC     vendor_id,
# MAGIC     MIN(rate_usd)                                   AS cheapest_shipping_usd,
# MAGIC     MAX(rate_usd)                                   AS most_expensive_shipping_usd,
# MAGIC     ROUND(AVG(rate_usd), 2)                         AS avg_shipping_rate_usd,
# MAGIC     MIN(est_delivery_days)                          AS fastest_delivery_days,
# MAGIC     ROUND(AVG(est_delivery_days), 1)                AS avg_logistics_days,
# MAGIC     COUNT(DISTINCT carrier)                         AS carrier_options,
# MAGIC     SUM(CASE WHEN delivery_guaranteed
# MAGIC         THEN 1 ELSE 0 END)                          AS guaranteed_services
# MAGIC   FROM udw_procurement.silver.logistics_rates
# MAGIC   GROUP BY vendor_id
# MAGIC ),
# MAGIC -- Info record coverage
# MAGIC info_record_metrics AS (
# MAGIC   SELECT
# MAGIC     vendor_id,
# MAGIC     COUNT(DISTINCT info_record)                     AS info_records_count,
# MAGIC     COUNT(DISTINCT material_number)                 AS materials_with_price_records,
# MAGIC     SUM(CASE WHEN is_regular_supplier
# MAGIC         THEN 1 ELSE 0 END)                          AS regular_supplier_records
# MAGIC   FROM udw_procurement.silver.info_records
# MAGIC   GROUP BY vendor_id
# MAGIC ),
# MAGIC -- Planned order assignments
# MAGIC planned_assignments AS (
# MAGIC   SELECT
# MAGIC     fixed_supplier AS vendor_id,
# MAGIC     COUNT(DISTINCT planned_order)                   AS assigned_planned_orders,
# MAGIC     SUM(total_quantity)                             AS total_planned_qty
# MAGIC   FROM udw_procurement.silver.planned_orders
# MAGIC   WHERE fixed_supplier IS NOT NULL
# MAGIC     AND fixed_supplier != ''
# MAGIC   GROUP BY fixed_supplier
# MAGIC ),
# MAGIC -- FX for spend normalization
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
# MAGIC )
# MAGIC SELECT
# MAGIC   v.vendor_id,
# MAGIC   v.vendor_name,
# MAGIC   v.vendor_category,
# MAGIC   v.country,
# MAGIC   v.city,
# MAGIC   v.payment_terms,
# MAGIC   v.incoterms,
# MAGIC   v.planned_delivery_days                           AS sap_planned_lead_days,
# MAGIC   v.abc_classification,
# MAGIC   v.is_active,
# MAGIC   v.purchasing_blocked,
# MAGIC   v.payment_blocked,
# MAGIC   v.quality_mgmt_system,
# MAGIC   v.quality_cert_valid_to,
# MAGIC
# MAGIC   -- Contract data
# MAGIC   v.contract_count,
# MAGIC   v.total_contract_value,
# MAGIC   v.latest_contract_end,
# MAGIC
# MAGIC   -- PO performance
# MAGIC   COALESCE(pm.total_pos, 0)                         AS total_pos,
# MAGIC   COALESCE(pm.total_line_items, 0)                  AS total_line_items,
# MAGIC   pm.avg_delivery_days,
# MAGIC   pm.median_delivery_days,
# MAGIC   COALESCE(pm.on_time_delivery_pct, 0)              AS on_time_delivery_pct,
# MAGIC   COALESCE(pm.invoice_accuracy_pct, 0)              AS invoice_accuracy_pct,
# MAGIC   COALESCE(pm.overdue_rate_pct, 0)                  AS overdue_rate_pct,
# MAGIC   -- Spend in USD
# MAGIC   ROUND(
# MAGIC     CASE COALESCE(pm.currency, 'INR')
# MAGIC       WHEN 'USD' THEN COALESCE(pm.total_po_value, 0)
# MAGIC       WHEN 'INR' THEN COALESCE(pm.total_po_value, 0) / fx.inr_per_usd
# MAGIC       WHEN 'EUR' THEN COALESCE(pm.total_po_value, 0) / fx.eur_per_usd
# MAGIC       WHEN 'SAR' THEN COALESCE(pm.total_po_value, 0) / fx.sar_per_usd
# MAGIC       ELSE COALESCE(pm.total_po_value, 0) / fx.inr_per_usd
# MAGIC     END, 2
# MAGIC   )                                                 AS total_spend_usd,
# MAGIC   COALESCE(pm.plants_supplied, 0)                   AS plants_supplied,
# MAGIC
# MAGIC   -- Quality performance
# MAGIC   COALESCE(qm.total_inspection_lots, 0)             AS total_inspection_lots,
# MAGIC   COALESCE(qm.avg_defect_rate_pct, 0)               AS avg_defect_rate_pct,
# MAGIC   COALESCE(qm.avg_scrap_rate_pct, 0)                AS avg_scrap_rate_pct,
# MAGIC   COALESCE(qm.avg_return_rate_pct, 0)               AS avg_return_rate_pct,
# MAGIC   qm.avg_quality_score,
# MAGIC   COALESCE(qm.lot_acceptance_rate_pct, 0)           AS lot_acceptance_rate_pct,
# MAGIC   qm.best_quality_grade,
# MAGIC   COALESCE(qm.excellent_lots, 0)                    AS excellent_lots,
# MAGIC   COALESCE(qm.poor_lots, 0)                         AS poor_lots,
# MAGIC
# MAGIC   -- Logistics
# MAGIC   lm.cheapest_shipping_usd,
# MAGIC   lm.avg_shipping_rate_usd,
# MAGIC   lm.fastest_delivery_days                          AS logistics_fastest_days,
# MAGIC   lm.avg_logistics_days,
# MAGIC   COALESCE(lm.carrier_options, 0)                   AS carrier_options,
# MAGIC
# MAGIC   -- Info records
# MAGIC   COALESCE(ir.info_records_count, 0)                AS info_records_count,
# MAGIC   COALESCE(ir.materials_with_price_records, 0)      AS materials_with_price_records,
# MAGIC   COALESCE(ir.regular_supplier_records, 0)          AS regular_supplier_records,
# MAGIC
# MAGIC   -- Planned order assignments
# MAGIC   COALESCE(pa.assigned_planned_orders, 0)           AS assigned_planned_orders,
# MAGIC   COALESCE(pa.total_planned_qty, 0)                 AS total_planned_qty,
# MAGIC
# MAGIC   -- ── COMPOSITE VENDOR SCORE (0-100) ──────────────────────
# MAGIC   -- Weighted components:
# MAGIC   -- On-time delivery:    30%
# MAGIC   -- Invoice accuracy:    20%
# MAGIC   -- Quality acceptance:  25%
# MAGIC   -- Low defect rate:     15%
# MAGIC   -- Carrier options:     10%
# MAGIC   ROUND(
# MAGIC     COALESCE(pm.on_time_delivery_pct, 50)     * 0.30 +
# MAGIC     COALESCE(pm.invoice_accuracy_pct, 50)     * 0.20 +
# MAGIC     COALESCE(qm.lot_acceptance_rate_pct, 50)  * 0.25 +
# MAGIC     -- Invert defect rate (0% defect = 100 points)
# MAGIC     GREATEST(0, 100 - COALESCE(qm.avg_defect_rate_pct * 100, 0)) * 0.15 +
# MAGIC     -- Carrier options scaled to 10 points max
# MAGIC     LEAST(COALESCE(lm.carrier_options, 0) * 2.0, 10.0), 1
# MAGIC   )                                                 AS vendor_score,
# MAGIC
# MAGIC   -- Score tier
# MAGIC   CASE
# MAGIC     WHEN ROUND(
# MAGIC       COALESCE(pm.on_time_delivery_pct, 50)   * 0.30 +
# MAGIC       COALESCE(pm.invoice_accuracy_pct, 50)   * 0.20 +
# MAGIC       COALESCE(qm.lot_acceptance_rate_pct, 50)* 0.25 +
# MAGIC       GREATEST(0, 100 - COALESCE(
# MAGIC         qm.avg_defect_rate_pct * 100, 0))     * 0.15 +
# MAGIC       LEAST(COALESCE(lm.carrier_options, 0)
# MAGIC             * 2.0, 10.0), 1
# MAGIC     ) >= 85 THEN 'STRATEGIC'
# MAGIC     WHEN ROUND(
# MAGIC       COALESCE(pm.on_time_delivery_pct, 50)   * 0.30 +
# MAGIC       COALESCE(pm.invoice_accuracy_pct, 50)   * 0.20 +
# MAGIC       COALESCE(qm.lot_acceptance_rate_pct, 50)* 0.25 +
# MAGIC       GREATEST(0, 100 - COALESCE(
# MAGIC         qm.avg_defect_rate_pct * 100, 0))     * 0.15 +
# MAGIC       LEAST(COALESCE(lm.carrier_options, 0)
# MAGIC             * 2.0, 10.0), 1
# MAGIC     ) >= 70 THEN 'PREFERRED'
# MAGIC     WHEN ROUND(
# MAGIC       COALESCE(pm.on_time_delivery_pct, 50)   * 0.30 +
# MAGIC       COALESCE(pm.invoice_accuracy_pct, 50)   * 0.20 +
# MAGIC       COALESCE(qm.lot_acceptance_rate_pct, 50)* 0.25 +
# MAGIC       GREATEST(0, 100 - COALESCE(
# MAGIC         qm.avg_defect_rate_pct * 100, 0))     * 0.15 +
# MAGIC       LEAST(COALESCE(lm.carrier_options, 0)
# MAGIC             * 2.0, 10.0), 1
# MAGIC     ) >= 50 THEN 'APPROVED'
# MAGIC     ELSE 'UNDER REVIEW'
# MAGIC   END                                               AS vendor_tier,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS snapshot_date
# MAGIC
# MAGIC FROM udw_procurement.silver.vendors v
# MAGIC CROSS JOIN fx
# MAGIC LEFT JOIN po_metrics        pm ON v.vendor_id = pm.vendor_id
# MAGIC LEFT JOIN quality_metrics   qm ON v.vendor_id = qm.vendor_id
# MAGIC LEFT JOIN logistics_metrics lm ON v.vendor_id = lm.vendor_id
# MAGIC LEFT JOIN info_record_metrics ir ON v.vendor_id = ir.vendor_id
# MAGIC LEFT JOIN planned_assignments pa ON v.vendor_id = pa.vendor_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 2 — Validate
# MAGIC SELECT
# MAGIC   vendor_tier,
# MAGIC   COUNT(*) AS vendor_count,
# MAGIC   ROUND(AVG(vendor_score), 1) AS avg_score,
# MAGIC   ROUND(AVG(on_time_delivery_pct), 2) AS avg_on_time,
# MAGIC   ROUND(AVG(lot_acceptance_rate_pct), 2) AS avg_quality_acceptance,
# MAGIC   ROUND(SUM(total_spend_usd), 2) AS total_spend_usd
# MAGIC FROM udw_procurement.gold.vendor_scorecard
# MAGIC GROUP BY 1
# MAGIC ORDER BY avg_score DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE udw_procurement.gold.vendor_scorecard;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_id, vendor_category FROM udw_procurement.gold.vendor_scorecard where vendor_category is NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.gold.spend_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM udw_procurement.gold.spend_analytics LIMIT 10;