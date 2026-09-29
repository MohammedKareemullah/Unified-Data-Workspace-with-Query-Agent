# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE udw_procurement.silver.sales_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Gold: Manufacturing Analytics
# MAGIC -- Additional use case enabled by your rich production data
# MAGIC -- Agent use: "What is our production yield rate by plant?"
# MAGIC --            "Which materials have the most overdue orders?"
# MAGIC --            "Show me production vs sales demand alignment"
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.manufacturing_analytics
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/manufacturing_analytics/'
# MAGIC AS
# MAGIC WITH -- Production order summary per material/plant
# MAGIC production_summary AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     order_status,
# MAGIC     COUNT(DISTINCT production_order)                AS order_count,
# MAGIC     ROUND(SUM(total_quantity), 0)                   AS total_planned_qty,
# MAGIC     ROUND(SUM(confirmed_yield_qty), 0)              AS total_confirmed_qty,
# MAGIC     ROUND(SUM(planned_scrap_qty), 0)                AS total_planned_scrap,
# MAGIC     ROUND(AVG(completion_pct), 2)                   AS avg_completion_pct,
# MAGIC     -- ROUND(AVG(yield_rate_pct), 2)                   AS avg_yield_rate_pct,
# MAGIC     SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END)     AS overdue_orders,
# MAGIC     ROUND(AVG(planned_duration_days), 1)            AS avg_planned_duration,
# MAGIC     MIN(planned_start_date)                         AS earliest_start,
# MAGIC     MAX(planned_end_date)                           AS latest_end
# MAGIC   FROM udw_procurement.silver.production_orders
# MAGIC   GROUP BY material_number, plant, order_status
# MAGIC ),
# MAGIC -- Sales demand per material/plant
# MAGIC sales_demand AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     ROUND(SUM(requested_qty), 0)                    AS total_demand_qty,
# MAGIC     ROUND(SUM(confirmed_qty), 0)                    AS total_confirmed_demand,
# MAGIC     ROUND(SUM(delivered_qty), 0)                    AS total_delivered_qty,
# MAGIC     ROUND(SUM(open_demand_qty), 0)                  AS total_open_demand,
# MAGIC     COUNT(DISTINCT sales_order)                     AS sales_order_count,
# MAGIC     ROUND(AVG(fulfillment_rate_pct), 2)             AS avg_fulfillment_rate,
# MAGIC     MAX(requested_delivery_date)                    AS latest_delivery_req
# MAGIC   FROM udw_procurement.silver.sales_orders
# MAGIC   WHERE is_active_order = TRUE
# MAGIC   GROUP BY material_number, plant
# MAGIC ),
# MAGIC -- Inventory position
# MAGIC inv_position AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     SUM(available_qty)                              AS available_stock,
# MAGIC     SUM(unrestricted_qty)                           AS unrestricted_stock,
# MAGIC     MAX(stockout_risk)                              AS stockout_risk
# MAGIC   FROM udw_procurement.silver.inventory
# MAGIC   GROUP BY material_number, plant
# MAGIC ),
# MAGIC -- Planned order pipeline
# MAGIC planned_pipeline AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     SUM(open_quantity)                              AS total_planned_qty,
# MAGIC     COUNT(DISTINCT planned_order)                   AS planned_order_count,
# MAGIC     MIN(planned_start_date)                         AS next_planned_start
# MAGIC   FROM udw_procurement.silver.planned_orders
# MAGIC   GROUP BY material_number, plant
# MAGIC ),
# MAGIC -- Quality context from inspection lots
# MAGIC quality_context AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     ROUND(AVG(defect_rate_pct), 4)                  AS avg_defect_rate,
# MAGIC     ROUND(AVG(quality_score), 1)                    AS avg_quality_score,
# MAGIC     COUNT(*)                                        AS inspection_count,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) *
# MAGIC       100.0 / COUNT(*), 2
# MAGIC     )                                               AS acceptance_rate_pct
# MAGIC   FROM udw_procurement.silver.inspection_lots
# MAGIC   GROUP BY material_number, plant
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Material identity
# MAGIC   COALESCE(ps.material_number, sd.material_number,
# MAGIC            ip.material_number)                      AS material_number,
# MAGIC   COALESCE(ps.plant, sd.plant, ip.plant)            AS plant,
# MAGIC   ps.order_status,
# MAGIC
# MAGIC   -- Production metrics
# MAGIC   COALESCE(ps.order_count, 0)                       AS production_order_count,
# MAGIC   COALESCE(ps.total_planned_qty, 0)                 AS production_planned_qty,
# MAGIC   COALESCE(ps.total_confirmed_qty, 0)               AS production_confirmed_qty,
# MAGIC   COALESCE(ps.avg_completion_pct, 0)                AS avg_production_completion,
# MAGIC --   COALESCE(ps.avg_yield_rate_pct, 0)                AS avg_yield_rate_pct,
# MAGIC   COALESCE(ps.overdue_orders, 0)                    AS overdue_production_orders,
# MAGIC   ps.avg_planned_duration                           AS avg_production_duration_days,
# MAGIC
# MAGIC   -- Sales demand
# MAGIC   COALESCE(sd.total_demand_qty, 0)                  AS total_sales_demand_qty,
# MAGIC   COALESCE(sd.total_delivered_qty, 0)               AS total_delivered_to_customers,
# MAGIC   COALESCE(sd.total_open_demand, 0)                 AS open_customer_demand,
# MAGIC   COALESCE(sd.avg_fulfillment_rate, 0)              AS customer_fulfillment_rate,
# MAGIC   COALESCE(sd.sales_order_count, 0)                 AS sales_order_count,
# MAGIC
# MAGIC   -- Inventory
# MAGIC   COALESCE(ip.available_stock, 0)                   AS available_stock,
# MAGIC   COALESCE(ip.unrestricted_stock, 0)                AS unrestricted_stock,
# MAGIC   COALESCE(ip.stockout_risk, 'UNKNOWN')             AS stockout_risk,
# MAGIC
# MAGIC   -- Planned supply pipeline
# MAGIC   COALESCE(pp.total_planned_qty, 0)                 AS planned_supply_qty,
# MAGIC   COALESCE(pp.planned_order_count, 0)               AS planned_order_count,
# MAGIC   pp.next_planned_start,
# MAGIC
# MAGIC   -- Supply vs demand balance
# MAGIC   COALESCE(ip.available_stock, 0) +
# MAGIC   COALESCE(pp.total_planned_qty, 0) -
# MAGIC   COALESCE(sd.total_open_demand, 0)                 AS supply_demand_balance,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ip.available_stock, 0) +
# MAGIC          COALESCE(pp.total_planned_qty, 0) >=
# MAGIC          COALESCE(sd.total_open_demand, 0)          THEN 'SUPPLY ADEQUATE'
# MAGIC     WHEN COALESCE(ip.available_stock, 0) > 0        THEN 'PARTIAL COVERAGE'
# MAGIC     ELSE                                                 'SUPPLY GAP'
# MAGIC   END                                               AS supply_demand_status,
# MAGIC
# MAGIC   -- Quality context
# MAGIC   COALESCE(qc.avg_defect_rate, 0)                   AS avg_defect_rate_pct,
# MAGIC   qc.avg_quality_score,
# MAGIC   COALESCE(qc.acceptance_rate_pct, 0)               AS quality_acceptance_rate,
# MAGIC   COALESCE(qc.inspection_count, 0)                  AS quality_inspections,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS snapshot_date
# MAGIC
# MAGIC FROM production_summary ps
# MAGIC FULL OUTER JOIN sales_demand sd
# MAGIC   ON  ps.material_number = sd.material_number
# MAGIC   AND ps.plant           = sd.plant
# MAGIC LEFT JOIN inv_position ip
# MAGIC   ON  COALESCE(ps.material_number, sd.material_number) = ip.material_number
# MAGIC   AND COALESCE(ps.plant, sd.plant) = ip.plant
# MAGIC LEFT JOIN planned_pipeline pp
# MAGIC   ON  COALESCE(ps.material_number, sd.material_number) = pp.material_number
# MAGIC   AND COALESCE(ps.plant, sd.plant) = pp.plant
# MAGIC LEFT JOIN quality_context qc
# MAGIC   ON  COALESCE(ps.material_number, sd.material_number) = qc.material_number
# MAGIC   AND COALESCE(ps.plant, sd.plant) = qc.plant;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 2 — Validate
# MAGIC SELECT
# MAGIC   supply_demand_status,
# MAGIC   stockout_risk,
# MAGIC   COUNT(*) AS material_plant_combinations,
# MAGIC   ROUND(SUM(production_planned_qty), 0) AS total_planned_production,
# MAGIC   ROUND(SUM(open_customer_demand), 0) AS total_open_demand,
# MAGIC   ROUND(SUM(available_stock), 0) AS total_available_stock,
# MAGIC   SUM(overdue_production_orders) AS total_overdue_orders
# MAGIC FROM udw_procurement.gold.manufacturing_analytics
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY total_open_demand DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe table udw_procurement.gold.manufacturing_analytics;