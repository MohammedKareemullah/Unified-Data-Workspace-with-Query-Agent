# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE udw_procurement.silver.purchase_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Gold: Inventory Health
# MAGIC -- UC-03: Stock coverage, stockout risk, overstock
# MAGIC -- Agent use: "Which materials are at risk of stockout?"
# MAGIC --            "What is our total inventory value by plant?"
# MAGIC --            "Show me slow moving stock older than 90 days"
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.inventory_health
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/inventory_health/'
# MAGIC AS
# MAGIC WITH -- Get material movement history for aging
# MAGIC material_movements AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     MAX(posting_date)                               AS last_movement_date,
# MAGIC     COUNT(*)                                        AS total_movements,
# MAGIC     SUM(CASE WHEN movement_direction = 'INBOUND'
# MAGIC         THEN quantity ELSE 0 END)                   AS total_received,
# MAGIC     SUM(CASE WHEN movement_direction = 'OUTBOUND'
# MAGIC         THEN quantity ELSE 0 END)                   AS total_issued
# MAGIC   FROM udw_procurement.silver.material_documents
# MAGIC   GROUP BY material_number, plant
# MAGIC ),
# MAGIC -- Get open planned orders per material
# MAGIC open_demand AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     SUM(open_quantity)                              AS total_planned_demand,
# MAGIC     COUNT(DISTINCT planned_order)                   AS planned_order_count,
# MAGIC     MIN(planned_start_date)                         AS next_requirement_date
# MAGIC   FROM udw_procurement.silver.planned_orders
# MAGIC   WHERE planning_status != 'CONVERTED'
# MAGIC   GROUP BY material_number, plant
# MAGIC ),
# MAGIC -- Get inbound POs (goods not yet received)
# MAGIC open_inbound AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     SUM(CASE WHEN NOT is_fully_received
# MAGIC         THEN order_qty - COALESCE(scheduled_qty, 0)
# MAGIC         ELSE 0 END)                                 AS open_po_qty,
# MAGIC     COUNT(DISTINCT po_number)                       AS open_po_count,
# MAGIC     MIN(scheduled_delivery_date)                    AS next_po_delivery_date
# MAGIC   FROM udw_procurement.silver.purchase_orders
# MAGIC   WHERE delivery_status = 'PENDING'
# MAGIC     OR delivery_status = 'OVERDUE'
# MAGIC   GROUP BY material_number, plant
# MAGIC ),
# MAGIC -- Get logistics lead time per plant
# MAGIC lead_times AS (
# MAGIC   SELECT
# MAGIC     plant_code,
# MAGIC     MIN(est_delivery_days)                          AS min_lead_time_days,
# MAGIC     AVG(est_delivery_days)                          AS avg_lead_time_days
# MAGIC   FROM udw_procurement.silver.logistics_rates
# MAGIC   GROUP BY plant_code
# MAGIC ),
# MAGIC -- Get inspection lot quality rate per material
# MAGIC quality_rates AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     COUNT(*)                                        AS total_lots,
# MAGIC     ROUND(AVG(defect_rate_pct), 4)                  AS avg_defect_rate,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) * 100.0 /
# MAGIC       COUNT(*), 2
# MAGIC     )                                               AS acceptance_rate_pct
# MAGIC   FROM udw_procurement.silver.inspection_lots
# MAGIC   WHERE vendor_id IS NOT NULL
# MAGIC   GROUP BY material_number, plant
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Material identity
# MAGIC   i.material_number,
# MAGIC   i.plant,
# MAGIC   i.storage_location,
# MAGIC   i.base_unit,
# MAGIC   -- Stock levels (from actual snapshot)
# MAGIC   i.unrestricted_qty,
# MAGIC   i.quality_inspection_qty,
# MAGIC   i.blocked_qty,
# MAGIC   i.total_stock_qty,
# MAGIC   i.available_qty,
# MAGIC   i.stockout_risk,
# MAGIC   i.quality_hold_exceeds_free,
# MAGIC   -- Movement history
# MAGIC   m.last_movement_date,
# MAGIC   m.total_movements,
# MAGIC   m.total_received,
# MAGIC   m.total_issued,
# MAGIC   DATEDIFF(CURRENT_DATE(), m.last_movement_date)    AS days_since_last_movement,
# MAGIC   -- Stock aging classification
# MAGIC   CASE
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(),
# MAGIC          m.last_movement_date) > 180               THEN 'SLOW MOVING (>180 days)'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(),
# MAGIC          m.last_movement_date) > 90                THEN 'MEDIUM MOVING (90-180 days)'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(),
# MAGIC          m.last_movement_date) > 30                THEN 'ACTIVE (30-90 days)'
# MAGIC     ELSE                                                'FAST MOVING (<30 days)'
# MAGIC   END                                               AS stock_aging_status,
# MAGIC   -- Open demand
# MAGIC   COALESCE(d.total_planned_demand, 0)               AS planned_demand_qty,
# MAGIC   d.planned_order_count,
# MAGIC   d.next_requirement_date,
# MAGIC   -- Inbound supply
# MAGIC   COALESCE(pi.open_po_qty, 0)                       AS open_po_inbound_qty,
# MAGIC   pi.open_po_count,
# MAGIC   pi.next_po_delivery_date,
# MAGIC   -- Net coverage position
# MAGIC   i.available_qty +
# MAGIC   COALESCE(pi.open_po_qty, 0) -
# MAGIC   COALESCE(d.total_planned_demand, 0)               AS net_coverage_qty,
# MAGIC   -- Coverage adequacy
# MAGIC   CASE
# MAGIC     WHEN i.available_qty +
# MAGIC          COALESCE(pi.open_po_qty, 0) >=
# MAGIC          COALESCE(d.total_planned_demand, 0)        THEN 'COVERED'
# MAGIC     WHEN i.available_qty > 0                        THEN 'PARTIALLY COVERED'
# MAGIC     ELSE                                                 'NOT COVERED'
# MAGIC   END                                               AS coverage_status,
# MAGIC   -- Lead time context
# MAGIC   COALESCE(lt.min_lead_time_days, 7)                AS min_replenishment_days,
# MAGIC   COALESCE(lt.avg_lead_time_days, 14)               AS avg_replenishment_days,
# MAGIC   -- Days of stock based on issue rate
# MAGIC   CASE
# MAGIC     WHEN COALESCE(m.total_issued, 0) > 0
# MAGIC       AND DATEDIFF(CURRENT_DATE(), m.last_movement_date) > 0
# MAGIC     THEN ROUND(
# MAGIC       i.available_qty /
# MAGIC       (m.total_issued /
# MAGIC        DATEDIFF(CURRENT_DATE(), m.last_movement_date)),
# MAGIC       1
# MAGIC     )
# MAGIC     ELSE NULL
# MAGIC   END                                               AS estimated_days_of_stock,
# MAGIC   -- Quality metrics
# MAGIC   COALESCE(q.total_lots, 0)                         AS inspection_lots_count,
# MAGIC   q.avg_defect_rate,
# MAGIC   q.acceptance_rate_pct,
# MAGIC   -- Combined risk score (for Bedrock agent prioritization)
# MAGIC   CASE
# MAGIC     WHEN i.stockout_risk = 'CRITICAL'
# MAGIC       AND COALESCE(pi.open_po_qty, 0) = 0          THEN 'URGENT ACTION REQUIRED'
# MAGIC     WHEN i.stockout_risk = 'CRITICAL'               THEN 'CRITICAL - SUPPLY INCOMING'
# MAGIC     WHEN i.stockout_risk = 'HIGH'
# MAGIC       AND COALESCE(pi.open_po_qty, 0) = 0          THEN 'HIGH RISK - NO SUPPLY'
# MAGIC     WHEN i.stockout_risk = 'HIGH'                   THEN 'HIGH RISK - MONITOR'
# MAGIC     WHEN i.stockout_risk = 'MEDIUM'                 THEN 'MEDIUM RISK'
# MAGIC     ELSE                                                 'LOW RISK'
# MAGIC   END                                               AS action_priority,
# MAGIC   CURRENT_DATE()                                    AS snapshot_date
# MAGIC FROM udw_procurement.silver.inventory i
# MAGIC LEFT JOIN material_movements m
# MAGIC   ON  i.material_number = m.material_number
# MAGIC   AND i.plant           = m.plant
# MAGIC LEFT JOIN open_demand d
# MAGIC   ON  i.material_number = d.material_number
# MAGIC   AND i.plant           = d.plant
# MAGIC LEFT JOIN open_inbound pi
# MAGIC   ON  i.material_number = pi.material_number
# MAGIC   AND i.plant           = pi.plant
# MAGIC LEFT JOIN lead_times lt
# MAGIC   ON  i.plant           = lt.plant_code
# MAGIC LEFT JOIN quality_rates q
# MAGIC   ON  i.material_number = q.material_number
# MAGIC   AND i.plant           = q.plant;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 2 — Validate
# MAGIC SELECT
# MAGIC   action_priority,
# MAGIC   stock_aging_status,
# MAGIC   COUNT(*) AS materials,
# MAGIC   ROUND(SUM(available_qty), 0) AS total_available,
# MAGIC   ROUND(SUM(planned_demand_qty), 0) AS total_demand,
# MAGIC   ROUND(SUM(open_po_inbound_qty), 0) AS total_inbound,
# MAGIC   SUM(CASE WHEN coverage_status = 'NOT COVERED'
# MAGIC       THEN 1 ELSE 0 END) AS uncovered_materials
# MAGIC FROM udw_procurement.gold.inventory_health
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY
# MAGIC   CASE action_priority
# MAGIC     WHEN 'URGENT ACTION REQUIRED'    THEN 1
# MAGIC     WHEN 'CRITICAL - SUPPLY INCOMING' THEN 2
# MAGIC     WHEN 'HIGH RISK - NO SUPPLY'     THEN 3
# MAGIC     WHEN 'HIGH RISK - MONITOR'       THEN 4
# MAGIC     WHEN 'MEDIUM RISK'               THEN 5
# MAGIC     ELSE 6
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE udw_procurement.gold.inventory_health;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT stockout_risk 
# MAGIC FROM 
# MAGIC udw_procurement.gold.inventory_health;