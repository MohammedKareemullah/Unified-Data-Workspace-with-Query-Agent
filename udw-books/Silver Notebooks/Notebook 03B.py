# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Rebuild Silver Inventory from actual stock snapshot
# MAGIC -- A_MatlStkInAcctMod has real current stock per material,
# MAGIC -- plant, storage location, and stock type
# MAGIC -- This replaces the movement-derived inventory table
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.inventory
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/inventory/'
# MAGIC AS
# MAGIC WITH stock_typed AS (
# MAGIC   SELECT
# MAGIC     Material                                          AS material_number,
# MAGIC     Plant                                             AS plant,
# MAGIC     StorageLocation                                   AS storage_location,
# MAGIC     MaterialBaseUnit                                  AS base_unit,
# MAGIC     Batch                                             AS batch,
# MAGIC     Supplier                                          AS supplier,
# MAGIC     Customer                                          AS customer,
# MAGIC     InventoryStockType                                AS stock_type,
# MAGIC     InventorySpecialStockType                         AS special_stock_type,
# MAGIC     CAST(MatlWrhsStkQtyInMatlBaseUnit AS DOUBLE)      AS stock_qty,
# MAGIC
# MAGIC     -- Stock type descriptions
# MAGIC     CASE InventoryStockType
# MAGIC       WHEN '01' THEN 'Unrestricted'
# MAGIC       WHEN '02' THEN 'Quality Inspection'
# MAGIC       WHEN '03' THEN 'Blocked'
# MAGIC       WHEN '04' THEN 'Restricted Use'
# MAGIC       WHEN '05' THEN 'In Transit'
# MAGIC       WHEN '06' THEN 'In Transfer'
# MAGIC       WHEN '07' THEN 'Blocked Returns'
# MAGIC       ELSE COALESCE(InventoryStockType, 'Unknown')
# MAGIC     END                                               AS stock_type_desc,
# MAGIC
# MAGIC     WBSElementExternalID                              AS wbs_element
# MAGIC   FROM udw_procurement.bronze.sap_material_stock_detaila
# MAGIC   WHERE Material IS NOT NULL
# MAGIC     AND Plant IS NOT NULL
# MAGIC ),
# MAGIC -- Pivot stock types into columns per material/plant/location
# MAGIC stock_pivoted AS (
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     storage_location,
# MAGIC     base_unit,
# MAGIC     -- Unrestricted stock (stock type 01) — freely available
# MAGIC     SUM(CASE WHEN stock_type = '01'
# MAGIC         THEN stock_qty ELSE 0 END)                    AS unrestricted_qty,
# MAGIC     -- Quality inspection stock (stock type 02)
# MAGIC     SUM(CASE WHEN stock_type = '02'
# MAGIC         THEN stock_qty ELSE 0 END)                    AS quality_inspection_qty,
# MAGIC     -- Blocked stock (stock type 03)
# MAGIC     SUM(CASE WHEN stock_type = '03'
# MAGIC         THEN stock_qty ELSE 0 END)                    AS blocked_qty,
# MAGIC     -- Total all stock types
# MAGIC     SUM(stock_qty)                                    AS total_stock_qty,
# MAGIC     -- Count of distinct stock type buckets with stock
# MAGIC     COUNT(CASE WHEN stock_qty > 0
# MAGIC           THEN stock_type END)                        AS active_stock_types,
# MAGIC     COUNT(DISTINCT CASE WHEN stock_qty > 0
# MAGIC           THEN batch END)                             AS batch_count
# MAGIC   FROM stock_typed
# MAGIC   GROUP BY material_number, plant, storage_location, base_unit
# MAGIC )
# MAGIC SELECT
# MAGIC   material_number,
# MAGIC   plant,
# MAGIC   storage_location,
# MAGIC   base_unit,
# MAGIC   COALESCE(unrestricted_qty, 0)                       AS unrestricted_qty,
# MAGIC   COALESCE(quality_inspection_qty, 0)                 AS quality_inspection_qty,
# MAGIC   COALESCE(blocked_qty, 0)                            AS blocked_qty,
# MAGIC   total_stock_qty,
# MAGIC   active_stock_types,
# MAGIC   batch_count,
# MAGIC
# MAGIC   -- Available stock = unrestricted + quality inspection
# MAGIC   COALESCE(unrestricted_qty, 0) +
# MAGIC   COALESCE(quality_inspection_qty, 0)                 AS available_qty,
# MAGIC
# MAGIC   -- Stockout risk based on unrestricted stock
# MAGIC   CASE
# MAGIC     WHEN COALESCE(unrestricted_qty, 0) <= 0           THEN 'CRITICAL'
# MAGIC     WHEN COALESCE(unrestricted_qty, 0) <=
# MAGIC          total_stock_qty * 0.20                       THEN 'HIGH'
# MAGIC     WHEN COALESCE(unrestricted_qty, 0) <=
# MAGIC          total_stock_qty * 0.50                       THEN 'MEDIUM'
# MAGIC     ELSE                                                   'LOW'
# MAGIC   END                                                 AS stockout_risk,
# MAGIC
# MAGIC   -- Is stock being held for quality reasons?
# MAGIC   CASE
# MAGIC     WHEN COALESCE(quality_inspection_qty, 0) >
# MAGIC          COALESCE(unrestricted_qty, 0)
# MAGIC     THEN TRUE ELSE FALSE
# MAGIC   END                                                 AS quality_hold_exceeds_free,
# MAGIC
# MAGIC   CURRENT_DATE()                                      AS snapshot_date,
# MAGIC   'SAP_S4H'                                           AS source_system
# MAGIC
# MAGIC FROM stock_pivoted
# MAGIC WHERE material_number IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Validate Silver Inventory
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*)                              AS total_records,
# MAGIC   COUNT(DISTINCT material_number)       AS distinct_materials,
# MAGIC   COUNT(DISTINCT plant)                 AS distinct_plants,
# MAGIC   COUNT(DISTINCT storage_location)      AS distinct_storage_locs,
# MAGIC   ROUND(SUM(unrestricted_qty), 0)       AS total_unrestricted,
# MAGIC   ROUND(SUM(quality_inspection_qty), 0) AS total_in_quality,
# MAGIC   ROUND(SUM(blocked_qty), 0)            AS total_blocked,
# MAGIC   ROUND(SUM(total_stock_qty), 0)        AS total_all_stock
# MAGIC FROM udw_procurement.silver.inventory;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stockout risk summary
# MAGIC SELECT
# MAGIC   stockout_risk,
# MAGIC   COUNT(*)                              AS material_plant_combinations,
# MAGIC   ROUND(SUM(unrestricted_qty), 0)       AS total_unrestricted,
# MAGIC   ROUND(SUM(total_stock_qty), 0)        AS total_stock
# MAGIC FROM udw_procurement.silver.inventory
# MAGIC GROUP BY 1
# MAGIC ORDER BY
# MAGIC   CASE stockout_risk
# MAGIC     WHEN 'CRITICAL' THEN 1 WHEN 'HIGH'   THEN 2
# MAGIC     WHEN 'MEDIUM'   THEN 3 ELSE 4
# MAGIC   END;