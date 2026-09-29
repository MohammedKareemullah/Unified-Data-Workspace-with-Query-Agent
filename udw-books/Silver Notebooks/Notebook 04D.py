# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.bronze.sap_inspection_lot_header

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.bronze.sap_inspection_characteristics;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.bronze.sap_inspection_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.bronze.sap_inspection_lot_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.bronze.sap_inspection_usage_decision;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Silver Inspection Lots
# MAGIC -- Using confirmed field names from A_InspectionLot preview
# MAGIC -- Key fields: InspectionLot, Material, Plant, Supplier,
# MAGIC -- PurchasingDocument, InspectionLotQuantity,
# MAGIC -- InspectionLotDefectiveQuantity, InspectionLotQualityScore
# MAGIC -- InspLotUsageDecisionValuation (A=accepted, R=rejected)
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.inspection_lots
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/inspection_lots/'
# MAGIC AS
# MAGIC WITH lot_header AS (
# MAGIC   SELECT
# MAGIC     InspectionLot                                     AS inspection_lot,
# MAGIC     InspectionLotType                                 AS lot_type,
# MAGIC     InspectionLotOrigin                               AS lot_origin,
# MAGIC     Material                                          AS material_number,
# MAGIC     Plant                                             AS plant,
# MAGIC     InspectionLotPlant                                AS inspection_plant,
# MAGIC     -- StorageLocation                                   AS storage_location,
# MAGIC     InspectionLotStorageLocation                      AS inspection_storage_location,
# MAGIC     Batch                                             AS batch,
# MAGIC     BatchStorageLocation                              AS batch_storage_location,    
# MAGIC
# MAGIC     Supplier                                          AS vendor_id,
# MAGIC     PurchasingDocument                                AS po_number,
# MAGIC     PurchasingDocumentItem                            AS po_item,
# MAGIC     SalesOrder                                        AS sales_order,
# MAGIC     SalesOrderItem                                    AS sales_order_item,
# MAGIC     ManufacturingOrder                                AS production_order,
# MAGIC     -- Quantities
# MAGIC     CAST(InspectionLotQuantity AS DOUBLE)             AS lot_quantity,
# MAGIC     InspectionLotQuantityUnit                         AS quantity_unit,
# MAGIC     CAST(InspectionLotSampleQuantity AS DOUBLE)       AS sample_quantity,
# MAGIC     CAST(InspectionLotDefectiveQuantity AS DOUBLE)    AS defective_quantity,
# MAGIC     CAST(InspLotQtyToScrap AS DOUBLE)                 AS scrap_quantity,
# MAGIC     CAST(InspLotQtyToFree AS DOUBLE)                  AS accepted_quantity,
# MAGIC     CAST(InspLotQtyToBlocked AS DOUBLE)               AS blocked_quantity,
# MAGIC     CAST(InspLotQtyReturnedToSupplier AS DOUBLE)      AS returned_quantity,
# MAGIC     CAST(InspLotQtyInspected AS DOUBLE)               AS inspected_quantity,
# MAGIC     -- Quality
# MAGIC     -- CAST(InspectionLotQualityScore AS DOUBLE)         AS quality_score,
# MAGIC     -- InspectionLotUsageDecisionCode                    AS usage_decision_code,
# MAGIC     CAST(InspectionLotHasUsageDecision AS BOOLEAN)    AS has_usage_decision,
# MAGIC     -- Usage decision valuation: A = Accepted, R = Rejected
# MAGIC     -- InspLotUsageDecisionValuation                     AS usage_decision_valuation,
# MAGIC     -- CASE InspLotUsageDecisionValuation
# MAGIC     --   WHEN 'A' THEN TRUE ELSE FALSE
# MAGIC     -- END                                               AS is_accepted,
# MAGIC     -- Defect counts
# MAGIC     -- CAST(InspectionNumberOfDefects AS BIGINT)         AS number_of_defects,
# MAGIC     -- CAST(InspLotNmbrOfNonconformingUnits AS BIGINT)   AS nonconforming_units,
# MAGIC     -- Defect rate
# MAGIC     CASE
# MAGIC       WHEN CAST(InspectionLotQuantity AS DOUBLE) > 0
# MAGIC       THEN ROUND(
# MAGIC         CAST(InspectionLotDefectiveQuantity AS DOUBLE) /
# MAGIC         CAST(InspectionLotQuantity AS DOUBLE) * 100, 4
# MAGIC       )
# MAGIC       ELSE 0
# MAGIC     END                                               AS defect_rate_pct,
# MAGIC     -- Scrap rate
# MAGIC     CASE
# MAGIC       WHEN CAST(InspectionLotQuantity AS DOUBLE) > 0
# MAGIC       THEN ROUND(
# MAGIC         CAST(InspLotQtyToScrap AS DOUBLE) /
# MAGIC         CAST(InspectionLotQuantity AS DOUBLE) * 100, 4
# MAGIC       )
# MAGIC       ELSE 0
# MAGIC     END                                               AS scrap_rate_pct,
# MAGIC     -- Return rate
# MAGIC     CASE
# MAGIC       WHEN CAST(InspectionLotQuantity AS DOUBLE) > 0
# MAGIC       THEN ROUND(
# MAGIC         CAST(InspLotQtyReturnedToSupplier AS DOUBLE) /
# MAGIC         CAST(InspectionLotQuantity AS DOUBLE) * 100, 4
# MAGIC       )
# MAGIC       ELSE 0
# MAGIC     END                                               AS return_rate_pct,
# MAGIC     -- Quality grade
# MAGIC     -- CASE
# MAGIC     --   WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 90 THEN 'EXCELLENT'
# MAGIC     --   WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 75 THEN 'GOOD'
# MAGIC     --   WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 50 THEN 'ACCEPTABLE'
# MAGIC     --   WHEN CAST(InspectionLotQualityScore AS DOUBLE) > 0  THEN 'POOR'
# MAGIC     --   ELSE 'NOT SCORED'
# MAGIC     -- END                                               AS quality_grade,
# MAGIC     -- Status flags
# MAGIC     CAST(InspectionLotIsSkipped AS BOOLEAN)           AS is_skipped,
# MAGIC     CAST(InspectionLotIsFullInspection AS BOOLEAN)    AS is_full_inspection,
# MAGIC     CAST(InspLotIsStockPostingCompleted AS BOOLEAN)   AS stock_posting_complete,
# MAGIC     -- Dates
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS lot_start_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS lot_end_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotCreatedOn,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS created_date
# MAGIC     -- CAST(
# MAGIC     --   from_unixtime(
# MAGIC     --     CAST(regexp_extract(InspectionLotUsageDecidedOn,
# MAGIC     --       '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     --   ) AS DATE
# MAGIC     -- )                                                 AS usage_decided_date
# MAGIC   FROM udw_procurement.bronze.sap_inspection_lot_header
# MAGIC   WHERE InspectionLot IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY InspectionLot ORDER BY InspectionLot
# MAGIC   ) = 1
# MAGIC ),
# MAGIC usage_decision as (
# MAGIC     SELECT 
# MAGIC     InspectionLot                                     AS inspection_lot1,
# MAGIC     InspectionLotUsageDecisionCode                    AS usage_decision_code,
# MAGIC     InspLotUsageDecisionValuation                     AS usage_decision_valuation,
# MAGIC     
# MAGIC     CASE InspLotUsageDecisionValuation
# MAGIC       WHEN 'A' THEN TRUE ELSE FALSE
# MAGIC     END                                               AS is_accepted,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotUsageDecidedOn,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS usage_decided_date,
# MAGIC     CAST(InspectionLotQualityScore AS DOUBLE)         AS quality_score,
# MAGIC     CASE
# MAGIC       WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 90 THEN 'EXCELLENT'
# MAGIC       WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 75 THEN 'GOOD'
# MAGIC       WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 50 THEN 'ACCEPTABLE'
# MAGIC       WHEN CAST(InspectionLotQualityScore AS DOUBLE) > 0  THEN 'POOR'
# MAGIC       ELSE 'NOT SCORED'
# MAGIC     END                                               AS quality_grade    
# MAGIC     FROM udw_procurement.bronze.sap_inspection_usage_decision
# MAGIC     WHERE InspectionLot IS NOT NULL
# MAGIC     QUALIFY ROW_NUMBER() OVER (
# MAGIC       PARTITION BY InspectionLot ORDER BY InspectionLot
# MAGIC     ) = 1
# MAGIC ),
# MAGIC results as (
# MAGIC     SELECT 
# MAGIC     InspectionLot                                     AS inspection_lot2,
# MAGIC     CAST(InspectionNumberOfDefects AS BIGINT)         AS number_of_defects
# MAGIC     FROM udw_procurement.bronze.sap_inspection_results
# MAGIC     WHERE InspectionLot IS NOT NULL
# MAGIC     QUALIFY ROW_NUMBER() OVER (
# MAGIC       PARTITION BY InspectionLot ORDER BY InspectionLot
# MAGIC     ) = 1
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   h.*,
# MAGIC   DATEDIFF(h.lot_end_date, h.lot_start_date)          AS inspection_duration_days,
# MAGIC   YEAR(h.created_date)                                AS creation_year,
# MAGIC   MONTH(h.created_date)                               AS creation_month,
# MAGIC   CURRENT_DATE()                                      AS ingestion_date,
# MAGIC   'SAP_S4H'                                           AS source_system,
# MAGIC   u.*,
# MAGIC   r.*
# MAGIC FROM lot_header h left JOIN usage_decision u ON h.inspection_lot = u.inspection_lot1 left JOIN results r ON h.inspection_lot = r.inspection_lot2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Validate Silver Inspection Lots
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   quality_grade,
# MAGIC   COUNT(*)                              AS lot_count,
# MAGIC   COUNT(DISTINCT vendor_id)             AS distinct_vendors,
# MAGIC   ROUND(AVG(defect_rate_pct), 4)        AS avg_defect_rate,
# MAGIC   ROUND(AVG(quality_score), 1)          AS avg_quality_score,
# MAGIC   SUM(CASE WHEN is_accepted
# MAGIC       THEN 1 ELSE 0 END)                AS accepted,
# MAGIC   COUNT(*) - SUM(CASE WHEN is_accepted
# MAGIC                  THEN 1 ELSE 0 END)     AS not_accepted,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN is_accepted
# MAGIC         THEN 1 ELSE 0 END) * 100.0 /
# MAGIC     COUNT(*), 2
# MAGIC   )                                     AS acceptance_rate_pct
# MAGIC FROM udw_procurement.silver.inspection_lots
# MAGIC GROUP BY 1
# MAGIC ORDER BY lot_count DESC;