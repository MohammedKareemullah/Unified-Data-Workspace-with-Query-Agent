# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_master LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_stock LIMIT 3;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_documents LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Inspect actual data content
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC -- Check material master actual values
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_master LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check material stock actual values
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_stock LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check material documents sample
# MAGIC SELECT
# MAGIC   MaterialDocument,
# MAGIC   Material,
# MAGIC   Plant,
# MAGIC   StorageLocation,
# MAGIC   GoodsMovementType,
# MAGIC   QuantityInBaseUnit,
# MAGIC   MaterialBaseUnit,
# MAGIC   PostingDate,
# MAGIC   Supplier,
# MAGIC   PurchaseOrder,
# MAGIC   CostCenter,
# MAGIC   ManufacturingOrder
# MAGIC FROM udw_procurement.bronze.sap_material_documents
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Silver Material Documents (goods movements)
# MAGIC -- This is your richest inventory-related dataset (3000 records)
# MAGIC -- Covers GR, GI, transfers, production consumption, scrapping
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.material_documents
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/material_documents/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   MaterialDocument                                  AS material_doc,
# MAGIC   MaterialDocumentYear                              AS doc_year,
# MAGIC   MaterialDocumentItem                              AS doc_item,
# MAGIC   MaterialDocumentLine                              AS doc_line,
# MAGIC   Material                                          AS material_number,
# MAGIC   Plant                                             AS plant,
# MAGIC   StorageLocation                                   AS storage_location,
# MAGIC   MaterialBaseUnit                                  AS base_unit,
# MAGIC
# MAGIC   -- Movement classification
# MAGIC   GoodsMovementType                                 AS movement_type,
# MAGIC   CASE GoodsMovementType
# MAGIC     WHEN '101' THEN 'GR for Purchase Order'
# MAGIC     WHEN '102' THEN 'GR Reversal for PO'
# MAGIC     WHEN '103' THEN 'GR into Blocked Stock'
# MAGIC     WHEN '105' THEN 'GR Release from Blocked Stock'
# MAGIC     WHEN '122' THEN 'Return Delivery to Vendor'
# MAGIC     WHEN '201' THEN 'GI for Cost Center'
# MAGIC     WHEN '261' THEN 'GI for Production Order'
# MAGIC     WHEN '301' THEN 'Transfer Plant to Plant'
# MAGIC     WHEN '311' THEN 'Transfer Storage Location'
# MAGIC     WHEN '501' THEN 'Receipt without Reference'
# MAGIC     WHEN '551' THEN 'Scrapping'
# MAGIC     WHEN '601' THEN 'GI for Delivery (SD)'
# MAGIC     WHEN '641' THEN 'Transfer Posting to Own Stock'
# MAGIC     ELSE COALESCE(GoodsMovementType, 'Unknown')
# MAGIC   END                                               AS movement_description,
# MAGIC
# MAGIC   -- Movement direction
# MAGIC   CASE
# MAGIC     WHEN GoodsMovementType IN
# MAGIC          ('101','103','105','501','641')
# MAGIC     THEN 'INBOUND'
# MAGIC     WHEN GoodsMovementType IN
# MAGIC          ('102','122','201','261','551','601')
# MAGIC     THEN 'OUTBOUND'
# MAGIC     WHEN GoodsMovementType IN ('301','311')
# MAGIC     THEN 'TRANSFER'
# MAGIC     ELSE 'OTHER'
# MAGIC   END                                               AS movement_direction,
# MAGIC
# MAGIC   CAST(QuantityInBaseUnit AS DOUBLE)                AS quantity,
# MAGIC   CAST(QuantityInEntryUnit AS DOUBLE)               AS quantity_entry_unit,
# MAGIC   EntryUnit                                         AS entry_unit,
# MAGIC
# MAGIC   -- Financial data
# MAGIC   CAST(GdsMvtExtAmtInCoCodeCrcy AS DOUBLE)          AS amount_company_currency,
# MAGIC   CompanyCodeCurrency                               AS company_currency,
# MAGIC
# MAGIC   -- Reference documents
# MAGIC   PurchaseOrder                                     AS po_number,
# MAGIC   PurchaseOrderItem                                 AS po_item,
# MAGIC   ManufacturingOrder                                AS production_order,
# MAGIC   ManufacturingOrderItem                            AS production_order_item,
# MAGIC   SalesOrder                                        AS sales_order,
# MAGIC   SalesOrderItem                                    AS sales_order_item,
# MAGIC   Reservation                                       AS reservation,
# MAGIC   ReservationItem                                   AS reservation_item,
# MAGIC   Supplier                                          AS vendor_id,
# MAGIC   Customer                                          AS customer_id,
# MAGIC
# MAGIC   -- Cost assignment
# MAGIC   CostCenter                                        AS cost_center,
# MAGIC   ControllingArea                                   AS controlling_area,
# MAGIC   ProfitCenter                                      AS profit_center,
# MAGIC   GLAccount                                         AS gl_account,
# MAGIC   WBSElement                                        AS wbs_element,
# MAGIC   FunctionalArea                                    AS functional_area,
# MAGIC
# MAGIC   -- Stock type information
# MAGIC   InventoryStockType                                AS stock_type,
# MAGIC   InventorySpecialStockType                         AS special_stock_type,
# MAGIC   InventoryValuationType                            AS valuation_type,
# MAGIC   IssuingOrReceivingPlant                           AS receiving_plant,
# MAGIC   IssuingOrReceivingStorageLoc                      AS receiving_storage_loc,
# MAGIC
# MAGIC   -- Status flags
# MAGIC   CAST(GoodsMovementIsCancelled AS BOOLEAN)         AS is_cancelled,
# MAGIC   CAST(IsCompletelyDelivered AS BOOLEAN)            AS is_completely_delivered,
# MAGIC   CAST(ReservationIsFinallyIssued AS BOOLEAN)       AS reservation_final_issued,
# MAGIC
# MAGIC   -- Fiscal information
# MAGIC   FiscalYear                                        AS fiscal_year,
# MAGIC   FiscalYearPeriod                                  AS fiscal_period,
# MAGIC
# MAGIC   -- Dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(PostingDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS posting_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(DocumentDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS document_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(CreationDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS creation_date,
# MAGIC
# MAGIC   -- Month/Year for aggregation
# MAGIC   YEAR(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PostingDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS posting_year,
# MAGIC   MONTH(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PostingDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS posting_month,
# MAGIC
# MAGIC   GoodsMovementReasonCode                           AS reason_code,
# MAGIC   MaterialDocumentHeaderText                        AS header_text,
# MAGIC   MaterialDocumentItemText                          AS item_text,
# MAGIC   GoodsRecipientName                                AS recipient_name,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_material_documents
# MAGIC WHERE MaterialDocument IS NOT NULL
# MAGIC   AND CAST(GoodsMovementIsCancelled AS BOOLEAN) IS DISTINCT FROM TRUE
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY MaterialDocument, MaterialDocumentItem
# MAGIC   ORDER BY MaterialDocument
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Silver Inventory derived from material documents
# MAGIC -- Since A_MaterialStock only has Material + BaseUnit,
# MAGIC -- we derive current stock position from document history
# MAGIC -- This is the standard approach when stock snapshot
# MAGIC -- data is unavailable or incomplete
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.inventory
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/inventory/'
# MAGIC AS
# MAGIC WITH stock_movements AS (
# MAGIC   -- Calculate net stock per material/plant/storage location
# MAGIC   -- from all goods movements
# MAGIC   SELECT
# MAGIC     material_number,
# MAGIC     plant,
# MAGIC     storage_location,
# MAGIC     base_unit,
# MAGIC     cost_center,
# MAGIC     -- Inbound movements increase stock, outbound decrease
# MAGIC     SUM(
# MAGIC       CASE movement_direction
# MAGIC         WHEN 'INBOUND'  THEN  quantity
# MAGIC         WHEN 'OUTBOUND' THEN -quantity
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )                                               AS net_stock_qty,
# MAGIC     SUM(
# MAGIC       CASE movement_direction
# MAGIC         WHEN 'INBOUND'  THEN  quantity
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )                                               AS total_received_qty,
# MAGIC     SUM(
# MAGIC       CASE movement_direction
# MAGIC         WHEN 'OUTBOUND' THEN quantity
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     )                                               AS total_issued_qty,
# MAGIC     SUM(
# MAGIC       CASE WHEN movement_type = '551'
# MAGIC         THEN quantity ELSE 0
# MAGIC       END
# MAGIC     )                                               AS scrapped_qty,
# MAGIC     SUM(
# MAGIC       CASE WHEN movement_type IN ('101','102')
# MAGIC         THEN quantity ELSE 0
# MAGIC       END
# MAGIC     )                                               AS po_receipt_qty,
# MAGIC     SUM(
# MAGIC       CASE WHEN movement_type = '261'
# MAGIC         THEN quantity ELSE 0
# MAGIC       END
# MAGIC     )                                               AS production_issue_qty,
# MAGIC     COUNT(DISTINCT material_doc)                    AS movement_count,
# MAGIC     MAX(posting_date)                               AS last_movement_date,
# MAGIC     MIN(posting_date)                               AS first_movement_date,
# MAGIC     COUNT(DISTINCT vendor_id)                       AS distinct_vendors,
# MAGIC     ROUND(AVG(
# MAGIC       CASE WHEN amount_company_currency IS NOT NULL
# MAGIC         THEN amount_company_currency / NULLIF(quantity, 0)
# MAGIC       END
# MAGIC     ), 2)                                           AS avg_unit_value,
# MAGIC     MAX(company_currency)                           AS currency
# MAGIC   FROM udw_procurement.silver.material_documents
# MAGIC   WHERE material_number IS NOT NULL
# MAGIC     AND plant IS NOT NULL
# MAGIC   GROUP BY
# MAGIC     material_number, plant, storage_location,
# MAGIC     base_unit, cost_center
# MAGIC ),
# MAGIC -- Get days since last movement for aging analysis
# MAGIC stock_with_aging AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     DATEDIFF(CURRENT_DATE(), last_movement_date)    AS days_since_last_movement,
# MAGIC     ROUND(
# MAGIC       net_stock_qty * COALESCE(avg_unit_value, 0), 2
# MAGIC     )                                               AS estimated_stock_value
# MAGIC   FROM stock_movements
# MAGIC )
# MAGIC SELECT
# MAGIC   material_number,
# MAGIC   plant,
# MAGIC   storage_location,
# MAGIC   base_unit,
# MAGIC   cost_center,
# MAGIC   net_stock_qty                                     AS current_stock_qty,
# MAGIC   total_received_qty,
# MAGIC   total_issued_qty,
# MAGIC   scrapped_qty,
# MAGIC   po_receipt_qty,
# MAGIC   production_issue_qty,
# MAGIC   movement_count,
# MAGIC   last_movement_date,
# MAGIC   first_movement_date,
# MAGIC   days_since_last_movement,
# MAGIC   distinct_vendors,
# MAGIC   avg_unit_value,
# MAGIC   estimated_stock_value,
# MAGIC   currency,
# MAGIC
# MAGIC   -- Stock health classification
# MAGIC   CASE
# MAGIC     WHEN net_stock_qty <= 0                         THEN 'ZERO/NEGATIVE'
# MAGIC     WHEN days_since_last_movement > 180             THEN 'SLOW MOVING'
# MAGIC     WHEN days_since_last_movement > 90              THEN 'MEDIUM MOVING'
# MAGIC     ELSE                                                 'ACTIVE'
# MAGIC   END                                               AS stock_activity_status,
# MAGIC
# MAGIC   -- Stock risk based on net position
# MAGIC   CASE
# MAGIC     WHEN net_stock_qty <= 0                         THEN 'CRITICAL'
# MAGIC     WHEN net_stock_qty <=
# MAGIC          total_received_qty * 0.10                  THEN 'HIGH'
# MAGIC     WHEN net_stock_qty <=
# MAGIC          total_received_qty * 0.25                  THEN 'MEDIUM'
# MAGIC     ELSE                                                 'LOW'
# MAGIC   END                                               AS stockout_risk,
# MAGIC
# MAGIC   -- Turnover ratio (issues / average stock)
# MAGIC   CASE
# MAGIC     WHEN net_stock_qty > 0
# MAGIC     THEN ROUND(total_issued_qty / net_stock_qty, 2)
# MAGIC     ELSE NULL
# MAGIC   END                                               AS stock_turnover_ratio,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS snapshot_date,
# MAGIC   'SAP_S4H_DERIVED'                                 AS source_system
# MAGIC
# MAGIC FROM stock_with_aging
# MAGIC WHERE net_stock_qty IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 4 — Silver Material Master
# MAGIC -- Check if it has real product data or document data
# MAGIC -- and handle accordingly
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC -- First check what's actually in material master
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN MAX(Material) IS NOT NULL
# MAGIC      AND MAX(MaterialDocument) IS NULL
# MAGIC     THEN 'PRODUCT MASTER DATA'
# MAGIC     WHEN MAX(MaterialDocument) IS NOT NULL
# MAGIC     THEN 'MATERIAL DOCUMENT DATA (same as mat_docs)'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC   END AS data_type,
# MAGIC   COUNT(*) AS record_count
# MAGIC FROM udw_procurement.bronze.sap_material_master;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 5 — Create material master or derive from documents
# MAGIC -- Run this AFTER seeing Cell 4 output
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC -- If material master has product data (MaterialDocument is null):
# MAGIC -- Use this version:
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.material_master
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/material_master/'
# MAGIC AS
# MAGIC -- Derive material dimension from movement data
# MAGIC -- since material master may contain document records
# MAGIC SELECT DISTINCT
# MAGIC   md.material_number,
# MAGIC   md.plant,
# MAGIC   md.base_unit,
# MAGIC   COUNT(DISTINCT md.material_doc)     AS total_movements,
# MAGIC   MIN(md.posting_date)                AS first_seen_date,
# MAGIC   MAX(md.posting_date)                AS last_seen_date,
# MAGIC   COUNT(DISTINCT md.vendor_id)        AS supplier_count,
# MAGIC   COUNT(DISTINCT md.cost_center)      AS cost_center_count,
# MAGIC   -- Movement type mix tells us about the material's role
# MAGIC   SUM(CASE WHEN md.movement_type IN ('101','102')
# MAGIC       THEN 1 ELSE 0 END)              AS po_receipt_movements,
# MAGIC   SUM(CASE WHEN md.movement_type = '261'
# MAGIC       THEN 1 ELSE 0 END)              AS production_movements,
# MAGIC   SUM(CASE WHEN md.movement_type IN ('201')
# MAGIC       THEN 1 ELSE 0 END)              AS cost_center_movements,
# MAGIC   SUM(CASE WHEN md.movement_type = '551'
# MAGIC       THEN 1 ELSE 0 END)              AS scrap_movements,
# MAGIC   -- Classify material role from movement patterns
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN md.movement_type IN ('101','102')
# MAGIC              THEN 1 ELSE 0 END) > 0
# MAGIC       AND SUM(CASE WHEN md.movement_type = '261'
# MAGIC               THEN 1 ELSE 0 END) > 0  THEN 'RAW MATERIAL'
# MAGIC     WHEN SUM(CASE WHEN md.movement_type = '261'
# MAGIC              THEN 1 ELSE 0 END) > 0   THEN 'PRODUCTION INPUT'
# MAGIC     WHEN SUM(CASE WHEN md.movement_type IN ('601','641')
# MAGIC              THEN 1 ELSE 0 END) > 0   THEN 'FINISHED GOODS'
# MAGIC     WHEN SUM(CASE WHEN md.movement_type = '201'
# MAGIC              THEN 1 ELSE 0 END) > 0   THEN 'CONSUMABLE'
# MAGIC     ELSE 'GENERAL'
# MAGIC   END                                 AS derived_material_type,
# MAGIC   CURRENT_DATE()                      AS ingestion_date,
# MAGIC   'SAP_S4H_DERIVED'                   AS source_system
# MAGIC FROM udw_procurement.silver.material_documents md
# MAGIC GROUP BY md.material_number, md.plant, md.base_unit;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 6 — Validate all three tables
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   'material_documents' AS table_name,
# MAGIC   COUNT(*)             AS records,
# MAGIC   COUNT(DISTINCT material_number) AS materials,
# MAGIC   COUNT(DISTINCT plant)           AS plants,
# MAGIC   MIN(posting_date)               AS earliest_date,
# MAGIC   MAX(posting_date)               AS latest_date
# MAGIC FROM udw_procurement.silver.material_documents
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'inventory',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT material_number),
# MAGIC   COUNT(DISTINCT plant),
# MAGIC   MIN(first_movement_date),
# MAGIC   MAX(last_movement_date)
# MAGIC FROM udw_procurement.silver.inventory
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'material_master',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT material_number),
# MAGIC   COUNT(DISTINCT plant),
# MAGIC   MIN(first_seen_date),
# MAGIC   MAX(last_seen_date)
# MAGIC FROM udw_procurement.silver.material_master;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 7 — Stock activity breakdown
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   stock_activity_status,
# MAGIC   stockout_risk,
# MAGIC   COUNT(*)                              AS material_plant_combinations,
# MAGIC   ROUND(SUM(current_stock_qty), 0)      AS total_current_stock,
# MAGIC   ROUND(SUM(estimated_stock_value), 0)  AS total_stock_value,
# MAGIC   ROUND(AVG(days_since_last_movement),0) AS avg_days_since_movement,
# MAGIC   ROUND(AVG(stock_turnover_ratio), 2)   AS avg_turnover_ratio
# MAGIC FROM udw_procurement.silver.inventory
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY
# MAGIC   CASE stockout_risk
# MAGIC     WHEN 'CRITICAL' THEN 1 WHEN 'HIGH'   THEN 2
# MAGIC     WHEN 'MEDIUM'   THEN 3 ELSE 4
# MAGIC   END,
# MAGIC   CASE stock_activity_status
# MAGIC     WHEN 'ZERO/NEGATIVE' THEN 1 WHEN 'SLOW MOVING' THEN 2
# MAGIC     WHEN 'MEDIUM MOVING' THEN 3 ELSE 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 8 — Movement type summary (what goods flows look like)
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   movement_type,
# MAGIC   movement_description,
# MAGIC   movement_direction,
# MAGIC   COUNT(*)                          AS movement_count,
# MAGIC   COUNT(DISTINCT material_number)   AS distinct_materials,
# MAGIC   COUNT(DISTINCT plant)             AS distinct_plants,
# MAGIC   ROUND(SUM(quantity), 0)           AS total_quantity,
# MAGIC   ROUND(SUM(amount_company_currency), 0) AS total_amount
# MAGIC FROM udw_procurement.silver.material_documents
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY movement_count DESC;