# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Silver Material Documents
# MAGIC -- Using A_MaterialDocumentItem (confirmed field names from Cell 6)
# MAGIC -- Joins with header for posting date
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC -- Preview header fields
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_doc_header LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Create Silver Material Documents
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.material_documents
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/material_documents/'
# MAGIC AS
# MAGIC WITH mat_doc_items AS (
# MAGIC   SELECT
# MAGIC     MaterialDocument                                  AS material_doc,
# MAGIC     MaterialDocumentYear                              AS doc_year,
# MAGIC     MaterialDocumentItem                              AS doc_item,
# MAGIC     MaterialDocumentLine                              AS doc_line,
# MAGIC     Material                                          AS material_number,
# MAGIC     Plant                                             AS plant,
# MAGIC     StorageLocation                                   AS storage_location,
# MAGIC     MaterialBaseUnit                                  AS base_unit,
# MAGIC     GoodsMovementType                                 AS movement_type,
# MAGIC     CASE GoodsMovementType
# MAGIC       WHEN '101' THEN 'GR for Purchase Order'
# MAGIC       WHEN '102' THEN 'GR Reversal for PO'
# MAGIC       WHEN '103' THEN 'GR into Blocked Stock'
# MAGIC       WHEN '122' THEN 'Return Delivery to Vendor'
# MAGIC       WHEN '201' THEN 'GI for Cost Center'
# MAGIC       WHEN '261' THEN 'GI for Production Order'
# MAGIC       WHEN '301' THEN 'Transfer Plant to Plant'
# MAGIC       WHEN '311' THEN 'Transfer Storage Location'
# MAGIC       WHEN '501' THEN 'Receipt without Reference'
# MAGIC       WHEN '551' THEN 'Scrapping'
# MAGIC       WHEN '601' THEN 'GI for Sales Order Delivery'
# MAGIC       WHEN '641' THEN 'Transfer to Own Stock'
# MAGIC       ELSE COALESCE(GoodsMovementType, 'Unknown')
# MAGIC     END                                               AS movement_description,
# MAGIC     CASE
# MAGIC       WHEN GoodsMovementType IN ('101','103','501','641') THEN 'INBOUND'
# MAGIC       WHEN GoodsMovementType IN ('102','122','201','261',
# MAGIC                                   '551','601')         THEN 'OUTBOUND'
# MAGIC       WHEN GoodsMovementType IN ('301','311')          THEN 'TRANSFER'
# MAGIC       ELSE 'OTHER'
# MAGIC     END                                               AS movement_direction,
# MAGIC     CAST(QuantityInBaseUnit AS DOUBLE)                AS quantity,
# MAGIC     CAST(QuantityInEntryUnit AS DOUBLE)               AS quantity_entry_unit,
# MAGIC     EntryUnit                                         AS entry_unit,
# MAGIC     CAST(GdsMvtExtAmtInCoCodeCrcy AS DOUBLE)          AS amount_company_currency,
# MAGIC     CompanyCodeCurrency                               AS company_currency,
# MAGIC     -- Reference documents
# MAGIC     PurchaseOrder                                     AS po_number,
# MAGIC     PurchaseOrderItem                                 AS po_item,
# MAGIC     ManufacturingOrder                                AS production_order,
# MAGIC     ManufacturingOrderItem                            AS production_order_item,
# MAGIC     SalesOrder                                        AS sales_order,
# MAGIC     SalesOrderItem                                    AS sales_order_item,
# MAGIC     Reservation                                       AS reservation,
# MAGIC     ReservationItem                                   AS reservation_item,
# MAGIC     Supplier                                          AS vendor_id,
# MAGIC     Customer                                          AS customer_id,
# MAGIC     -- Cost assignment
# MAGIC     CostCenter                                        AS cost_center,
# MAGIC     ControllingArea                                   AS controlling_area,
# MAGIC     FunctionalArea                                    AS functional_area,
# MAGIC     GLAccount                                         AS gl_account,
# MAGIC     WBSElement                                        AS wbs_element,
# MAGIC     -- Stock information
# MAGIC     InventoryStockType                                AS stock_type,
# MAGIC     IssuingOrReceivingPlant                           AS receiving_plant,
# MAGIC     IssuingOrReceivingStorageLoc                      AS receiving_storage_loc,
# MAGIC     -- Status
# MAGIC     CAST(GoodsMovementIsCancelled AS BOOLEAN)         AS is_cancelled,
# MAGIC     CAST(IsCompletelyDelivered AS BOOLEAN)            AS is_completely_delivered,
# MAGIC     FiscalYear                                        AS fiscal_year,
# MAGIC     FiscalYearPeriod                                  AS fiscal_period
# MAGIC   FROM udw_procurement.bronze.sap_material_doc_items
# MAGIC   WHERE MaterialDocument IS NOT NULL
# MAGIC     AND CAST(GoodsMovementIsCancelled AS BOOLEAN)
# MAGIC         IS DISTINCT FROM TRUE
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY MaterialDocument, MaterialDocumentItem
# MAGIC     ORDER BY MaterialDocument
# MAGIC   ) = 1
# MAGIC ),
# MAGIC mat_doc_header AS (
# MAGIC   SELECT
# MAGIC     MaterialDocument,
# MAGIC     MaterialDocumentYear,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PostingDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS posting_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(DocumentDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS document_date,
# MAGIC     CreatedByUser                                     AS created_by
# MAGIC   FROM udw_procurement.bronze.sap_material_doc_header
# MAGIC   WHERE MaterialDocument IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY MaterialDocument ORDER BY MaterialDocument
# MAGIC   ) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   i.*,
# MAGIC   h.posting_date,
# MAGIC   h.document_date,
# MAGIC   h.created_by,
# MAGIC   YEAR(h.posting_date)                               AS posting_year,
# MAGIC   MONTH(h.posting_date)                              AS posting_month,
# MAGIC   CURRENT_DATE()                                     AS ingestion_date,
# MAGIC   'SAP_S4H'                                          AS source_system
# MAGIC FROM mat_doc_items i
# MAGIC LEFT JOIN mat_doc_header h
# MAGIC   ON  i.material_doc = h.MaterialDocument
# MAGIC   AND i.doc_year     = h.MaterialDocumentYear;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Validate Material Documents
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   movement_type,
# MAGIC   movement_description,
# MAGIC   movement_direction,
# MAGIC   COUNT(*)                              AS movement_count,
# MAGIC   COUNT(DISTINCT material_number)       AS distinct_materials,
# MAGIC   COUNT(DISTINCT plant)                 AS distinct_plants,
# MAGIC   ROUND(SUM(quantity), 0)               AS total_quantity
# MAGIC FROM udw_procurement.silver.material_documents
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY movement_count DESC;