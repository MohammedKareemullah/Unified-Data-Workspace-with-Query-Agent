# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview actual data values in each table
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC SELECT
# MAGIC   InspectionLot,
# MAGIC   Material,
# MAGIC   Plant,
# MAGIC   Supplier,
# MAGIC   PurchasingDocument,
# MAGIC   InspectionLotType,
# MAGIC   InspectionLotOrigin,
# MAGIC   InspectionLotQuantity,
# MAGIC   InspectionLotQuantityUnit,
# MAGIC   InspectionLotDefectiveQuantity,
# MAGIC   InspectionLotQualityScore,
# MAGIC   InspectionLotUsageDecisionCode,
# MAGIC   InspectionLotHasUsageDecision,
# MAGIC   InspectionLotStartDate,
# MAGIC   InspectionLotEndDate
# MAGIC FROM udw_procurement.bronze.sap_inspection_lots
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ManufacturingOrder,
# MAGIC   Material,
# MAGIC   Plant,
# MAGIC   ManufacturingOrderType,
# MAGIC   MfgOrderPlannedScrapQty,
# MAGIC   MfgOrderConfirmedYieldQty,
# MAGIC   MfgOrderItemActualDeviationQty,
# MAGIC   MfgOrderPlannedStartDate,
# MAGIC   MfgOrderPlannedEndDate,
# MAGIC   MfgOrderActualReleaseDate,
# MAGIC   OrderIsReleased,
# MAGIC   OrderIsConfirmed,
# MAGIC   OrderIsTechnicallyCompleted,
# MAGIC   OrderIsClosed,
# MAGIC   ProductionVersion
# MAGIC FROM udw_procurement.bronze.sap_production_orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SalesOrder,
# MAGIC   SalesOrderItem,
# MAGIC   SalesOrderType,
# MAGIC   SalesOrganization,
# MAGIC   SoldToParty,
# MAGIC   Material,
# MAGIC   MaterialGroup,
# MAGIC   OriginalPlant,
# MAGIC   ProductionPlant,
# MAGIC   RequestedQuantity,
# MAGIC   RequestedQuantityUnit,
# MAGIC   NetAmount,
# MAGIC   TransactionCurrency,
# MAGIC   SalesDocumentRjcnReason,
# MAGIC   CreationDate,
# MAGIC   RequestedDeliveryDate,
# MAGIC   OverallSDProcessStatus,
# MAGIC   DeliveryStatus
# MAGIC FROM udw_procurement.bronze.sap_sales_orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Silver Inspection Lots
# MAGIC -- Quality management data with supplier linkage
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.inspection_lots
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/inspection_lots/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   InspectionLot                                     AS inspection_lot,
# MAGIC   InspectionLotType                                 AS lot_type,
# MAGIC   InspectionLotOrigin                               AS lot_origin,
# MAGIC
# MAGIC   -- Material and location
# MAGIC   Material                                          AS material_number,
# MAGIC   Plant                                             AS plant,
# MAGIC   InspectionLotPlant                                AS inspection_plant,
# MAGIC   StorageLocation                                   AS storage_location,
# MAGIC   Batch                                             AS batch,
# MAGIC
# MAGIC   -- Supplier and purchasing link
# MAGIC   Supplier                                          AS vendor_id,
# MAGIC   PurchasingDocument                                AS po_number,
# MAGIC   PurchasingDocumentItem                            AS po_item,
# MAGIC
# MAGIC   -- Sales link
# MAGIC   SalesOrder                                        AS sales_order,
# MAGIC   SalesOrderItem                                    AS sales_order_item,
# MAGIC
# MAGIC   -- Production link
# MAGIC   ManufacturingOrder                                AS production_order,
# MAGIC
# MAGIC   -- Quantities
# MAGIC   CAST(InspectionLotQuantity AS DOUBLE)             AS lot_quantity,
# MAGIC   InspectionLotQuantityUnit                         AS quantity_unit,
# MAGIC   CAST(InspectionLotSampleQuantity AS DOUBLE)       AS sample_quantity,
# MAGIC   CAST(InspectionLotDefectiveQuantity AS DOUBLE)    AS defective_quantity,
# MAGIC   CAST(InspLotQtyToScrap AS DOUBLE)                 AS scrap_quantity,
# MAGIC   CAST(InspLotQtyToFree AS DOUBLE)                  AS accepted_quantity,
# MAGIC   CAST(InspLotQtyToBlocked AS DOUBLE)               AS blocked_quantity,
# MAGIC   CAST(InspLotQtyReturnedToSupplier AS DOUBLE)      AS returned_quantity,
# MAGIC   CAST(InspLotQtyInspected AS DOUBLE)               AS inspected_quantity,
# MAGIC
# MAGIC   -- Quality scoring
# MAGIC   CAST(InspectionLotQualityScore AS DOUBLE)         AS quality_score,
# MAGIC   InspectionLotUsageDecisionCode                    AS usage_decision_code,
# MAGIC   CAST(InspectionLotHasUsageDecision AS BOOLEAN)    AS has_usage_decision,
# MAGIC   InspLotUsageDecisionValuation                     AS usage_decision_valuation,
# MAGIC   -- Accepted = usage decision valuation is Accepted (A)
# MAGIC   CASE InspLotUsageDecisionValuation
# MAGIC     WHEN 'A' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS is_accepted,
# MAGIC
# MAGIC   -- Defect metrics
# MAGIC   CAST(InspectionNumberOfDefects AS BIGINT)         AS number_of_defects,
# MAGIC   CAST(InspRsltNonconformingValsNmbr AS BIGINT)     AS nonconforming_values,
# MAGIC --   CAST(InspLotNmbrOfNonconformingUnits AS BIGINT)   AS nonconforming_units,
# MAGIC   
# MAGIC
# MAGIC   -- Defect rate
# MAGIC   CASE
# MAGIC     WHEN CAST(InspectionLotQuantity AS DOUBLE) > 0
# MAGIC     THEN ROUND(
# MAGIC       CAST(InspectionLotDefectiveQuantity AS DOUBLE) /
# MAGIC       CAST(InspectionLotQuantity AS DOUBLE) * 100, 4
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END                                               AS defect_rate_pct,
# MAGIC
# MAGIC   -- Scrap rate
# MAGIC   CASE
# MAGIC     WHEN CAST(InspectionLotQuantity AS DOUBLE) > 0
# MAGIC     THEN ROUND(
# MAGIC       CAST(InspLotQtyToScrap AS DOUBLE) /
# MAGIC       CAST(InspectionLotQuantity AS DOUBLE) * 100, 4
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END                                               AS scrap_rate_pct,
# MAGIC
# MAGIC   -- Return rate
# MAGIC   CASE
# MAGIC     WHEN CAST(InspectionLotQuantity AS DOUBLE) > 0
# MAGIC     THEN ROUND(
# MAGIC       CAST(InspLotQtyReturnedToSupplier AS DOUBLE) /
# MAGIC       CAST(InspectionLotQuantity AS DOUBLE) * 100, 4
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END                                               AS return_rate_pct,
# MAGIC
# MAGIC   -- Quality classification
# MAGIC   CASE
# MAGIC     WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 90
# MAGIC     THEN 'EXCELLENT'
# MAGIC     WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 75
# MAGIC     THEN 'GOOD'
# MAGIC     WHEN CAST(InspectionLotQualityScore AS DOUBLE) >= 50
# MAGIC     THEN 'ACCEPTABLE'
# MAGIC     WHEN CAST(InspectionLotQualityScore AS DOUBLE) > 0
# MAGIC     THEN 'POOR'
# MAGIC     ELSE 'NOT SCORED'
# MAGIC   END                                               AS quality_grade,
# MAGIC
# MAGIC   -- Status flags
# MAGIC   CAST(InspectionLotIsSkipped AS BOOLEAN)           AS is_skipped,
# MAGIC   CAST(InspectionLotIsFullInspection AS BOOLEAN)    AS is_full_inspection,
# MAGIC   CAST(InspLotIsStockPostingCompleted AS BOOLEAN)   AS stock_posting_complete,
# MAGIC
# MAGIC   -- Dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(InspectionLotStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS lot_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(InspectionLotEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS lot_end_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(InspectionLotCreatedOn,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS lot_created_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(InspectionLotUsageDecidedOn,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS usage_decided_date,
# MAGIC
# MAGIC   -- Inspection duration in days
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS inspection_duration_days,
# MAGIC
# MAGIC   -- Fiscal period for aggregation
# MAGIC   YEAR(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotCreatedOn,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS fiscal_year,
# MAGIC   MONTH(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(InspectionLotCreatedOn,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS fiscal_month,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_inspection_lots
# MAGIC WHERE InspectionLot IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY InspectionLot ORDER BY InspectionLot
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Silver Production Orders
# MAGIC -- Manufacturing execution with schedule adherence
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.production_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/production_orders/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   ManufacturingOrder                                AS production_order,
# MAGIC   ManufacturingOrderType                            AS order_type,
# MAGIC   ManufacturingOrderCategory                        AS order_category,
# MAGIC   ProductionVersion                                 AS production_version,
# MAGIC
# MAGIC   -- Material and location
# MAGIC   Material                                          AS material_number,
# MAGIC   MaterialGroup                                     AS material_group,
# MAGIC   Plant                                             AS plant,
# MAGIC   ProductionPlant                                   AS production_plant,
# MAGIC   StorageLocation                                   AS storage_location,
# MAGIC   MRPArea                                           AS mrp_area,
# MAGIC   MRPController                                     AS mrp_controller,
# MAGIC   ProductionSupervisor                              AS production_supervisor,
# MAGIC   WorkCenter                                        AS work_center,
# MAGIC
# MAGIC   -- Quantities
# MAGIC   CAST(MfgOrderItemPlannedTotalQty AS DOUBLE)       AS planned_qty,
# MAGIC   CAST(MfgOrderConfirmedYieldQty AS DOUBLE)         AS confirmed_yield_qty,
# MAGIC   CAST(MfgOrderItemActualDeviationQty AS DOUBLE)    AS deviation_qty,
# MAGIC   CAST(MfgOrderItemPlannedScrapQty AS DOUBLE)       AS planned_scrap_qty,
# MAGIC   CAST(MfgOrderItemGoodsReceiptQty AS DOUBLE)       AS goods_receipt_qty,
# MAGIC   ProductionUnit                                    AS production_unit,
# MAGIC   BaseUnit                                          AS base_unit,
# MAGIC
# MAGIC   -- Completion metrics
# MAGIC   CASE
# MAGIC     WHEN CAST(MfgOrderItemPlannedTotalQty AS DOUBLE) > 0
# MAGIC     THEN ROUND(
# MAGIC       CAST(MfgOrderConfirmedYieldQty AS DOUBLE) /
# MAGIC       CAST(MfgOrderItemPlannedTotalQty AS DOUBLE) * 100, 2
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END                                               AS completion_pct,
# MAGIC
# MAGIC   -- Yield rate (good output vs planned)
# MAGIC   CASE
# MAGIC     WHEN CAST(MfgOrderItemPlannedTotalQty AS DOUBLE) > 0
# MAGIC     THEN ROUND(
# MAGIC       CAST(MfgOrderItemGoodsReceiptQty AS DOUBLE) /
# MAGIC       CAST(MfgOrderItemPlannedTotalQty AS DOUBLE) * 100, 2
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END                                               AS yield_rate_pct,
# MAGIC
# MAGIC   -- Order status flags
# MAGIC   CASE
# MAGIC     WHEN OrderIsCreated = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_created,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN OrderIsReleased = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_released,
# MAGIC   
# MAGIC   CASE
# MAGIC     WHEN OrderIsConfirmed = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_confirmed,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN OrderIsPartiallyDelivered = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_partial_delivery,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN OrderIsDelivered = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_delivered,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN OrderIsTechnicallyCompleted = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_teco,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN OrderIsClosed = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_closed,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN OrderIsLocked = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_locked,
# MAGIC   
# MAGIC   CASE
# MAGIC     WHEN OrderIsMarkedForDeletion = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS status_deletion,
# MAGIC     
# MAGIC   -- CAST(OrderIsMarkedForDeletion AS BOOLEAN)         AS status_deletion,
# MAGIC
# MAGIC   -- Derived order status
# MAGIC   CASE
# MAGIC     WHEN OrderIsClosed = 'X'             THEN 'CLOSED'
# MAGIC     WHEN OrderIsTechnicallyCompleted = 'X' THEN 'TECO'
# MAGIC     WHEN OrderIsConfirmed = 'X'          THEN 'CONFIRMED'
# MAGIC     WHEN OrderIsDelivered = 'X'          THEN 'DELIVERED'
# MAGIC     WHEN OrderIsReleased = 'X'           THEN 'RELEASED'
# MAGIC     WHEN OrderIsCreated = 'X'            THEN 'CREATED'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC   END                                               AS order_status,
# MAGIC
# MAGIC   -- References
# MAGIC   SalesOrder                                        AS sales_order,
# MAGIC   SalesOrderItem                                    AS sales_order_item,
# MAGIC   PlannedOrder                                      AS planned_order,
# MAGIC
# MAGIC   -- Planned dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MfgOrderPlannedStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS planned_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MfgOrderPlannedEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS planned_end_date,
# MAGIC
# MAGIC   -- Scheduled dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MfgOrderScheduledStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS scheduled_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MfgOrderScheduledEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS scheduled_end_date,
# MAGIC
# MAGIC   -- Actual dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MfgOrderActualReleaseDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS actual_release_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MfgOrderItemActualDeliveryDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS actual_delivery_date,
# MAGIC
# MAGIC   -- Creation date
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MfgOrderCreationDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS creation_date,
# MAGIC
# MAGIC   -- Schedule adherence
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderItemActualDeliveryDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderPlannedEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS schedule_variance_days,
# MAGIC
# MAGIC   -- Late flag
# MAGIC   CASE
# MAGIC     WHEN DATEDIFF(
# MAGIC       CAST(
# MAGIC         from_unixtime(
# MAGIC           CAST(regexp_extract(MfgOrderItemActualDeliveryDate,
# MAGIC             '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC         ) AS DATE
# MAGIC       ),
# MAGIC       CAST(
# MAGIC         from_unixtime(
# MAGIC           CAST(regexp_extract(MfgOrderPlannedEndDate,
# MAGIC             '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC         ) AS DATE
# MAGIC       )
# MAGIC     ) > 0 THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS is_late,
# MAGIC
# MAGIC   -- Planned duration in days
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderPlannedEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderPlannedStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS planned_duration_days,
# MAGIC
# MAGIC   -- Component data (from flattened BOM)
# MAGIC   BOMItem                                           AS bom_item,
# MAGIC   ManufacturingOrderItem                            AS order_item,
# MAGIC   CAST(RequiredQuantity AS DOUBLE)                  AS required_component_qty,
# MAGIC   CAST(WithdrawnQuantity AS DOUBLE)                 AS withdrawn_component_qty,
# MAGIC   ManufacturingOrderOperation                       AS operation,
# MAGIC
# MAGIC   -- Fiscal period
# MAGIC   YEAR(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderCreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS creation_year,
# MAGIC   MONTH(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderCreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS creation_month,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_production_orders
# MAGIC WHERE ManufacturingOrder IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY ManufacturingOrder, ManufacturingOrderItem
# MAGIC   ORDER BY ManufacturingOrder
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 4 — Silver Sales Orders
# MAGIC -- Demand signal for supply chain planning
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.sales_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/sales_orders/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   SalesOrder                                        AS sales_order,
# MAGIC   SalesOrderItem                                    AS sales_order_item,
# MAGIC   SalesOrderType                                    AS order_type,
# MAGIC   SalesOrganization                                 AS sales_org,
# MAGIC   DistributionChannel                               AS distribution_channel,
# MAGIC   OrganizationDivision                              AS division,
# MAGIC
# MAGIC   -- Customer
# MAGIC   SoldToParty                                       AS customer_id,
# MAGIC   Customer                                          AS customer_alt,
# MAGIC
# MAGIC   -- Material
# MAGIC   Material                                          AS material_number,
# MAGIC   MaterialGroup                                     AS material_group,
# MAGIC   OriginalPlant                                             AS plant,
# MAGIC   ProductionPlant                                   AS production_plant,
# MAGIC   StorageLocation                                   AS storage_location,
# MAGIC   Batch                                             AS batch,
# MAGIC
# MAGIC   -- Quantities
# MAGIC   CAST(RequestedQuantity AS DOUBLE)                 AS requested_qty,
# MAGIC   RequestedQuantityUnit                             AS qty_unit,
# MAGIC   CAST(ConfdDelivQtyInOrderQtyUnit AS DOUBLE)       AS confirmed_qty,
# MAGIC   CAST(DeliveredQtyInOrderQtyUnit AS DOUBLE)        AS delivered_qty,
# MAGIC   CAST(OpenConfdDelivQtyInOrdQtyUnit AS DOUBLE)     AS open_confirmed_qty,
# MAGIC   CAST(ScheduleLineOrderQuantity AS DOUBLE)         AS schedule_line_qty,
# MAGIC
# MAGIC   -- Financials
# MAGIC   CAST(NetAmount AS DOUBLE)                         AS net_amount,
# MAGIC   TransactionCurrency                               AS currency,
# MAGIC   CAST(TotalNetAmount AS DOUBLE)                    AS total_net_amount,
# MAGIC   CAST(TaxAmount AS DOUBLE)                         AS tax_amount,
# MAGIC   CAST(CostAmount AS DOUBLE)                        AS cost_amount,
# MAGIC
# MAGIC   -- Delivery and fulfillment
# MAGIC   DeliveryStatus                                    AS delivery_status,
# MAGIC   OverallSDProcessStatus                            AS overall_process_status,
# MAGIC   OverallDeliveryStatus                             AS overall_delivery_status,
# MAGIC   OverallTotalDeliveryStatus                        AS total_delivery_status,
# MAGIC   SalesDocumentRjcnReason                           AS rejection_reason,
# MAGIC   SalesDocumentRjcnReason IS NULL                   AS is_active_order,
# MAGIC
# MAGIC   -- Fulfillment rate
# MAGIC   CASE
# MAGIC     WHEN CAST(RequestedQuantity AS DOUBLE) > 0
# MAGIC     THEN ROUND(
# MAGIC       CAST(DeliveredQtyInOrderQtyUnit AS DOUBLE) /
# MAGIC       CAST(RequestedQuantity AS DOUBLE) * 100, 2
# MAGIC     )
# MAGIC     ELSE 0
# MAGIC   END                                               AS fulfillment_rate_pct,
# MAGIC
# MAGIC   -- Open demand
# MAGIC   CAST(RequestedQuantity AS DOUBLE) -
# MAGIC   COALESCE(CAST(DeliveredQtyInOrderQtyUnit AS DOUBLE), 0)
# MAGIC                                                     AS open_demand_qty,
# MAGIC
# MAGIC   -- Incoterms and shipping
# MAGIC   IncotermsClassification                           AS incoterms,
# MAGIC   IncotermsLocation1                                AS incoterms_location,
# MAGIC   ShippingCondition                                 AS shipping_condition,
# MAGIC   ShippingPoint                                     AS shipping_point,
# MAGIC   DeliveryPriority                                  AS delivery_priority,
# MAGIC
# MAGIC   -- Payment
# MAGIC   CustomerPaymentTerms                              AS payment_terms,
# MAGIC   PaymentMethod                                     AS payment_method,
# MAGIC
# MAGIC   -- Pricing
# MAGIC   PriceListType                                     AS price_list_type,
# MAGIC   CAST(ConditionAmount AS DOUBLE)                   AS condition_amount,
# MAGIC   ConditionCurrency                                 AS condition_currency,
# MAGIC   ConditionType                                     AS condition_type,
# MAGIC
# MAGIC   -- Cross-references
# MAGIC   PurchaseOrderByCustomer                           AS customer_po_number,
# MAGIC   ReferenceSDDocument                               AS reference_so,
# MAGIC   Supplier                                          AS supplier_id,
# MAGIC   WBSElement                                        AS wbs_element,
# MAGIC   ProfitCenter                                      AS profit_center,
# MAGIC
# MAGIC   -- Address
# MAGIC   CityName                                          AS ship_to_city,
# MAGIC   Country                                           AS ship_to_country,
# MAGIC   Region                                            AS ship_to_region,
# MAGIC   PostalCode                                        AS ship_to_postal_code,
# MAGIC
# MAGIC   -- Dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(CreationDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS creation_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(RequestedDeliveryDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS requested_delivery_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(ConfirmedDeliveryDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS confirmed_delivery_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(BillingDocumentDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS billing_date,
# MAGIC
# MAGIC   -- Delivery commitment variance (confirmed vs requested)
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(ConfirmedDeliveryDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(RequestedDeliveryDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS delivery_date_variance_days,
# MAGIC
# MAGIC   -- Time dimensions
# MAGIC   YEAR(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(CreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS order_year,
# MAGIC   MONTH(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(CreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS order_month,
# MAGIC   DATE_TRUNC('MONTH',
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(CreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS order_month_date,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_sales_orders
# MAGIC WHERE SalesOrder IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY SalesOrder, SalesOrderItem
# MAGIC   ORDER BY SalesOrder
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 5 — Silver Logistics Rates
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.logistics_rates
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/logistics_rates/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   shipment_id,
# MAGIC   scenario_id,
# MAGIC   vendor_id,
# MAGIC   vendor_name,
# MAGIC   vendor_country,
# MAGIC   plant_code,
# MAGIC   plant_name,
# MAGIC   material_group,
# MAGIC   carrier,
# MAGIC   service,
# MAGIC   CAST(rate_usd AS DOUBLE)              AS rate_usd,
# MAGIC   CAST(est_delivery_days AS INT)        AS est_delivery_days,
# MAGIC   CAST(delivery_guaranteed AS BOOLEAN)  AS delivery_guaranteed,
# MAGIC   from_country,
# MAGIC   to_country,
# MAGIC   CAST(run_date AS DATE)                AS run_date
# MAGIC FROM udw_procurement.bronze.logistics_rates
# MAGIC WHERE shipment_id IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY shipment_id, carrier, service
# MAGIC   ORDER BY run_date DESC
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 6 — Validate all Silver tables in this notebook
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   'inspection_lots'     AS table_name,
# MAGIC   COUNT(*)              AS total_records,
# MAGIC   COUNT(DISTINCT vendor_id)   AS distinct_vendors,
# MAGIC   COUNT(DISTINCT material_number) AS distinct_materials,
# MAGIC   ROUND(AVG(defect_rate_pct), 4) AS avg_defect_rate_pct,
# MAGIC   ROUND(AVG(quality_score), 1)   AS avg_quality_score,
# MAGIC   SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) AS accepted_lots,
# MAGIC   COUNT(*) - SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) AS rejected_lots
# MAGIC FROM udw_procurement.silver.inspection_lots
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'production_orders',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT work_center),
# MAGIC   COUNT(DISTINCT material_number),
# MAGIC   ROUND(AVG(completion_pct), 2),
# MAGIC   ROUND(AVG(yield_rate_pct), 2),
# MAGIC   SUM(CASE WHEN status_teco OR status_closed THEN 1 ELSE 0 END),
# MAGIC   SUM(CASE WHEN is_late THEN 1 ELSE 0 END)
# MAGIC FROM udw_procurement.silver.production_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'sales_orders',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT customer_id),
# MAGIC   COUNT(DISTINCT material_number),
# MAGIC   ROUND(AVG(fulfillment_rate_pct), 2),
# MAGIC   ROUND(AVG(net_amount), 2),
# MAGIC   SUM(CASE WHEN is_active_order THEN 1 ELSE 0 END),
# MAGIC   SUM(CASE WHEN NOT is_active_order THEN 1 ELSE 0 END)
# MAGIC FROM udw_procurement.silver.sales_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'logistics_rates',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT vendor_id),
# MAGIC   COUNT(DISTINCT carrier),
# MAGIC   ROUND(AVG(rate_usd), 2),
# MAGIC   ROUND(MIN(rate_usd), 2),
# MAGIC   SUM(CASE WHEN delivery_guaranteed THEN 1 ELSE 0 END),
# MAGIC   COUNT(DISTINCT service)
# MAGIC FROM udw_procurement.silver.logistics_rates;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 7 — Quality analytics preview
# MAGIC -- What the inspection lot data tells us about suppliers
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   vendor_id,
# MAGIC   material_number,
# MAGIC   plant,
# MAGIC   COUNT(*)                              AS inspection_lots,
# MAGIC   ROUND(AVG(defect_rate_pct), 4)        AS avg_defect_rate,
# MAGIC   ROUND(AVG(scrap_rate_pct), 4)         AS avg_scrap_rate,
# MAGIC   ROUND(AVG(return_rate_pct), 4)        AS avg_return_rate,
# MAGIC   ROUND(AVG(quality_score), 1)          AS avg_quality_score,
# MAGIC   SUM(CASE WHEN is_accepted
# MAGIC       THEN 1 ELSE 0 END)                AS accepted,
# MAGIC   COUNT(*) - SUM(CASE WHEN is_accepted
# MAGIC                  THEN 1 ELSE 0 END)     AS rejected,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) * 100.0 /
# MAGIC     COUNT(*), 2
# MAGIC   )                                     AS acceptance_rate_pct,
# MAGIC   quality_grade
# MAGIC FROM udw_procurement.silver.inspection_lots
# MAGIC WHERE vendor_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3, 12
# MAGIC ORDER BY avg_defect_rate DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 8 — Production schedule adherence preview
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   order_status,
# MAGIC   plant,
# MAGIC   COUNT(*)                              AS order_count,
# MAGIC   COUNT(DISTINCT material_number)       AS materials,
# MAGIC   ROUND(AVG(planned_qty), 0)            AS avg_planned_qty,
# MAGIC   ROUND(AVG(completion_pct), 1)         AS avg_completion_pct,
# MAGIC   ROUND(AVG(yield_rate_pct), 1)         AS avg_yield_rate,
# MAGIC   SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_orders,
# MAGIC   ROUND(AVG(schedule_variance_days), 1) AS avg_schedule_variance_days
# MAGIC FROM udw_procurement.silver.production_orders
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 9 — Sales demand signal preview
# MAGIC -- Monthly demand by material group
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   order_year,
# MAGIC   order_month,
# MAGIC   material_group,
# MAGIC   sales_org,
# MAGIC   plant,
# MAGIC   COUNT(DISTINCT sales_order)           AS order_count,
# MAGIC   COUNT(*)                              AS line_items,
# MAGIC   ROUND(SUM(requested_qty), 0)          AS total_requested_qty,
# MAGIC   ROUND(SUM(confirmed_qty), 0)          AS total_confirmed_qty,
# MAGIC   ROUND(SUM(net_amount), 2)             AS total_net_amount,
# MAGIC   currency,
# MAGIC   ROUND(AVG(fulfillment_rate_pct), 2)   AS avg_fulfillment_pct,
# MAGIC   SUM(CASE WHEN is_active_order
# MAGIC       THEN 1 ELSE 0 END)                AS active_orders,
# MAGIC   ROUND(SUM(open_demand_qty), 0)        AS total_open_demand
# MAGIC FROM udw_procurement.silver.sales_orders
# MAGIC WHERE is_active_order = TRUE
# MAGIC GROUP BY 1, 2, 3, 4, 5, 11
# MAGIC ORDER BY order_year DESC, order_month DESC, total_net_amount DESC
# MAGIC LIMIT 30;