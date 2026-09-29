# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE udw_procurement.bronze.sap_sales_order_schedule_lines;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Silver Sales Orders
# MAGIC -- Using A_SalesOrder (header) + A_SalesOrderItem
# MAGIC -- Joined on SalesOrder key
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC -- Preview header fields available
# MAGIC SELECT
# MAGIC   SalesOrder, SalesOrderType, SalesOrganization,
# MAGIC   SoldToParty, TotalNetAmount, TransactionCurrency,
# MAGIC   OverallSDProcessStatus, OverallDeliveryStatus,
# MAGIC   OverallOrdReltdBillgStatus, RequestedDeliveryDate,
# MAGIC   CreationDate, ShippingCondition, IncotermsClassification,
# MAGIC   CustomerPaymentTerms, SalesDistrict
# MAGIC FROM udw_procurement.bronze.sap_sales_order_header
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE udw_procurement.bronze.sap_sales_order_items;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE udw_procurement.bronze.sap_sales_order_header;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview item fields
# MAGIC SELECT
# MAGIC   SalesOrder, SalesOrderItem, Material, MaterialGroup,
# MAGIC   OriginalPlant, RequestedQuantity, OrderQuantityUnit,
# MAGIC   NetAmount, TransactionCurrency, SalesDocumentRjcnReason,
# MAGIC   SDProcessStatus, DeliveryStatus
# MAGIC   -- RequestedDeliveryDate, ConfirmedDeliveryDate
# MAGIC FROM udw_procurement.bronze.sap_sales_order_items
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Create Silver Sales Orders
# MAGIC -- Joins header + items for complete picture
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.sales_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/sales_orders/'
# MAGIC AS
# MAGIC WITH so_header AS (
# MAGIC   SELECT
# MAGIC     SalesOrder                                        AS sales_order,
# MAGIC     SalesOrderType                                    AS order_type,
# MAGIC     SalesOrganization                                 AS sales_org,
# MAGIC     DistributionChannel                               AS distribution_channel,
# MAGIC     OrganizationDivision                              AS division,
# MAGIC     SoldToParty                                       AS customer_id,
# MAGIC     CAST(TotalNetAmount AS DOUBLE)                    AS header_total_net_amount,
# MAGIC     TransactionCurrency                               AS currency,
# MAGIC     OverallSDProcessStatus                            AS overall_process_status,
# MAGIC     OverallDeliveryStatus                             AS overall_delivery_status,
# MAGIC     OverallOrdReltdBillgStatus                        AS billing_status,
# MAGIC     OverallSDDocumentRejectionSts                     AS rejection_status,
# MAGIC     IncotermsClassification                           AS incoterms,
# MAGIC     IncotermsLocation1                                AS incoterms_location,
# MAGIC     ShippingCondition                                 AS shipping_condition,
# MAGIC     CustomerPaymentTerms                              AS payment_terms,
# MAGIC     PaymentMethod                                     AS payment_method,
# MAGIC     SalesDistrict                                     AS sales_district,
# MAGIC     SalesGroup                                        AS sales_group,
# MAGIC     SalesOffice                                       AS sales_office,
# MAGIC     PurchaseOrderByCustomer                           AS customer_po_number,
# MAGIC     SDDocumentReason                                  AS order_reason,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(CreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS creation_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(RequestedDeliveryDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS requested_delivery_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PricingDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS pricing_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(RequestedDeliveryDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS requested_delivery
# MAGIC   FROM udw_procurement.bronze.sap_sales_order_header
# MAGIC   WHERE SalesOrder IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY SalesOrder ORDER BY SalesOrder
# MAGIC   ) = 1
# MAGIC ),
# MAGIC so_items AS (
# MAGIC   SELECT
# MAGIC     SalesOrder                                        AS sales_order,
# MAGIC     SalesOrderItem                                    AS sales_order_item,
# MAGIC     Material                                          AS material_number,
# MAGIC     MaterialGroup                                     AS material_group,
# MAGIC     OriginalPlant                                             AS plant,
# MAGIC     ProductionPlant                                   AS production_plant,
# MAGIC     StorageLocation                                   AS storage_location,
# MAGIC     CAST(RequestedQuantity AS DOUBLE)                 AS requested_qty,
# MAGIC     OrderQuantityUnit                                 AS qty_unit,
# MAGIC     CAST(ConfdDelivQtyInOrderQtyUnit AS DOUBLE)       AS confirmed_qty,
# MAGIC     -- CAST(DeliveredQtyInOrderQtyUnit AS DOUBLE)        AS delivered_qty,
# MAGIC     CAST(ConfdDelivQtyInOrderQtyUnit AS DOUBLE)     AS open_confirmed_qty,
# MAGIC     CAST(NetAmount AS DOUBLE)                         AS item_net_amount,
# MAGIC     TransactionCurrency                               AS item_currency,
# MAGIC     SalesDocumentRjcnReason                           AS rejection_reason,
# MAGIC     SalesDocumentRjcnReason IS NULL                   AS is_active_order,
# MAGIC     SDProcessStatus                                   AS item_process_status,
# MAGIC     DeliveryStatus                                    AS item_delivery_status,
# MAGIC     HigherLevelItem                                   AS higher_level_item,
# MAGIC     ProfitCenter                                      AS profit_center,
# MAGIC     IncotermsClassification                           AS item_incoterms,
# MAGIC     WBSElement                                        AS wbs_element,
# MAGIC     -- Fulfillment rate
# MAGIC     CASE
# MAGIC       WHEN CAST(RequestedQuantity AS DOUBLE) > 0
# MAGIC       THEN ROUND(
# MAGIC         CAST(ConfdDelivQtyInOrderQtyUnit AS DOUBLE) /
# MAGIC         CAST(RequestedQuantity AS DOUBLE) * 100, 2
# MAGIC       )
# MAGIC       ELSE 0
# MAGIC     END                                               AS fulfillment_rate_pct,
# MAGIC     -- Open demand
# MAGIC     CAST(RequestedQuantity AS DOUBLE) -
# MAGIC     COALESCE(CAST(ConfdDelivQtyInOrderQtyUnit AS DOUBLE), 0)
# MAGIC                                                       AS open_demand_qty
# MAGIC     -- CAST(
# MAGIC     --   from_unixtime(
# MAGIC     --     CAST(regexp_extract(RequestedDeliveryDate,
# MAGIC     --       '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     --   ) AS DATE
# MAGIC     -- )                                                 AS item_requested_delivery,
# MAGIC     -- CAST(
# MAGIC     --   from_unixtime(
# MAGIC     --     CAST(regexp_extract(ConfirmedDeliveryDate,
# MAGIC     --       '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     --   ) AS DATE
# MAGIC     -- )                                                 AS confirmed_delivery_date
# MAGIC   FROM udw_procurement.bronze.sap_sales_order_items
# MAGIC   WHERE SalesOrder IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY SalesOrder, SalesOrderItem
# MAGIC     ORDER BY SalesOrder
# MAGIC   ) = 1
# MAGIC ),
# MAGIC so_schedules as (
# MAGIC   SELECT 
# MAGIC   SalesOrder as sales_order,
# MAGIC   DeliveredQtyInOrderQtyUnit as delivered_qty
# MAGIC   FROM udw_procurement.bronze.sap_sales_order_schedule_lines
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY SalesOrder, SalesOrderItem
# MAGIC     ORDER BY SalesOrder
# MAGIC   )
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Header fields
# MAGIC   h.sales_order,
# MAGIC   h.order_type,
# MAGIC   h.sales_org,
# MAGIC   h.distribution_channel,
# MAGIC   h.division,
# MAGIC   h.customer_id,
# MAGIC   h.header_total_net_amount,
# MAGIC   h.currency,
# MAGIC   h.overall_process_status,
# MAGIC   h.overall_delivery_status,
# MAGIC   h.billing_status,
# MAGIC   h.rejection_status,
# MAGIC   h.incoterms,
# MAGIC   h.incoterms_location,
# MAGIC   h.shipping_condition,
# MAGIC   h.payment_terms,
# MAGIC   h.customer_po_number,
# MAGIC   h.order_reason,
# MAGIC   h.creation_date,
# MAGIC   h.requested_delivery_date,
# MAGIC   h.pricing_date,
# MAGIC   h.sales_district,
# MAGIC
# MAGIC   -- Item fields
# MAGIC   i.sales_order_item,
# MAGIC   i.material_number,
# MAGIC   i.material_group,
# MAGIC   i.plant,
# MAGIC   i.production_plant,
# MAGIC   i.storage_location,
# MAGIC   i.requested_qty,
# MAGIC   i.qty_unit,
# MAGIC   i.confirmed_qty,
# MAGIC   j.delivered_qty,
# MAGIC   i.open_confirmed_qty,
# MAGIC   i.item_net_amount,
# MAGIC   i.rejection_reason,
# MAGIC   i.is_active_order,
# MAGIC   i.item_process_status,
# MAGIC   i.item_delivery_status,
# MAGIC   i.profit_center,
# MAGIC   i.wbs_element,
# MAGIC   i.fulfillment_rate_pct,
# MAGIC   i.open_demand_qty,
# MAGIC   h.requested_delivery,
# MAGIC --   i.confirmed_delivery_date,
# MAGIC
# MAGIC   -- Delivery commitment variance (confirmed vs requested)
# MAGIC --   DATEDIFF(
# MAGIC --     i.confirmed_delivery_date,
# MAGIC --     h.requested_delivery
# MAGIC --   )                                                   AS delivery_date_variance_days,
# MAGIC
# MAGIC   -- Time dimensions
# MAGIC   YEAR(h.creation_date)                               AS order_year,
# MAGIC   MONTH(h.creation_date)                              AS order_month,
# MAGIC   DATE_TRUNC('MONTH', h.creation_date)                AS order_month_date,
# MAGIC
# MAGIC   CURRENT_DATE()                                      AS ingestion_date,
# MAGIC   'SAP_S4H'                                           AS source_system
# MAGIC
# MAGIC FROM so_header h
# MAGIC LEFT JOIN so_items i ON h.sales_order = i.sales_order
# MAGIC LEFT JOIN so_schedules j ON h.sales_order = j.sales_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Validate Silver Sales Orders
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   order_year,
# MAGIC   order_month,
# MAGIC   COUNT(DISTINCT sales_order)           AS distinct_orders,
# MAGIC   COUNT(*)                              AS line_items,
# MAGIC   ROUND(SUM(item_net_amount), 2)        AS total_net_amount,
# MAGIC   currency,
# MAGIC   ROUND(AVG(fulfillment_rate_pct), 2)   AS avg_fulfillment_pct,
# MAGIC   SUM(CASE WHEN is_active_order
# MAGIC       THEN 1 ELSE 0 END)                AS active_line_items,
# MAGIC   ROUND(SUM(open_demand_qty), 0)        AS total_open_demand
# MAGIC FROM udw_procurement.silver.sales_orders
# MAGIC GROUP BY 1, 2, 6
# MAGIC ORDER BY 1 DESC, 2 DESC;