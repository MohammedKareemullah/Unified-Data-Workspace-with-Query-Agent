# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview raw fields before transforming
# MAGIC -- Run this first to confirm field names match your SAP system
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC -- Check PO header fields
# MAGIC SELECT * FROM udw_procurement.bronze.sap_purchase_orders LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE udw_procurement.bronze.sap_purchase_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Check PO item fields
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT * FROM udw_procurement.bronze.sap_po_items LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Check schedule lines (delivery dates per PO item)
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT * FROM udw_procurement.bronze.sap_po_schedule_lines LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 4 — Check pricing elements (conditions per PO item)
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT * FROM udw_procurement.bronze.sap_po_pricing LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 5 — Create Silver Purchase Orders Delta table
# MAGIC -- Using actual field names from your SAP system
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.purchase_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/purchase_orders/'
# MAGIC AS
# MAGIC WITH po_header AS (
# MAGIC   SELECT
# MAGIC     PurchaseOrder                                     AS po_number,
# MAGIC     PurchaseOrderType                                 AS po_type,
# MAGIC     PurchaseOrderSubtype                              AS po_subtype,
# MAGIC     Supplier                                          AS vendor_id,
# MAGIC     PurchasingOrganization                            AS purchasing_org,
# MAGIC     PurchasingGroup                                   AS purchasing_group,
# MAGIC     CompanyCode                                       AS company_code,
# MAGIC     DocumentCurrency                                  AS currency,
# MAGIC     SupplyingPlant                                    AS supplying_plant,
# MAGIC     PaymentTerms                                      AS payment_terms,
# MAGIC     IncotermsClassification                           AS incoterms,
# MAGIC     IncotermsLocation1                                AS incoterms_location,
# MAGIC     PurchasingProcessingStatus                        AS po_status,
# MAGIC     PurchasingDocumentOrigin                          AS po_origin,
# MAGIC     ExchangeRate                                      AS exchange_rate,
# MAGIC     -- Address fields on the header
# MAGIC     AddressCityName                                   AS supplier_city,
# MAGIC     AddressCountry                                    AS supplier_country,
# MAGIC     AddressRegion                                     AS supplier_region,
# MAGIC     AddressPostalCode                                 AS supplier_postal_code,
# MAGIC     AddressStreetName                                 AS supplier_street,
# MAGIC     AddressPhoneNumber                                AS supplier_phone,
# MAGIC     -- Dates
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(CreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS po_creation_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PurchaseOrderDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS po_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(ValidityStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS validity_start_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(ValidityEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS validity_end_date
# MAGIC   FROM udw_procurement.bronze.sap_purchase_orders
# MAGIC   WHERE PurchaseOrder IS NOT NULL
# MAGIC     AND Supplier IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY PurchaseOrder ORDER BY PurchaseOrder
# MAGIC   ) = 1
# MAGIC ),
# MAGIC po_items AS (
# MAGIC   SELECT
# MAGIC     PurchaseOrder                                     AS po_number,
# MAGIC     PurchaseOrderItem                                 AS po_item,
# MAGIC     Material                                          AS material_number,
# MAGIC     MaterialGroup                                     AS material_group,
# MAGIC     Plant                                             AS plant,
# MAGIC     StorageLocation                                   AS storage_location,
# MAGIC     CAST(OrderQuantity AS DOUBLE)                     AS order_qty,
# MAGIC     PurchaseOrderQuantityUnit                         AS order_unit,
# MAGIC     CAST(NetPriceAmount AS DOUBLE)                    AS net_price,
# MAGIC     CAST(NetPriceQuantity AS DOUBLE)                  AS net_price_qty,
# MAGIC     -- Net amount derived since not directly in items
# MAGIC     CAST(OrderQuantity AS DOUBLE) *
# MAGIC     CAST(NetPriceAmount AS DOUBLE)                    AS net_amount,
# MAGIC     TaxCode                                           AS tax_code,
# MAGIC     AccountAssignmentCategory                         AS account_category,
# MAGIC     PurchaseOrderItemCategory                         AS item_category,
# MAGIC     PurchaseOrderItemText                             AS item_text,
# MAGIC     -- GR/IV flags directly from SAP
# MAGIC     CAST(GoodsReceiptIsExpected AS BOOLEAN)           AS gr_expected,
# MAGIC     CAST(IsCompletelyDelivered AS BOOLEAN)            AS is_fully_received,
# MAGIC     CAST(IsFinallyInvoiced AS BOOLEAN)                AS is_fully_invoiced,
# MAGIC     CAST(InvoiceIsExpected AS BOOLEAN)                AS invoice_expected,
# MAGIC     CAST(PurchasingItemIsFreeOfCharge AS BOOLEAN)     AS is_free_of_charge,
# MAGIC     -- Overdelivery tolerance
# MAGIC     CAST(OverdelivTolrtdLmtRatioInPct AS DOUBLE)      AS overdelivery_tolerance_pct,
# MAGIC     CAST(UnderdelivTolrtdLmtRatioInPct AS DOUBLE)     AS underdelivery_tolerance_pct,
# MAGIC     -- Delivery address
# MAGIC     DeliveryAddressCityName                           AS delivery_city,
# MAGIC     DeliveryAddressCountry                            AS delivery_country,
# MAGIC     DeliveryAddressRegion                             AS delivery_region,
# MAGIC     -- Source references
# MAGIC     PurchaseRequisition                               AS purchase_requisition,
# MAGIC     PurchaseRequisitionItem                           AS pr_item,
# MAGIC     PurchasingInfoRecord                              AS info_record
# MAGIC   FROM udw_procurement.bronze.sap_po_items
# MAGIC   WHERE PurchaseOrder IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY PurchaseOrder, PurchaseOrderItem
# MAGIC     ORDER BY PurchaseOrder
# MAGIC   ) = 1
# MAGIC ),
# MAGIC schedule_lines AS (
# MAGIC   SELECT
# MAGIC     PurchasingDocument                                AS po_number,
# MAGIC     PurchasingDocumentItem                            AS po_item,
# MAGIC     MIN(
# MAGIC       CAST(
# MAGIC         from_unixtime(
# MAGIC           CAST(regexp_extract(ScheduleLineDeliveryDate,
# MAGIC             '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC         ) AS DATE
# MAGIC       )
# MAGIC     )                                                 AS scheduled_delivery_date,
# MAGIC     SUM(CAST(ScheduleLineOrderQuantity AS DOUBLE))    AS scheduled_qty,
# MAGIC     SUM(CAST(ScheduleLineCommittedQuantity AS DOUBLE)) AS committed_qty
# MAGIC   FROM udw_procurement.bronze.sap_po_schedule_lines
# MAGIC   WHERE PurchasingDocument IS NOT NULL
# MAGIC   GROUP BY 1, 2
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Header fields
# MAGIC   h.po_number,
# MAGIC   h.po_type,
# MAGIC   h.po_subtype,
# MAGIC   h.vendor_id,
# MAGIC   h.purchasing_org,
# MAGIC   h.purchasing_group,
# MAGIC   h.company_code,
# MAGIC   h.currency,
# MAGIC   h.supplying_plant,
# MAGIC   h.payment_terms,
# MAGIC   h.incoterms,
# MAGIC   h.incoterms_location,
# MAGIC   h.po_status,
# MAGIC   h.po_origin,
# MAGIC   h.exchange_rate,
# MAGIC   h.supplier_city,
# MAGIC   h.supplier_country,
# MAGIC   h.supplier_region,
# MAGIC   h.supplier_postal_code,
# MAGIC   h.supplier_street,
# MAGIC   h.po_creation_date,
# MAGIC   h.po_date,
# MAGIC   h.validity_start_date,
# MAGIC   h.validity_end_date,
# MAGIC
# MAGIC   -- Item fields
# MAGIC   i.po_item,
# MAGIC   i.material_number,
# MAGIC   i.material_group,
# MAGIC   i.plant,
# MAGIC   i.storage_location,
# MAGIC   i.order_qty,
# MAGIC   i.order_unit,
# MAGIC   i.net_price,
# MAGIC   i.net_price_qty,
# MAGIC   i.net_amount,
# MAGIC   i.tax_code,
# MAGIC   i.account_category,
# MAGIC   i.item_category,
# MAGIC   i.item_text,
# MAGIC   i.gr_expected,
# MAGIC   i.is_fully_received,
# MAGIC   i.is_fully_invoiced,
# MAGIC   i.invoice_expected,
# MAGIC   i.is_free_of_charge,
# MAGIC   i.overdelivery_tolerance_pct,
# MAGIC   i.underdelivery_tolerance_pct,
# MAGIC   i.delivery_city,
# MAGIC   i.delivery_country,
# MAGIC   i.purchase_requisition,
# MAGIC   i.pr_item,
# MAGIC   i.info_record,
# MAGIC
# MAGIC   -- Schedule line fields
# MAGIC   s.scheduled_delivery_date,
# MAGIC   s.scheduled_qty,
# MAGIC   s.committed_qty,
# MAGIC
# MAGIC   -- Calculated fields
# MAGIC   ROUND(i.net_amount, 2)                            AS line_total_amount,
# MAGIC
# MAGIC   -- Delivery performance
# MAGIC   DATEDIFF(s.scheduled_delivery_date, h.po_date)    AS po_to_scheduled_days,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN i.is_fully_received = TRUE
# MAGIC     THEN 'COMPLETED'
# MAGIC     WHEN i.is_fully_received = FALSE
# MAGIC       AND s.scheduled_delivery_date < CURRENT_DATE()
# MAGIC     THEN 'OVERDUE'
# MAGIC     WHEN i.is_fully_received = FALSE
# MAGIC     THEN 'PENDING'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC   END                                               AS delivery_status,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM po_header h
# MAGIC INNER JOIN po_items i
# MAGIC   ON  h.po_number = i.po_number
# MAGIC LEFT JOIN schedule_lines s
# MAGIC   ON  h.po_number = s.po_number
# MAGIC   AND i.po_item   = s.po_item;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 6 — Validate
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(*)                              AS total_line_items,
# MAGIC   COUNT(DISTINCT po_number)             AS distinct_pos,
# MAGIC   COUNT(DISTINCT vendor_id)             AS distinct_vendors,
# MAGIC   COUNT(DISTINCT material_group)        AS material_groups,
# MAGIC   COUNT(DISTINCT plant)                 AS plants,
# MAGIC   MIN(po_creation_date)                 AS earliest_po,
# MAGIC   MAX(po_creation_date)                 AS latest_po,
# MAGIC   ROUND(SUM(net_amount), 2)             AS total_spend,
# MAGIC   COUNT(DISTINCT currency)              AS currencies,
# MAGIC   SUM(CASE WHEN is_fully_received
# MAGIC       THEN 1 ELSE 0 END)                AS fully_received,
# MAGIC   SUM(CASE WHEN delivery_status = 'OVERDUE'
# MAGIC       THEN 1 ELSE 0 END)                AS overdue_items,
# MAGIC   SUM(CASE WHEN delivery_status = 'PENDING'
# MAGIC       THEN 1 ELSE 0 END)                AS pending_items
# MAGIC FROM udw_procurement.silver.purchase_orders;