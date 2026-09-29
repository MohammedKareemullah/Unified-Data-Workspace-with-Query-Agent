# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview production order header fields
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC SELECT * FROM udw_procurement.bronze.sap_production_order_header LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM udw_procurement.bronze.sap_production_order_status LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DESCRIBE udw_procurement.bronze.sap_production_order_header;
# MAGIC
# MAGIC SELECT OrderIsReleased FROM udw_procurement.bronze.sap_production_order_header;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Create Silver Production Orders
# MAGIC -- Using A_ProductionOrder_2 fields confirmed from preview
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.production_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/production_orders/'
# MAGIC AS
# MAGIC WITH prod_header AS (
# MAGIC   SELECT
# MAGIC     ManufacturingOrder                                AS production_order,
# MAGIC     ManufacturingOrderType                            AS order_type,
# MAGIC     ManufacturingOrderCategory                        AS order_category,
# MAGIC     ProductionVersion                                 AS production_version,
# MAGIC     Material                                          AS material_number,
# MAGIC     -- MaterialGroup                                     AS material_group,
# MAGIC     Plant                                             AS plant,
# MAGIC     ProductionPlant                                   AS production_plant,
# MAGIC     StorageLocation                                   AS storage_location,
# MAGIC     MRPArea                                           AS mrp_area,
# MAGIC     MRPController                                     AS mrp_controller,
# MAGIC     ProductionSupervisor                              AS production_supervisor,
# MAGIC     -- WorkCenter                                        AS work_center,
# MAGIC     BusinessArea                                      AS business_area,
# MAGIC     CompanyCode                                       AS company_code,
# MAGIC     ProfitCenter                                      AS profit_center,
# MAGIC
# MAGIC     -- Quantities
# MAGIC     CAST(TotalQuantity AS DOUBLE)                     AS total_quantity,
# MAGIC     CAST(MfgOrderConfirmedYieldQty AS DOUBLE)         AS confirmed_yield_qty,
# MAGIC     CAST(MfgOrderPlannedScrapQty AS DOUBLE)           AS planned_scrap_qty,
# MAGIC     ProductionUnit                                    AS production_unit,
# MAGIC
# MAGIC     -- Completion
# MAGIC     CASE
# MAGIC       WHEN CAST(TotalQuantity AS DOUBLE) > 0
# MAGIC       THEN ROUND(
# MAGIC         CAST(MfgOrderConfirmedYieldQty AS DOUBLE) /
# MAGIC         CAST(TotalQuantity AS DOUBLE) * 100, 2
# MAGIC       )
# MAGIC       ELSE 0
# MAGIC     END                                               AS completion_pct,
# MAGIC
# MAGIC     -- Status flags from header
# MAGIC
# MAGIC     -- CAST(OrderIsCreated AS BOOLEAN)                   AS status_created,
# MAGIC     -- CAST(OrderIsReleased AS BOOLEAN)                  AS status_released,
# MAGIC     -- CAST(OrderIsConfirmed AS BOOLEAN)                 AS status_confirmed,
# MAGIC     -- CAST(OrderIsDelivered AS BOOLEAN)                 AS status_delivered,
# MAGIC     -- CAST(OrderIsTechnicallyCompleted AS BOOLEAN)      AS status_teco,
# MAGIC     -- CAST(OrderIsClosed AS BOOLEAN)                    AS status_closed,
# MAGIC     -- CAST(OrderIsLocked AS BOOLEAN)                    AS status_locked,
# MAGIC     -- CAST(OrderIsDeleted AS BOOLEAN)                   AS status_deleted,
# MAGIC     -- CAST(OrderIsPartiallyDelivered AS BOOLEAN)        AS status_partial_delivery,
# MAGIC     -- CAST(OrderIsPartiallyConfirmed AS BOOLEAN)        AS status_partial_confirmed,
# MAGIC     -- CAST(OrderIsMarkedForDeletion AS BOOLEAN)         AS status_marked_deletion,
# MAGIC
# MAGIC     CASE
# MAGIC     WHEN OrderIsCreated = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_created,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsReleased = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_released,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsConfirmed = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_confirmed,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsDelivered = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_delivered,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsTechnicallyCompleted = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_teco,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsClosed = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_closed,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsLocked = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_locked,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsDeleted = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_deleted,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsPartiallyDelivered = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_partial_delivery,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsPartiallyConfirmed = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_partial_confirmed,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN OrderIsMarkedForDeletion = 'X' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC END AS status_marked_deletion,
# MAGIC
# MAGIC     -- -- Derived order status
# MAGIC     -- CASE
# MAGIC     --   WHEN CAST(OrderIsClosed AS BOOLEAN)
# MAGIC     --     THEN 'CLOSED'
# MAGIC     --   WHEN CAST(OrderIsTechnicallyCompleted AS BOOLEAN)
# MAGIC     --     THEN 'TECO'
# MAGIC     --   WHEN CAST(OrderIsConfirmed AS BOOLEAN)
# MAGIC     --     THEN 'CONFIRMED'
# MAGIC     --   WHEN CAST(OrderIsDelivered AS BOOLEAN)
# MAGIC     --     THEN 'DELIVERED'
# MAGIC     --   WHEN CAST(OrderIsPartiallyDelivered AS BOOLEAN)
# MAGIC     --     THEN 'PARTIALLY DELIVERED'
# MAGIC     --   WHEN CAST(OrderIsReleased AS BOOLEAN)
# MAGIC     --     THEN 'RELEASED'
# MAGIC     --   WHEN CAST(OrderIsCreated AS BOOLEAN)
# MAGIC     --     THEN 'CREATED'
# MAGIC     --   ELSE 'UNKNOWN'
# MAGIC     -- END                                               AS order_status,
# MAGIC
# MAGIC     -- Derived order status
# MAGIC CASE
# MAGIC     WHEN OrderIsClosed = 'X'
# MAGIC         THEN 'CLOSED'
# MAGIC     WHEN OrderIsTechnicallyCompleted = 'X'
# MAGIC         THEN 'TECO'
# MAGIC     WHEN OrderIsConfirmed = 'X'
# MAGIC         THEN 'CONFIRMED'
# MAGIC     WHEN OrderIsDelivered = 'X'
# MAGIC         THEN 'DELIVERED'
# MAGIC     WHEN OrderIsPartiallyDelivered = 'X'
# MAGIC         THEN 'PARTIALLY DELIVERED'
# MAGIC     WHEN OrderIsReleased = 'X'
# MAGIC         THEN 'RELEASED'
# MAGIC     WHEN OrderIsCreated = 'X'
# MAGIC         THEN 'CREATED'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC END AS order_status,
# MAGIC
# MAGIC     -- References
# MAGIC     SalesOrder                                        AS sales_order,
# MAGIC     SalesOrderItem                                    AS sales_order_item,
# MAGIC     PlannedOrder                                      AS planned_order,
# MAGIC
# MAGIC     -- Planned dates
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderPlannedStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS planned_start_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderPlannedEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS planned_end_date,
# MAGIC
# MAGIC     -- Scheduled dates
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderScheduledStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS scheduled_start_date,
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderScheduledEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS scheduled_end_date,
# MAGIC
# MAGIC     -- Actual release date
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderActualReleaseDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS actual_release_date,
# MAGIC
# MAGIC     -- Creation date
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MfgOrderCreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )                                                 AS creation_date
# MAGIC
# MAGIC   FROM udw_procurement.bronze.sap_production_order_header
# MAGIC   WHERE ManufacturingOrder IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY ManufacturingOrder ORDER BY ManufacturingOrder
# MAGIC   ) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   h.*,
# MAGIC
# MAGIC   -- Schedule adherence: planned end vs scheduled end
# MAGIC   DATEDIFF(h.scheduled_end_date, h.planned_end_date) AS schedule_vs_plan_variance,
# MAGIC
# MAGIC   -- Days until planned end from today
# MAGIC   DATEDIFF(h.planned_end_date, CURRENT_DATE())        AS days_until_planned_end,
# MAGIC
# MAGIC   -- Is overdue (planned end passed and not completed)
# MAGIC   CASE
# MAGIC     WHEN h.planned_end_date < CURRENT_DATE()
# MAGIC       AND NOT COALESCE(h.status_teco, FALSE)
# MAGIC       AND NOT COALESCE(h.status_closed, FALSE)
# MAGIC     THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                                 AS is_overdue,
# MAGIC
# MAGIC   -- Planned duration
# MAGIC   DATEDIFF(h.planned_end_date, h.planned_start_date)  AS planned_duration_days,
# MAGIC
# MAGIC   -- Fiscal period
# MAGIC   YEAR(h.creation_date)                               AS creation_year,
# MAGIC   MONTH(h.creation_date)                              AS creation_month,
# MAGIC
# MAGIC   CURRENT_DATE()                                      AS ingestion_date,
# MAGIC   'SAP_S4H'                                           AS source_system
# MAGIC
# MAGIC FROM prod_header h
# MAGIC WHERE COALESCE(status_deleted, FALSE) IS DISTINCT FROM TRUE
# MAGIC   AND COALESCE(status_marked_deletion, FALSE) IS DISTINCT FROM TRUE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Validate Silver Production Orders
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   order_status,
# MAGIC   plant,
# MAGIC   COUNT(*)                              AS order_count,
# MAGIC   COUNT(DISTINCT material_number)       AS distinct_materials,
# MAGIC   ROUND(SUM(total_quantity), 0)         AS total_planned_qty,
# MAGIC   ROUND(SUM(confirmed_yield_qty), 0)    AS total_confirmed_qty,
# MAGIC   ROUND(AVG(completion_pct), 1)         AS avg_completion_pct,
# MAGIC   SUM(CASE WHEN is_overdue
# MAGIC       THEN 1 ELSE 0 END)                AS overdue_orders,
# MAGIC   ROUND(AVG(planned_duration_days), 1)  AS avg_planned_duration_days
# MAGIC FROM udw_procurement.silver.production_orders
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY order_count DESC;