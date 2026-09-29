# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE udw_procurement.bronze.sap_maint_order_header;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT MaintOrderCreationDateTime from udw_procurement.bronze.sap_maint_order_header;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Silver Maintenance Orders
# MAGIC -- Using confirmed field names from Cell 8 preview
# MAGIC -- Key fields: MaintenanceOrder, MaintenanceOrderType,
# MAGIC -- TechnicalObject, MaintenancePlant, SystemStatusText,
# MAGIC -- MaintOrdBasicStartDate, MaintOrdBasicEndDate
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.maintenance_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/maintenance_orders/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   MaintenanceOrder                                  AS order_number,
# MAGIC   MaintenanceOrderType                              AS order_type,
# MAGIC   MaintenanceOrderDesc                              AS order_description,
# MAGIC   MaintenanceNotification                           AS notification_number,
# MAGIC   MaintenancePlan                                   AS maintenance_plan,
# MAGIC   MaintenanceItem                                   AS maintenance_item,
# MAGIC   -- Technical object
# MAGIC   TechnicalObject                                   AS technical_object,
# MAGIC   TechnicalObjectLabel                              AS technical_object_label,
# MAGIC   FunctionalLocation                                AS functional_location,
# MAGIC   Equipment                                         AS equipment,
# MAGIC   EquipmentName                                     AS equipment_name,
# MAGIC   -- Plant and org
# MAGIC   MaintenancePlant                                  AS plant,
# MAGIC   MaintenancePlanningPlant                          AS planning_plant,
# MAGIC   PlantSection                                      AS plant_section,
# MAGIC   CompanyCode                                       AS company_code,
# MAGIC   CostCenter                                        AS cost_center,
# MAGIC   WBSElement                                        AS wbs_element,
# MAGIC   ProfitCenter                                      AS profit_center,
# MAGIC   ControllingArea                                   AS controlling_area,
# MAGIC   -- Work center
# MAGIC   MainWorkCenter                                    AS work_center,
# MAGIC   MainWorkCenterPlant                               AS work_center_plant,
# MAGIC   MaintenancePlannerGroup                           AS planner_group,
# MAGIC   MaintenanceActivityType                           AS activity_type,
# MAGIC   MaintPriority                                     AS priority_code,
# MAGIC   -- Status
# MAGIC   SystemStatusText                                  AS system_status,
# MAGIC   UserStatusText                                    AS user_status,
# MAGIC   -- Derived status from system status text
# MAGIC   CASE
# MAGIC     WHEN SystemStatusText LIKE '%CLSD%' THEN 'CLOSED'
# MAGIC     WHEN SystemStatusText LIKE '%TECO%' THEN 'TECO'
# MAGIC     WHEN SystemStatusText LIKE '%CNF%'  THEN 'CONFIRMED'
# MAGIC     WHEN SystemStatusText LIKE '%REL%'  THEN 'RELEASED'
# MAGIC     WHEN SystemStatusText LIKE '%CRTD%' THEN 'CREATED'
# MAGIC     ELSE COALESCE(SystemStatusText, 'UNKNOWN')
# MAGIC   END                                               AS order_status,
# MAGIC   -- Schedule dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MaintOrdBasicStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS basic_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MaintOrdBasicEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS basic_end_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(ScheduledBasicStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS scheduled_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(ScheduledBasicEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS scheduled_end_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(LatestAcceptableCompletionDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS latest_completion_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MaintOrderCreationDateTime,
# MAGIC         '/Date\\((\\d+)\\+0000\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS creation_date,
# MAGIC   
# MAGIC   -- Planned duration
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(ScheduledBasicEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(ScheduledBasicStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS planned_duration_days,
# MAGIC   -- Fiscal period
# MAGIC   YEAR(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MaintOrderCreationDateTime,
# MAGIC           '/Date\\((\\d+)\\+0000\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS creation_year,
# MAGIC   MONTH(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(MaintOrderCreationDateTime,
# MAGIC           '/Date\\((\\d+)\\+0000\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS creation_month,
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC FROM udw_procurement.bronze.sap_maint_order_header
# MAGIC WHERE MaintenanceOrder IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY MaintenanceOrder ORDER BY MaintenanceOrder
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Validate Silver Maintenance Orders
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   order_status,
# MAGIC   plant,
# MAGIC   COUNT(*)                              AS order_count,
# MAGIC   COUNT(DISTINCT equipment)             AS distinct_equipment,
# MAGIC   ROUND(AVG(planned_duration_days), 1)  AS avg_planned_duration,
# MAGIC   COUNT(DISTINCT creation_year)         AS years_covered
# MAGIC FROM udw_procurement.silver.maintenance_orders
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY order_count DESC;