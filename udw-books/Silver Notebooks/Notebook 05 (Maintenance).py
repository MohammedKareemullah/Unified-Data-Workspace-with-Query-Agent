# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Preview actual data values
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC SELECT
# MAGIC   MaintenanceNotification,
# MAGIC   NotificationType,
# MAGIC   TechnicalObject,
# MAGIC   TechnicalObjectDescription,
# MAGIC   MaintenancePlant,
# MAGIC   MaintPriority,
# MAGIC   MaintPriorityDesc,
# MAGIC   NotificationText,
# MAGIC   MaintNotificationCauseCode,
# MAGIC   MaintNotificationCauseCodeName,
# MAGIC   MaintenanceObjectIsDown,
# MAGIC   MalfunctionStartDate,
# MAGIC   MalfunctionEndDate,
# MAGIC   MaintObjectDowntimeDuration,
# MAGIC   MaintObjDowntimeDurationUnit,
# MAGIC   NotificationCreationDate,
# MAGIC   NotificationCompletionDate,
# MAGIC   MaintenanceOrder
# MAGIC FROM udw_procurement.bronze.sap_maintenance_notifications
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MaintenanceOrder,
# MAGIC   MaintenanceOrderType,
# MAGIC   MaintenanceOrderDesc,
# MAGIC   MaintenancePlant,
# MAGIC   TechnicalObject,
# MAGIC   MaintPriority,
# MAGIC   FunctionalLocation,
# MAGIC   Equipment,
# MAGIC   CostCenter,
# MAGIC   Material,
# MAGIC   MaintenanceNotification,
# MAGIC   ScheduledBasicStartDate,
# MAGIC   ScheduledBasicEndDate,
# MAGIC   MaintOrdBasicStartDate,
# MAGIC   MaintOrdBasicEndDate,
# MAGIC   SystemStatusText,
# MAGIC   UserStatusText,
# MAGIC   MaintenanceOrderComponent,
# MAGIC   MaintOrdOpCompRequiredQuantity,
# MAGIC   BaseUnit,
# MAGIC   Supplier
# MAGIC FROM udw_procurement.bronze.sap_maintenance_orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   PlannedOrder,
# MAGIC   PlannedOrderType,
# MAGIC   Material,
# MAGIC   MaterialName,
# MAGIC   Plant,
# MAGIC   ProductionPlant,
# MAGIC   TotalQuantity,
# MAGIC   RequiredQuantity,
# MAGIC   BaseUnit,
# MAGIC   PlndOrderPlannedStartDate,
# MAGIC   PlndOrderPlannedEndDate,
# MAGIC   ProductionStartDate,
# MAGIC   ProductionEndDate,
# MAGIC   PlannedOrderIsFirm,
# MAGIC   PlannedOrderIsConvertible,
# MAGIC   MRPController,
# MAGIC   MRPArea,
# MAGIC   FixedSupplier,
# MAGIC   PurchasingGroup
# MAGIC FROM udw_procurement.bronze.sap_planned_orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Silver Maintenance Notifications
# MAGIC -- Equipment downtime and failure tracking
# MAGIC -- Links to maintenance orders for cost context
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.maintenance_notifications
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/maintenance_notifications/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   MaintenanceNotification                           AS notification_number,
# MAGIC   NotificationType                                  AS notification_type,
# MAGIC   MaintenanceOrderType                              AS order_type,
# MAGIC   MaintenanceOrder                                  AS linked_order,
# MAGIC   MaintenancePlan                                   AS maintenance_plan,
# MAGIC   MaintenanceItem                                   AS maintenance_item,
# MAGIC
# MAGIC   -- Technical object (equipment/functional location)
# MAGIC   TechnicalObject                                   AS technical_object,
# MAGIC   TechnicalObjectLabel                              AS technical_object_label,
# MAGIC   TechnicalObjectDescription                        AS technical_object_desc,
# MAGIC   TechnicalObjectCategory                           AS technical_object_category,
# MAGIC   TechObjIsEquipOrFuncnlLoc                         AS object_type,
# MAGIC   FunctionalLocation                                AS functional_location,
# MAGIC   FunctionalLocationLabelName                       AS functional_location_name,
# MAGIC
# MAGIC   -- Plant and organizational data
# MAGIC   MaintenancePlant                                  AS plant,
# MAGIC   MaintenancePlantName                              AS plant_name,
# MAGIC   MaintenancePlanningPlant                          AS planning_plant,
# MAGIC   PlantSection                                      AS plant_section,
# MAGIC   MainWorkCenter                                    AS work_center,
# MAGIC   MainWorkCenterPlant                               AS work_center_plant,
# MAGIC   MaintenancePlannerGroup                           AS planner_group,
# MAGIC
# MAGIC   -- Priority and classification
# MAGIC   MaintPriority                                     AS priority_code,
# MAGIC   MaintPriorityDesc                                 AS priority_description,
# MAGIC   MaintenanceActivityType                           AS activity_type,
# MAGIC
# MAGIC   -- Notification content
# MAGIC   NotificationText                                  AS notification_text,
# MAGIC   MaintNotificationCauseCode                        AS cause_code,
# MAGIC   MaintNotificationCauseCodeName                    AS cause_name,
# MAGIC   MaintNotificationRootCause                        AS root_cause,
# MAGIC   MaintNotificationRootCauseText                    AS root_cause_text,
# MAGIC   MaintNotificationDamageCode                       AS damage_code,
# MAGIC   MaintNotifDamageCodeName                          AS damage_name,
# MAGIC   MalfunctionEffect                                 AS malfunction_effect,
# MAGIC   MalfunctionEffectText                             AS malfunction_effect_text,
# MAGIC
# MAGIC   -- Downtime
# MAGIC   CAST(MaintenanceObjectIsDown AS BOOLEAN)          AS is_object_down,
# MAGIC   CAST(MaintObjectDowntimeDuration AS DOUBLE)       AS downtime_duration,
# MAGIC   MaintObjDowntimeDurationUnit                      AS downtime_unit,
# MAGIC
# MAGIC   -- Status
# MAGIC   NotifProcessingPhase                              AS processing_phase,
# MAGIC   NotifProcessingPhaseDesc                          AS processing_phase_desc,
# MAGIC   CAST(IsDeleted AS BOOLEAN)                        AS is_deleted,
# MAGIC   PersonResponsible                                 AS person_responsible,
# MAGIC   PersonResponsibleName                             AS responsible_person_name,
# MAGIC   ReportedByUser                                    AS reported_by,
# MAGIC   ReporterFullName                                  AS reporter_name,
# MAGIC
# MAGIC   -- Dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(NotificationCreationDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS creation_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(NotificationCompletionDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS completion_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MalfunctionStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS malfunction_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MalfunctionEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS malfunction_end_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(RequiredStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS required_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(RequiredEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS required_end_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(LatestAcceptableCompletionDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS latest_completion_date,
# MAGIC
# MAGIC   -- Time to complete
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(NotificationCompletionDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(NotificationCreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS resolution_days,
# MAGIC
# MAGIC   -- Notification completed flag
# MAGIC   CASE
# MAGIC     WHEN NotificationCompletionDate IS NOT NULL
# MAGIC       AND regexp_extract(NotificationCompletionDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) != ''
# MAGIC     THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END                                               AS is_completed,
# MAGIC
# MAGIC   -- Priority classification for analytics
# MAGIC   CASE MaintPriority
# MAGIC     WHEN '1' THEN 'VERY HIGH'
# MAGIC     WHEN '2' THEN 'HIGH'
# MAGIC     WHEN '3' THEN 'MEDIUM'
# MAGIC     WHEN '4' THEN 'LOW'
# MAGIC     ELSE COALESCE(MaintPriorityDesc, 'UNCLASSIFIED')
# MAGIC   END                                               AS priority_level,
# MAGIC
# MAGIC   -- Fiscal period
# MAGIC   YEAR(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(NotificationCreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS creation_year,
# MAGIC   MONTH(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(NotificationCreationDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS creation_month,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_maintenance_notifications
# MAGIC WHERE MaintenanceNotification IS NOT NULL
# MAGIC   AND CAST(IsDeleted AS BOOLEAN) IS DISTINCT FROM TRUE
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY MaintenanceNotification
# MAGIC   ORDER BY MaintenanceNotification
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Silver Maintenance Orders
# MAGIC -- Work orders with cost, material, and operation details
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.maintenance_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/maintenance_orders/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   MaintenanceOrder                                  AS order_number,
# MAGIC   MaintenanceOrderType                              AS order_type,
# MAGIC   MaintenanceOrderDesc                              AS order_description,
# MAGIC   MaintenanceNotification                           AS notification_number,
# MAGIC   MaintenancePlan                                   AS maintenance_plan,
# MAGIC   MaintenanceItem                                   AS maintenance_item,
# MAGIC
# MAGIC   -- Technical object
# MAGIC   TechnicalObject                                   AS technical_object,
# MAGIC   TechnicalObjectLabel                              AS technical_object_label,
# MAGIC   FunctionalLocation                                AS functional_location,
# MAGIC   Equipment                                         AS equipment,
# MAGIC   EquipmentName                                     AS equipment_name,
# MAGIC
# MAGIC   -- Plant and org
# MAGIC   MaintenancePlant                                  AS plant,
# MAGIC   MaintenancePlanningPlant                          AS planning_plant,
# MAGIC   PlantSection                                      AS plant_section,
# MAGIC   CompanyCode                                       AS company_code,
# MAGIC   CostCenter                                        AS cost_center,
# MAGIC   WBSElement                                        AS wbs_element,
# MAGIC   ProfitCenter                                      AS profit_center,
# MAGIC   ControllingArea                                   AS controlling_area,
# MAGIC
# MAGIC   -- Work center and execution
# MAGIC   MainWorkCenter                                    AS work_center,
# MAGIC   MainWorkCenterPlant                               AS work_center_plant,
# MAGIC   WorkCenter                                        AS operation_work_center,
# MAGIC   MaintenancePlannerGroup                           AS planner_group,
# MAGIC   MaintenanceActivityType                           AS activity_type,
# MAGIC   MaintPriority                                     AS priority_code,
# MAGIC
# MAGIC   -- Material and components
# MAGIC   Material                                          AS material_number,
# MAGIC   MaterialGroup                                     AS material_group,
# MAGIC   MaintenanceOrderComponent                         AS component,
# MAGIC   CAST(MaintOrdOpCompRequiredQuantity AS DOUBLE)    AS required_qty,
# MAGIC   BaseUnit                                          AS base_unit,
# MAGIC   Supplier                                          AS vendor_id,
# MAGIC   PurchasingGroup                                   AS purchasing_group,
# MAGIC   PurchasingOrganization                            AS purchasing_org,
# MAGIC   PurchasingInfoRecord                              AS info_record,
# MAGIC   PurchaseRequisition                               AS purchase_requisition,
# MAGIC
# MAGIC   -- Operation details
# MAGIC   MaintenanceOrderOperation                         AS operation,
# MAGIC   OperationDescription                              AS operation_description,
# MAGIC   CAST(MaintOrdOperationWorkDuration AS DOUBLE)     AS operation_work_duration,
# MAGIC   MaintOrdOperationDurationUnit                     AS duration_unit,
# MAGIC   CAST(MaintOrderOperationQuantity AS DOUBLE)       AS operation_quantity,
# MAGIC   CAST(ActualWorkQuantity AS DOUBLE)                AS actual_work_quantity,
# MAGIC   CAST(ForecastWorkQuantity AS DOUBLE)              AS forecast_work_quantity,
# MAGIC   ActivityType                                      AS activity_type_code,
# MAGIC
# MAGIC   -- Costs
# MAGIC   Currency                                          AS currency,
# MAGIC   CAST(SettlementAmount AS DOUBLE)                  AS settlement_amount,
# MAGIC   CAST(MaintOrderOpComponentPrice AS DOUBLE)          AS component_price,
# MAGIC   
# MAGIC   CAST(ExpectedOverallLimitAmount AS DOUBLE)        AS expected_limit_amount,
# MAGIC   CAST(OverallLimitAmount AS DOUBLE)                AS overall_limit_amount,
# MAGIC   GLAccount                                         AS gl_account,
# MAGIC
# MAGIC   -- Status
# MAGIC   SystemStatusText                                  AS system_status,
# MAGIC   UserStatusText                                    AS user_status,
# MAGIC   CAST(IsMarkedForDeletion AS BOOLEAN)              AS is_marked_for_deletion,
# MAGIC   CAST(GoodsMovementIsAllowed AS BOOLEAN)           AS goods_movement_allowed,
# MAGIC   CAST(ReservationIsFinallyIssued AS BOOLEAN)       AS reservation_final_issued,
# MAGIC
# MAGIC   -- Derived status
# MAGIC   CASE
# MAGIC     WHEN CAST(IsMarkedForDeletion AS BOOLEAN) = TRUE THEN 'DELETED'
# MAGIC     WHEN SystemStatusText LIKE '%TECO%'              THEN 'COMPLETED'
# MAGIC     WHEN SystemStatusText LIKE '%REL%'               THEN 'RELEASED'
# MAGIC     WHEN SystemStatusText LIKE '%CRTD%'              THEN 'CREATED'
# MAGIC     ELSE COALESCE(SystemStatusText, 'UNKNOWN')
# MAGIC   END                                               AS order_status,
# MAGIC
# MAGIC   -- Schedule dates
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
# MAGIC       CAST(regexp_extract(LatestAcceptableCompletionDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC
# MAGIC     -- /Date(1774742400000)/
# MAGIC   )                                                 AS latest_completion_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MaintOrderCreationDateTime,
# MAGIC         '/Date\\((\\d+)\\+0000\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS creation_date,
# MAGIC
# MAGIC --   /Date(1780561766000+0000)/
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
# MAGIC
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
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_maintenance_orders
# MAGIC WHERE MaintenanceOrder IS NOT NULL
# MAGIC   AND CAST(IsMarkedForDeletion AS BOOLEAN) IS DISTINCT FROM TRUE
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY MaintenanceOrder, MaintenanceOrderOperation,
# MAGIC                MaintenanceOrderComponent
# MAGIC   ORDER BY MaintenanceOrder
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 4 — Silver Planned Orders
# MAGIC -- MRP-generated demand for future procurement/production
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.planned_orders
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/planned_orders/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   PlannedOrder                                      AS planned_order,
# MAGIC   PlannedOrderType                                  AS order_type,
# MAGIC   PlannedOrderProfile                               AS order_profile,
# MAGIC
# MAGIC   -- Material and location
# MAGIC   Material                                          AS material_number,
# MAGIC   MaterialName                                      AS material_name,
# MAGIC   Plant                                             AS plant,
# MAGIC   ProductionPlant                                   AS production_plant,
# MAGIC   StorageLocation                                   AS storage_location,
# MAGIC   MRPArea                                           AS mrp_area,
# MAGIC   MRPController                                     AS mrp_controller,
# MAGIC   MRPPlant                                          AS mrp_plant,
# MAGIC   ProductionVersion                                 AS production_version,
# MAGIC   ProductionSupervisor                              AS production_supervisor,
# MAGIC   SupplyArea                                        AS supply_area,
# MAGIC
# MAGIC   -- Quantities
# MAGIC   CAST(TotalQuantity AS DOUBLE)                     AS total_quantity,
# MAGIC   CAST(RequiredQuantity AS DOUBLE)                  AS required_quantity,
# MAGIC   CAST(GoodsReceiptQty AS DOUBLE)                   AS goods_receipt_qty,
# MAGIC   CAST(IssuedQuantity AS DOUBLE)                    AS issued_quantity,
# MAGIC   CAST(WithdrawnQuantity AS DOUBLE)                 AS withdrawn_quantity,
# MAGIC   CAST(PlndOrderPlannedScrapQty AS DOUBLE)          AS planned_scrap_qty,
# MAGIC   BaseUnit                                          AS base_unit,
# MAGIC
# MAGIC   -- Open quantity
# MAGIC   CAST(TotalQuantity AS DOUBLE) -
# MAGIC   COALESCE(CAST(GoodsReceiptQty AS DOUBLE), 0)      AS open_quantity,
# MAGIC
# MAGIC   -- Component data
# MAGIC   BOMItem                                           AS bom_item,
# MAGIC   BOMItemDescription                                AS bom_item_desc,
# MAGIC   CAST(ComponentScrapInPercent AS DOUBLE)           AS component_scrap_pct,
# MAGIC   MaterialProcurementCategory                       AS procurement_category,
# MAGIC   MaterialProcurementType                           AS procurement_type,
# MAGIC
# MAGIC   -- Supplier
# MAGIC   FixedSupplier                                     AS fixed_supplier,
# MAGIC   SupplierName                                      AS supplier_name,
# MAGIC   PurchasingGroup                                   AS purchasing_group,
# MAGIC   PurchasingOrganization                            AS purchasing_org,
# MAGIC   PurchasingDocument                                AS purchasing_document,
# MAGIC
# MAGIC   -- Status flags
# MAGIC   CAST(PlannedOrderIsFirm AS BOOLEAN)               AS is_firm,
# MAGIC   CAST(PlannedOrderIsConvertible AS BOOLEAN)        AS is_convertible,
# MAGIC   CAST(PlannedOrderBOMIsFixed AS BOOLEAN)           AS bom_is_fixed,
# MAGIC   CAST(PlannedOrderCapacityIsDsptchd AS BOOLEAN)    AS capacity_dispatched,
# MAGIC
# MAGIC   -- Planning classification
# MAGIC   CASE
# MAGIC     WHEN CAST(PlannedOrderIsFirm AS BOOLEAN) = TRUE
# MAGIC     THEN 'FIRM'
# MAGIC     WHEN CAST(PlannedOrderIsConvertible AS BOOLEAN) = TRUE
# MAGIC     THEN 'CONVERTIBLE'
# MAGIC     ELSE 'PLANNED'
# MAGIC   END                                               AS planning_status,
# MAGIC
# MAGIC   -- Procurement type description
# MAGIC   CASE MaterialProcurementCategory
# MAGIC     WHEN 'E' THEN 'In-house Production'
# MAGIC     WHEN 'F' THEN 'External Procurement'
# MAGIC     WHEN 'X' THEN 'Both'
# MAGIC     ELSE COALESCE(MaterialProcurementCategory, 'Unknown')
# MAGIC   END                                               AS procurement_category_desc,
# MAGIC
# MAGIC   -- References
# MAGIC   SalesOrder                                        AS sales_order,
# MAGIC   SalesOrderItem                                    AS sales_order_item,
# MAGIC   WBSElement                                        AS wbs_element,
# MAGIC   Reservation                                       AS reservation,
# MAGIC   ReservationItem                                   AS reservation_item,
# MAGIC
# MAGIC   -- Scheduling
# MAGIC   SchedulingType                                    AS scheduling_type,
# MAGIC   WorkCenter                                        AS work_center,
# MAGIC   Operation                                         AS operation,
# MAGIC   OperationText                                     AS operation_text,
# MAGIC
# MAGIC   -- Dates
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(PlndOrderPlannedStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS planned_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(PlndOrderPlannedEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS planned_end_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(ProductionStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS production_start_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(ProductionEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS production_end_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(PlannedOrderOpeningDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS opening_date,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(MatlCompRequirementDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS component_requirement_date,
# MAGIC
# MAGIC   -- Duration
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PlndOrderPlannedEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PlndOrderPlannedStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS planned_duration_days,
# MAGIC
# MAGIC   -- Urgency — how soon is the planned start?
# MAGIC   DATEDIFF(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PlndOrderPlannedStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ),
# MAGIC     CURRENT_DATE()
# MAGIC   )                                                 AS days_until_start,
# MAGIC
# MAGIC   -- Fiscal period
# MAGIC   YEAR(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PlndOrderPlannedStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS planned_year,
# MAGIC   MONTH(
# MAGIC     CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(PlndOrderPlannedStartDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     )
# MAGIC   )                                                 AS planned_month,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_planned_orders
# MAGIC WHERE PlannedOrder IS NOT NULL
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY PlannedOrder, BOMItem
# MAGIC   ORDER BY PlannedOrder
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 5 — Silver Purchasing Info Records
# MAGIC -- Vendor-material-price relationships
# MAGIC -- The source of truth for standard purchase prices
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.silver.info_records
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/silver/info_records/'
# MAGIC AS
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   PurchasingInfoRecord                              AS info_record,
# MAGIC   PurchasingInfoRecordDesc                          AS description,
# MAGIC
# MAGIC   -- Material and vendor
# MAGIC   Material                                          AS material_number,
# MAGIC   MaterialGroup                                     AS material_group,
# MAGIC   Supplier                                          AS vendor_id,
# MAGIC   SupplierMaterialNumber                            AS supplier_material_number,
# MAGIC   SupplierSubrange                                  AS supplier_subrange,
# MAGIC   Manufacturer                                      AS manufacturer,
# MAGIC
# MAGIC   -- Units
# MAGIC   BaseUnit                                          AS base_unit,
# MAGIC   PurgDocOrderQuantityUnit                          AS po_order_unit,
# MAGIC
# MAGIC   -- Quantity conversion
# MAGIC   CAST(OrderItemQtyToBaseQtyNmrtr AS DOUBLE)        AS qty_numerator,
# MAGIC   CAST(OrderItemQtyToBaseQtyDnmntr AS DOUBLE)       AS qty_denominator,
# MAGIC
# MAGIC   -- Supplier contact
# MAGIC   SupplierPhoneNumber                               AS supplier_phone,
# MAGIC   SupplierRespSalesPersonName                       AS sales_person,
# MAGIC
# MAGIC   -- Regular supplier flag
# MAGIC   CAST(IsRegularSupplier AS BOOLEAN)                AS is_regular_supplier,
# MAGIC   CAST(IsDeleted AS BOOLEAN)                        AS is_deleted,
# MAGIC
# MAGIC   -- Reminders
# MAGIC   CAST(NoDaysReminder1 AS INT)                      AS reminder_days_1,
# MAGIC   CAST(NoDaysReminder2 AS INT)                      AS reminder_days_2,
# MAGIC   CAST(NoDaysReminder3 AS INT)                      AS reminder_days_3,
# MAGIC
# MAGIC   -- Validity
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(AvailabilityStartDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS availability_start,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(AvailabilityEndDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS availability_end,
# MAGIC   CAST(
# MAGIC     from_unixtime(
# MAGIC       CAST(regexp_extract(CreationDate,
# MAGIC         '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC     ) AS DATE
# MAGIC   )                                                 AS creation_date,
# MAGIC
# MAGIC   -- Is currently valid?
# MAGIC   CASE
# MAGIC     WHEN CAST(IsDeleted AS BOOLEAN) = TRUE THEN FALSE
# MAGIC     WHEN CAST(
# MAGIC       from_unixtime(
# MAGIC         CAST(regexp_extract(AvailabilityEndDate,
# MAGIC           '/Date\\((\\d+)\\)/', 1) AS BIGINT) / 1000
# MAGIC       ) AS DATE
# MAGIC     ) < CURRENT_DATE() THEN FALSE
# MAGIC     ELSE TRUE
# MAGIC   END                                               AS is_active,
# MAGIC
# MAGIC   CURRENT_DATE()                                    AS ingestion_date,
# MAGIC   'SAP_S4H'                                         AS source_system
# MAGIC
# MAGIC FROM udw_procurement.bronze.sap_info_records
# MAGIC WHERE PurchasingInfoRecord IS NOT NULL
# MAGIC   AND CAST(IsDeleted AS BOOLEAN) IS DISTINCT FROM TRUE
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY PurchasingInfoRecord
# MAGIC   ORDER BY PurchasingInfoRecord
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 6 — Validate all Silver tables in this notebook
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   'maintenance_notifications'   AS table_name,
# MAGIC   COUNT(*)                      AS total_records,
# MAGIC   COUNT(DISTINCT technical_object) AS technical_objects,
# MAGIC   COUNT(DISTINCT plant)         AS plants,
# MAGIC   SUM(CASE WHEN is_completed
# MAGIC       THEN 1 ELSE 0 END)        AS completed,
# MAGIC   SUM(CASE WHEN is_object_down
# MAGIC       THEN 1 ELSE 0 END)        AS downtime_events,
# MAGIC   ROUND(AVG(downtime_duration),2) AS avg_downtime,
# MAGIC   ROUND(AVG(resolution_days),1) AS avg_resolution_days
# MAGIC FROM udw_procurement.silver.maintenance_notifications
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'maintenance_orders',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT technical_object),
# MAGIC   COUNT(DISTINCT plant),
# MAGIC   SUM(CASE WHEN order_status = 'COMPLETED'
# MAGIC       THEN 1 ELSE 0 END),
# MAGIC   COUNT(DISTINCT vendor_id),
# MAGIC   ROUND(AVG(planned_duration_days),1),
# MAGIC   ROUND(AVG(CAST(required_qty AS DOUBLE)),2)
# MAGIC FROM udw_procurement.silver.maintenance_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'planned_orders',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT material_number),
# MAGIC   COUNT(DISTINCT plant),
# MAGIC   SUM(CASE WHEN is_firm THEN 1 ELSE 0 END),
# MAGIC   SUM(CASE WHEN days_until_start <= 7 THEN 1 ELSE 0 END),
# MAGIC   ROUND(AVG(total_quantity),1),
# MAGIC   ROUND(AVG(planned_duration_days),1)
# MAGIC FROM udw_procurement.silver.planned_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'info_records',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT material_number),
# MAGIC   COUNT(DISTINCT vendor_id),
# MAGIC   SUM(CASE WHEN is_regular_supplier THEN 1 ELSE 0 END),
# MAGIC   SUM(CASE WHEN is_active THEN 1 ELSE 0 END),
# MAGIC   COUNT(DISTINCT material_group),
# MAGIC   COUNT(DISTINCT manufacturer)
# MAGIC FROM udw_procurement.silver.info_records;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 7 — Maintenance analytics preview
# MAGIC -- Equipment downtime by plant and priority
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   plant,
# MAGIC   priority_level,
# MAGIC   COUNT(*)                              AS notification_count,
# MAGIC   SUM(CASE WHEN is_object_down
# MAGIC       THEN 1 ELSE 0 END)                AS downtime_events,
# MAGIC   ROUND(SUM(downtime_duration), 1)      AS total_downtime,
# MAGIC   downtime_unit,
# MAGIC   ROUND(AVG(resolution_days), 1)        AS avg_resolution_days,
# MAGIC   SUM(CASE WHEN is_completed
# MAGIC       THEN 1 ELSE 0 END)                AS completed_notifications,
# MAGIC   COUNT(*) - SUM(CASE WHEN is_completed
# MAGIC                  THEN 1 ELSE 0 END)     AS open_notifications
# MAGIC FROM udw_procurement.silver.maintenance_notifications
# MAGIC GROUP BY 1, 2, 6
# MAGIC ORDER BY total_downtime DESC NULLS LAST;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 8 — Planned order urgency — MRP demand horizon
# MAGIC -- Shows what needs to be procured or produced soon
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   plant,
# MAGIC   procurement_category_desc,
# MAGIC   planning_status,
# MAGIC   CASE
# MAGIC     WHEN days_until_start < 0  THEN 'OVERDUE'
# MAGIC     WHEN days_until_start <= 7 THEN 'THIS WEEK'
# MAGIC     WHEN days_until_start <= 30 THEN 'THIS MONTH'
# MAGIC     WHEN days_until_start <= 90 THEN 'THIS QUARTER'
# MAGIC     ELSE 'FUTURE'
# MAGIC   END                                   AS urgency_bucket,
# MAGIC   COUNT(DISTINCT planned_order)         AS planned_orders,
# MAGIC   COUNT(DISTINCT material_number)       AS distinct_materials,
# MAGIC   ROUND(SUM(total_quantity), 0)         AS total_quantity,
# MAGIC   ROUND(SUM(open_quantity), 0)          AS total_open_quantity,
# MAGIC   COUNT(DISTINCT fixed_supplier)        AS assigned_suppliers
# MAGIC FROM udw_procurement.silver.planned_orders
# MAGIC GROUP BY 1, 2, 3, 4
# MAGIC ORDER BY
# MAGIC   CASE urgency_bucket
# MAGIC     WHEN 'OVERDUE'      THEN 1
# MAGIC     WHEN 'THIS WEEK'    THEN 2
# MAGIC     WHEN 'THIS MONTH'   THEN 3
# MAGIC     WHEN 'THIS QUARTER' THEN 4
# MAGIC     ELSE 5
# MAGIC   END,
# MAGIC   total_open_quantity DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 9 — Info record coverage analysis
# MAGIC -- Which vendor-material pairs have established price records
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT
# MAGIC   vendor_id,
# MAGIC   material_group,
# MAGIC   COUNT(DISTINCT info_record)           AS info_records,
# MAGIC   COUNT(DISTINCT material_number)       AS materials_covered,
# MAGIC   SUM(CASE WHEN is_regular_supplier
# MAGIC       THEN 1 ELSE 0 END)                AS regular_supplier_records,
# MAGIC   SUM(CASE WHEN is_active
# MAGIC       THEN 1 ELSE 0 END)                AS active_records,
# MAGIC   MIN(availability_start)               AS earliest_availability,
# MAGIC   MAX(availability_end)                 AS latest_availability,
# MAGIC   MAX(creation_date)                    AS latest_creation
# MAGIC FROM udw_procurement.silver.info_records
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY info_records DESC
# MAGIC LIMIT 20;