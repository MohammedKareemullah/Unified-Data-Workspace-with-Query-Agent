# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Fix Bronze tables to point at specific entity sets
# MAGIC -- Drop the broad parent-folder tables and recreate them
# MAGIC -- pointing at the correct specific entity subfolders
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC -- ── MATERIAL STOCK — fix to use A_MatlStkInAcctMod ──────────
# MAGIC -- This has actual stock quantities, A_MaterialStock only has
# MAGIC -- Material and BaseUnit
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_material_stock_detail;
# MAGIC
# MAGIC create table if not exists udw_procurement.bronze.sap_material_stock_detaila
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/material_stock/A_MatlStkInAcctMod/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── PRODUCTION ORDER — point at specific entity sets ─────────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_production_order_header;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_production_order_items;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_production_order_operations;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_production_order_status;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_production_order_components;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_production_orders;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_material_documents;
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_production_order_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/production_orders/A_ProductionOrder_2/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_production_order_items
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/production_orders/A_ProductionOrderItem_2/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_production_order_operations
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/production_orders/A_ProductionOrderOperation_2/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_production_order_status
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/production_orders/A_ProductionOrderStatus_2/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_production_order_components
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/production_orders/A_ProductionOrderComponent_2/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── SALES ORDERS — point at specific entity sets ─────────────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_sales_order_header;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_sales_orders;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_sales_order_items;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_sales_order_schedule_lines;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_sales_order_partners;
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_sales_order_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/sales_orders/A_SalesOrder/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_sales_order_items
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/sales_orders/A_SalesOrderItem/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_sales_order_schedule_lines
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/sales_orders/A_SalesOrderScheduleLine/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_sales_order_partners
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/sales_orders/A_SalesOrderHeaderPartner/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── MATERIAL DOCUMENTS — point at specific entity sets ───────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_material_doc_header;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_material_doc_items;
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_material_doc_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/material_documents/A_MaterialDocumentHeader/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_material_doc_items
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/material_documents/A_MaterialDocumentItem/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── INSPECTION LOTS — point at specific entity sets ──────────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_inspection_lot_header;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_inspection_lot_status;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_inspection_characteristics;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_inspection_results;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_inspection_usage_decision;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_inspection_lots;
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_inspection_lot_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/inspection_lots/A_InspectionLot/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_inspection_lot_status
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/inspection_lots/A_InspectionLotWithStatus/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_inspection_characteristics
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/inspection_lots/A_InspectionCharacteristic/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_inspection_results
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/inspection_lots/A_InspectionResult/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_inspection_usage_decision
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/inspection_lots/A_InspLotUsageDecision/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── MAINTENANCE NOTIFICATIONS — specific entity ───────────────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_maint_notification_header;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_maintenance_notifications;
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_maint_notification_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/maintenance_notif/MaintenanceNotification/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── MAINTENANCE ORDERS — specific entity sets ─────────────────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_maint_order_header;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_maint_order_operations;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_maint_order_components;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_maintenance_orders;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_maint_order_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/maintenance_order/MaintenanceOrder/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_maint_order_operations
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/maintenance_order/MaintenanceOrderOperation/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_maint_order_components
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/maintenance_order/MaintOrderOpComponent/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── PLANNED ORDERS — specific entity sets ────────────────────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_planned_order_header;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_planned_order_components;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_planned_orders;
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_planned_order_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/planned_orders/A_PlannedOrder/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_planned_order_components
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/planned_orders/A_PlannedOrderComponent/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── INFO RECORD PRICING — additional entity sets ──────────────
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_info_record_pricing;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_info_record_org_plant;
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_info_records;
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_info_record_pricing
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/info_records/A_PurInfoRecdPrcgCndn/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE if not EXISTS udw_procurement.bronze.sap_info_record_org_plant
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/info_records/A_PurgInfoRecdOrgPlantData/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_process_orders;
# MAGIC
# MAGIC
# MAGIC -- ── PROCESS ORDERS — new, not covered before ─────────────────
# MAGIC CREATE TABLE IF NOT EXISTS udw_procurement.bronze.sap_process_order_header
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/process_orders/A_ProcessOrder/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS udw_procurement.bronze.sap_process_order_components
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/process_orders/A_ProcessOrderComponent/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS udw_procurement.bronze.sap_process_order_operations
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/process_orders/A_ProcessOrderOperation/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC -- ── WAREHOUSE ─────────────────────────────────────────────────
# MAGIC
# MAGIC DROP TABLE IF EXISTS udw_procurement.bronze.sap_warehouse;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS udw_procurement.bronze.sap_warehouse_master
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/warehouse/Warehouse/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS udw_procurement.bronze.sap_warehouse_storage_types
# MAGIC USING JSON OPTIONS (
# MAGIC   path 's3://udw-lake/bronze/sap/warehouse/WarehouseStorageType/',
# MAGIC   recursiveFileLookup 'true');
# MAGIC
# MAGIC SHOW TABLES IN udw_procurement.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Verify record counts for all new Bronze tables
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT 'sap_material_stock_detail'       AS table_name, COUNT(*) AS records FROM udw_procurement.bronze.sap_material_stock_detaila
# MAGIC UNION ALL SELECT 'sap_production_order_header',        COUNT(*) FROM udw_procurement.bronze.sap_production_order_header
# MAGIC UNION ALL SELECT 'sap_production_order_items',         COUNT(*) FROM udw_procurement.bronze.sap_production_order_items
# MAGIC UNION ALL SELECT 'sap_production_order_operations',    COUNT(*) FROM udw_procurement.bronze.sap_production_order_operations
# MAGIC UNION ALL SELECT 'sap_production_order_status',        COUNT(*) FROM udw_procurement.bronze.sap_production_order_status
# MAGIC UNION ALL SELECT 'sap_production_order_components',    COUNT(*) FROM udw_procurement.bronze.sap_production_order_components
# MAGIC UNION ALL SELECT 'sap_sales_order_header',             COUNT(*) FROM udw_procurement.bronze.sap_sales_order_header
# MAGIC UNION ALL SELECT 'sap_sales_order_items',              COUNT(*) FROM udw_procurement.bronze.sap_sales_order_items
# MAGIC UNION ALL SELECT 'sap_sales_order_schedule_lines',     COUNT(*) FROM udw_procurement.bronze.sap_sales_order_schedule_lines
# MAGIC UNION ALL SELECT 'sap_sales_order_partners',           COUNT(*) FROM udw_procurement.bronze.sap_sales_order_partners
# MAGIC UNION ALL SELECT 'sap_material_doc_header',            COUNT(*) FROM udw_procurement.bronze.sap_material_doc_header
# MAGIC UNION ALL SELECT 'sap_material_doc_items',             COUNT(*) FROM udw_procurement.bronze.sap_material_doc_items
# MAGIC UNION ALL SELECT 'sap_inspection_lot_header',          COUNT(*) FROM udw_procurement.bronze.sap_inspection_lot_header
# MAGIC UNION ALL SELECT 'sap_inspection_lot_status',          COUNT(*) FROM udw_procurement.bronze.sap_inspection_lot_status
# MAGIC UNION ALL SELECT 'sap_inspection_characteristics',     COUNT(*) FROM udw_procurement.bronze.sap_inspection_characteristics
# MAGIC UNION ALL SELECT 'sap_inspection_results',             COUNT(*) FROM udw_procurement.bronze.sap_inspection_results
# MAGIC UNION ALL SELECT 'sap_inspection_usage_decision',      COUNT(*) FROM udw_procurement.bronze.sap_inspection_usage_decision
# MAGIC UNION ALL SELECT 'sap_maint_notification_header',      COUNT(*) FROM udw_procurement.bronze.sap_maint_notification_header
# MAGIC UNION ALL SELECT 'sap_maint_order_header',             COUNT(*) FROM udw_procurement.bronze.sap_maint_order_header
# MAGIC UNION ALL SELECT 'sap_maint_order_operations',         COUNT(*) FROM udw_procurement.bronze.sap_maint_order_operations
# MAGIC UNION ALL SELECT 'sap_maint_order_components',         COUNT(*) FROM udw_procurement.bronze.sap_maint_order_components
# MAGIC UNION ALL SELECT 'sap_planned_order_header',           COUNT(*) FROM udw_procurement.bronze.sap_planned_order_header
# MAGIC UNION ALL SELECT 'sap_planned_order_components',       COUNT(*) FROM udw_procurement.bronze.sap_planned_order_components
# MAGIC UNION ALL SELECT 'sap_info_record_pricing',            COUNT(*) FROM udw_procurement.bronze.sap_info_record_pricing
# MAGIC UNION ALL SELECT 'sap_info_record_org_plant',          COUNT(*) FROM udw_procurement.bronze.sap_info_record_org_plant
# MAGIC UNION ALL SELECT 'sap_process_order_header',           COUNT(*) FROM udw_procurement.bronze.sap_process_order_header
# MAGIC UNION ALL SELECT 'sap_process_order_components',       COUNT(*) FROM udw_procurement.bronze.sap_process_order_components
# MAGIC UNION ALL SELECT 'sap_process_order_operations',       COUNT(*) FROM udw_procurement.bronze.sap_process_order_operations
# MAGIC UNION ALL SELECT 'sap_warehouse_master',               COUNT(*) FROM udw_procurement.bronze.sap_warehouse_master
# MAGIC UNION ALL SELECT 'sap_warehouse_storage_types',        COUNT(*) FROM udw_procurement.bronze.sap_warehouse_storage_types
# MAGIC ORDER BY records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 3 — Preview new stock detail table
# MAGIC -- A_MatlStkInAcctMod has actual stock quantities
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_stock_detaila LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 4 — Preview production order header
# MAGIC SELECT * FROM udw_procurement.bronze.sap_production_order_header LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 5 — Preview sales order header
# MAGIC SELECT * FROM udw_procurement.bronze.sap_sales_order_header LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 6 — Preview material document items
# MAGIC -- This is the real transactional goods movement data
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_doc_items LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 7 — Preview inspection lot header
# MAGIC SELECT * FROM udw_procurement.bronze.sap_inspection_lot_header LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 8 — Preview maintenance order header
# MAGIC SELECT * FROM udw_procurement.bronze.sap_maint_order_header LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 9 — Rebuild Silver Inventory using A_MatlStkInAcctMod
# MAGIC -- This replaces the movement-derived inventory we built earlier
# MAGIC -- since we now have actual stock snapshot data
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC -- First check the fields
# MAGIC DESCRIBE udw_procurement.bronze.sap_material_stock_detaila;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 10 — Preview to understand the stock detail structure
# MAGIC SELECT * FROM udw_procurement.bronze.sap_material_stock_detaila LIMIT 10;