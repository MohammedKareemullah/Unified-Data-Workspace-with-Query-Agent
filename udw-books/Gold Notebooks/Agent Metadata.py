# Databricks notebook source
# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 1 — Gold: Agent Metadata
# MAGIC -- Describes every Gold table for Bedrock text-to-SQL
# MAGIC -- This is the knowledge base your AI agents use to
# MAGIC -- decide which table to query for each user question
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC USE CATALOG udw_procurement;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.agent_table_metadata
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/agent_metadata/'
# MAGIC AS
# MAGIC SELECT * FROM (VALUES
# MAGIC   (
# MAGIC     'udw_procurement.gold.spend_analytics',
# MAGIC     'Procurement spend by vendor, material group, plant, and month. Contains total spend in USD, PO counts, and delivery rates. Use for spend trend analysis, vendor spend ranking, category analysis, and monthly procurement reporting.',
# MAGIC     'vendor_id, vendor_name, vendor_category, vendor_country, material_group, company_code, plant, spend_month, spend_year, total_spend_usd, po_count, on_time_delivery_pct',
# MAGIC     'What is total spend by vendor this year?|Which material group has highest spend?|Show me monthly spend trend for 2025|What is procurement spend by plant?',
# MAGIC     'spend, procurement, purchase order, vendor cost, supplier spend, category spend, monthly spend',
# MAGIC     'GROUP BY vendor_name, sum total_spend_usd, filter by spend_year'
# MAGIC   ),
# MAGIC   (
# MAGIC     'udw_procurement.gold.spend_pareto',
# MAGIC     'Vendor Pareto analysis showing cumulative spend concentration. Pre-ranked vendors with spend percentage and cumulative percentage. Use for identifying top vendors, tail spend analysis, and 80/20 analysis.',
# MAGIC     'vendor_id, vendor_name, vendor_category, vendor_country, spend_rank, total_spend_usd, spend_pct_of_total, cumulative_spend_pct, spend_tier, total_pos',
# MAGIC     'Which vendors represent 80% of our spend?|Who are our top 10 vendors by spend?|What is tail spend percentage?|Show me vendor concentration risk',
# MAGIC     'pareto, 80/20, top vendors, spend concentration, tail spend, vendor ranking',
# MAGIC     'filter spend_tier = TOP 80% for strategic vendors, order by spend_rank'
# MAGIC   ),
# MAGIC   (
# MAGIC     'udw_procurement.gold.po_cycle_time',
# MAGIC     'Purchase order cycle time analysis by vendor and material group. Shows average, median, and P90 delivery days plus on-time delivery percentage. Use for procurement performance, vendor delivery analysis, and SLA monitoring.',
# MAGIC     'vendor_id, vendor_name, vendor_category, vendor_country, material_group, plant, avg_po_to_delivery_days, median_cycle_days, on_time_delivery_pct, overdue_rate_pct, cycle_time_rating, po_count',
# MAGIC     'Which vendors have longest delivery times?|What is average PO cycle time?|Show me overdue purchase orders by vendor|Which vendors consistently miss delivery dates?',
# MAGIC     'cycle time, delivery days, lead time, on-time delivery, overdue, procurement performance, SLA',
# MAGIC     'order by avg_po_to_delivery_days DESC for slowest vendors, filter cycle_time_rating for performance'
# MAGIC   ),
# MAGIC   (
# MAGIC     'udw_procurement.gold.inventory_health',
# MAGIC     'Current inventory health combining stock snapshot, movement history, planned demand, and inbound supply. Provides stockout risk, coverage status, and action priority for each material/plant combination.',
# MAGIC     'material_number, plant, storage_location, available_qty, stockout_risk, action_priority, coverage_status, days_since_last_movement, stock_aging_status, planned_demand_qty, open_po_inbound_qty, net_coverage_qty',
# MAGIC     'Which materials are at risk of stockout?|What is our inventory coverage for next month?|Show me slow moving stock|Which plants have critical stock levels?|What materials need urgent replenishment?',
# MAGIC     'inventory, stock, stockout, shortage, coverage, replenishment, slow moving, overstock, material availability',
# MAGIC     'filter action_priority = URGENT ACTION REQUIRED for critical items, filter stockout_risk for risk levels'
# MAGIC   ),
# MAGIC   (
# MAGIC     'udw_procurement.gold.vendor_scorecard',
# MAGIC     'Comprehensive vendor performance scorecard combining delivery, quality, logistics, and financial metrics. Each vendor has a composite score 0-100 and tier classification (STRATEGIC/PREFERRED/APPROVED/UNDER REVIEW).',
# MAGIC     'vendor_id, vendor_name, vendor_category, vendor_country, vendor_score, vendor_tier, on_time_delivery_pct, invoice_accuracy_pct, lot_acceptance_rate_pct, avg_defect_rate_pct, total_spend_usd, total_pos, contract_count, total_contract_value',
# MAGIC     'Who are my best performing vendors?|Which vendors have quality issues?|Compare vendor performance scores|Show me vendors under review|Which vendors should I prioritize for contract renewal?',
# MAGIC     'vendor score, supplier performance, vendor rating, quality, delivery, scorecard, KPI, supplier evaluation',
# MAGIC     'order by vendor_score DESC for best performers, filter vendor_tier for classification'
# MAGIC   ),
# MAGIC   (
# MAGIC     'udw_procurement.gold.budget_vs_actuals',
# MAGIC     'Budget vs actual spending by cost center, GL account, and fiscal period. Shows variance amount and percentage, utilization rate, and budget status. Use for financial control and procurement budget monitoring.',
# MAGIC     'cost_center, cost_center_name, gl_account, gl_description, fiscal_period_str, fiscal_year, fiscal_month, budget_amount_usd, actual_amount_usd, variance_usd, variance_pct, utilization_pct, budget_status',
# MAGIC     'Which cost centers are over budget?|What is budget utilization for procurement?|Show me variance by GL account|Which departments are underspending?',
# MAGIC     'budget, variance, actual, utilization, cost center, GL account, overspend, underspend, fiscal period',
# MAGIC     'filter budget_status for over/under, sum variance_usd for total exposure'
# MAGIC   ),
# MAGIC   (
# MAGIC     'udw_procurement.gold.manufacturing_analytics',
# MAGIC     'Manufacturing performance combining production orders, sales demand, inventory, and quality. Shows supply-demand balance per material and plant. Use for production planning, demand fulfillment analysis, and capacity insights.',
# MAGIC     'material_number, plant, order_status, production_order_count, avg_yield_rate_pct, open_customer_demand, available_stock, supply_demand_status, supply_demand_balance, avg_defect_rate_pct',
# MAGIC     'What is production yield rate by plant?|Which materials have supply gaps?|Show me production vs customer demand|Which materials are behind schedule?',
# MAGIC     'production, manufacturing, yield, supply gap, demand fulfillment, production order, schedule adherence',
# MAGIC     'filter supply_demand_status for gap analysis, filter stockout_risk for priority items'
# MAGIC   )
# MAGIC ) AS t(
# MAGIC   table_name,
# MAGIC   description,
# MAGIC   key_columns,
# MAGIC   example_questions,
# MAGIC   keywords,
# MAGIC   query_hints
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC -- CELL 2 — Gold: Agent Query Examples
# MAGIC -- Pre-built SQL for common agent queries
# MAGIC -- Bedrock can use these as few-shot examples
# MAGIC -- ════════════════════════════════════════════════════════════
# MAGIC
# MAGIC CREATE OR REPLACE TABLE udw_procurement.gold.agent_query_examples
# MAGIC USING DELTA
# MAGIC LOCATION 's3://udw-lake/gold/agent_query_examples/'
# MAGIC AS
# MAGIC SELECT * FROM (VALUES
# MAGIC   (
# MAGIC     'top_vendors_by_spend',
# MAGIC     'Who are our top 10 vendors by spend?',
# MAGIC     'SELECT vendor_name, vendor_category, vendor_country, ROUND(total_spend_usd, 2) AS total_spend_usd, spend_rank, ROUND(spend_pct_of_total, 2) AS spend_pct FROM udw_procurement.gold.spend_pareto ORDER BY spend_rank LIMIT 10',
# MAGIC     'spend_pareto',
# MAGIC     'spend, top vendors, ranking'
# MAGIC   ),
# MAGIC   (
# MAGIC     'overdue_pos_by_vendor',
# MAGIC     'Which vendors have the most overdue purchase orders?',
# MAGIC     'SELECT vendor_name, vendor_country, vendor_category, SUM(overdue_items) AS total_overdue, SUM(po_count) AS total_pos, ROUND(AVG(on_time_delivery_pct), 2) AS avg_on_time_pct FROM udw_procurement.gold.spend_analytics GROUP BY 1,2,3 HAVING SUM(overdue_items) > 0 ORDER BY total_overdue DESC',
# MAGIC     'spend_analytics',
# MAGIC     'overdue, delivery, vendor performance'
# MAGIC   ),
# MAGIC   (
# MAGIC     'critical_stock_materials',
# MAGIC     'Which materials need urgent replenishment?',
# MAGIC     'SELECT material_number, plant, available_qty, planned_demand_qty, open_po_inbound_qty, net_coverage_qty, action_priority, next_po_delivery_date FROM udw_procurement.gold.inventory_health WHERE action_priority IN (''URGENT ACTION REQUIRED'', ''CRITICAL - SUPPLY INCOMING'', ''HIGH RISK - NO SUPPLY'') ORDER BY available_qty ASC',
# MAGIC     'inventory_health',
# MAGIC     'stockout, critical, replenishment, urgent'
# MAGIC   ),
# MAGIC   (
# MAGIC     'vendor_performance_comparison',
# MAGIC     'Show me vendor performance scores ranked best to worst',
# MAGIC     'SELECT vendor_name, vendor_category, vendor_country, vendor_score, vendor_tier, ROUND(on_time_delivery_pct, 2) AS on_time_pct, ROUND(lot_acceptance_rate_pct, 2) AS quality_acceptance_pct, ROUND(avg_defect_rate_pct, 4) AS defect_rate, ROUND(total_spend_usd, 2) AS total_spend_usd FROM udw_procurement.gold.vendor_scorecard ORDER BY vendor_score DESC',
# MAGIC     'vendor_scorecard',
# MAGIC     'vendor score, performance, ranking, comparison'
# MAGIC   ),
# MAGIC   (
# MAGIC     'monthly_spend_trend',
# MAGIC     'Show me monthly procurement spend trend for the current year',
# MAGIC     'SELECT spend_year, spend_month_num, spend_month, SUM(total_spend_usd) AS total_spend_usd, SUM(po_count) AS po_count, ROUND(AVG(on_time_delivery_pct), 2) AS avg_on_time_pct FROM udw_procurement.gold.spend_analytics WHERE spend_year = YEAR(CURRENT_DATE()) GROUP BY 1,2,3 ORDER BY spend_year, spend_month_num',
# MAGIC     'spend_analytics',
# MAGIC     'monthly, trend, spend over time'
# MAGIC   ),
# MAGIC   (
# MAGIC     'budget_overrun',
# MAGIC     'Which cost centers are over budget?',
# MAGIC     'SELECT cost_center, cost_center_name, fiscal_year, SUM(budget_amount_usd) AS total_budget_usd, SUM(actual_amount_usd) AS total_actual_usd, SUM(variance_usd) AS total_variance_usd, ROUND(SUM(actual_amount_usd) / NULLIF(SUM(budget_amount_usd), 0) * 100, 2) AS utilization_pct FROM udw_procurement.gold.budget_vs_actuals WHERE budget_status = ''OVER BUDGET'' GROUP BY 1,2,3 ORDER BY total_variance_usd DESC',
# MAGIC     'budget_vs_actuals',
# MAGIC     'over budget, variance, cost center'
# MAGIC   ),
# MAGIC   (
# MAGIC     'supply_demand_gaps',
# MAGIC     'Which materials have supply gaps vs customer demand?',
# MAGIC     'SELECT material_number, plant, production_order_count, total_sales_demand_qty, available_stock, planned_supply_qty, supply_demand_balance, stockout_risk FROM udw_procurement.gold.manufacturing_analytics WHERE supply_demand_status = ''SUPPLY GAP'' ORDER BY supply_demand_balance ASC',
# MAGIC     'manufacturing_analytics',
# MAGIC     'supply gap, demand, shortage, fulfillment'
# MAGIC   ),
# MAGIC   (
# MAGIC     'quality_failing_vendors',
# MAGIC     'Which vendors have quality issues?',
# MAGIC     'SELECT vendor_name, vendor_category, vendor_country, total_inspection_lots, ROUND(avg_defect_rate_pct, 4) AS avg_defect_rate, ROUND(lot_acceptance_rate_pct, 2) AS acceptance_rate_pct, poor_lots, vendor_score, vendor_tier FROM udw_procurement.gold.vendor_scorecard WHERE total_inspection_lots > 0 AND (lot_acceptance_rate_pct < 80 OR avg_defect_rate_pct > 0.05) ORDER BY lot_acceptance_rate_pct ASC',
# MAGIC     'vendor_scorecard',
# MAGIC     'quality, defect, inspection, rejection'
# MAGIC   )
# MAGIC ) AS t(
# MAGIC   query_id,
# MAGIC   natural_language_question,
# MAGIC   sql_query,
# MAGIC   source_table,
# MAGIC   keywords
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CELL 3 — Final validation: all Gold tables created
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   CASE table_name
# MAGIC     WHEN 'spend_analytics'          THEN (SELECT COUNT(*) FROM udw_procurement.gold.spend_analytics)
# MAGIC     WHEN 'spend_pareto'             THEN (SELECT COUNT(*) FROM udw_procurement.gold.spend_pareto)
# MAGIC     WHEN 'po_cycle_time'            THEN (SELECT COUNT(*) FROM udw_procurement.gold.po_cycle_time)
# MAGIC     WHEN 'inventory_health'         THEN (SELECT COUNT(*) FROM udw_procurement.gold.inventory_health)
# MAGIC     WHEN 'vendor_scorecard'         THEN (SELECT COUNT(*) FROM udw_procurement.gold.vendor_scorecard)
# MAGIC     WHEN 'budget_vs_actuals'        THEN (SELECT COUNT(*) FROM udw_procurement.gold.budget_vs_actuals)
# MAGIC     WHEN 'manufacturing_analytics'  THEN (SELECT COUNT(*) FROM udw_procurement.gold.manufacturing_analytics)
# MAGIC     WHEN 'agent_table_metadata'     THEN (SELECT COUNT(*) FROM udw_procurement.gold.agent_table_metadata)
# MAGIC     WHEN 'agent_query_examples'     THEN (SELECT COUNT(*) FROM udw_procurement.gold.agent_query_examples)
# MAGIC   END AS record_count
# MAGIC FROM (VALUES
# MAGIC   ('spend_analytics'), ('spend_pareto'), ('po_cycle_time'),
# MAGIC   ('inventory_health'), ('vendor_scorecard'), ('budget_vs_actuals'),
# MAGIC   ('manufacturing_analytics'), ('agent_table_metadata'),
# MAGIC   ('agent_query_examples')
# MAGIC ) AS t(table_name);