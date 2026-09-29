# dashboard_aggregator.py
"""
Transform raw data from S3 into dashboard-ready models.
Handles filtering, calculations, and null/zero filtering.
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from models import SpendOverviewDashboard, SpendByVendor, SpendTrendPoint, SpendByMaterialGroup, POMetrics, VendorParetoDashboard, ParetoVendor, VendorTierMetrics, VendorParetoDashboard, InventoryHealthDashboard, MaterialInventoryStatus, StockoutRiskLevel, InventoryHeatmapCell, StockoutRiskLevel, ActionPriority, InventoryHealthDashboard, StockoutRiskLevel, ManufacturingAnalyticsDashboard, POCycleTimeDashboard, VendorCycleTime, DeliveryBucket, DashboardResponse, ProductionMetrics
from fetcher import S3DataFetcher
import logging

logger = logging.getLogger(__name__)

class DashboardAggregator:
    """Aggregate and transform data for dashboards"""
    
    def __init__(self):
        self.fetcher = S3DataFetcher()
    
    # ========================================================================
    # SPEND OVERVIEW
    # ========================================================================
    
    def aggregate_spend_overview(self) -> SpendOverviewDashboard:
        """Create complete spend overview dashboard"""
        
        try:
            # Fetch all required data
            spend_by_vendor_df = self.fetcher.get_spend_by_vendor(limit=20)
            spend_trend_df = self.fetcher.get_spend_trend()
            material_group_df = self.fetcher.get_spend_by_material_group()
            po_metrics_df = self.fetcher.get_po_metrics(limit=20)
            # delivery_metrics_df = self.fetcher.get_delivery_metrics()
            
            # Transform to models
            spend_by_vendor = [
                SpendByVendor(
                    vendor_id=row['vendor_id'],
                    vendor_name=row['vendor_name'],
                    vendor_category=row['vendor_category'],
                    vendor_country=row['vendor_country'],
                    total_spend_usd=float(row['total_spend_usd'])
                )
                for _, row in spend_by_vendor_df.iterrows()
            ]
            
            # Monthly trend with month names
            month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            spend_trend = [
                SpendTrendPoint(
                    year=int(row['year']),
                    month=int(row['month']),
                    month_name=month_names[int(row['month'])],
                    total_spend_usd=float(row['total_spend_usd'])
                )
                for _, row in spend_trend_df.iterrows()
            ]
            
            # Material group (calculate percentages)
            total_spend = material_group_df['total_spend_usd'].sum()
            spend_by_material = [
                SpendByMaterialGroup(
                    material_group=row['material_group'],
                    total_spend_usd=float(row['total_spend_usd']),
                    percentage_of_total=(float(row['total_spend_usd']) / total_spend * 100) if total_spend > 0 else 0
                )
                for _, row in material_group_df.iterrows()
            ]
            
            # PO metrics
            po_metrics = [
                POMetrics(
                    vendor_id=row['vendor_id'],
                    vendor_name=row['vendor_name'],
                    po_count=int(row['po_count']),
                    line_item_count=int(row['line_item_count']),
                    avg_po_value_usd=float(row['avg_po_value_usd'])
                )
                for _, row in po_metrics_df.iterrows()
            ]
            
            # # Delivery metrics
            # delivery_metrics = [
            #     DeliveryMetrics(
            #         vendor_id=row['vendor_id'],
            #         vendor_name=row['vendor_name'],
            #         on_time_delivery_pct=float(row['on_time_delivery_pct']),
            #         overdue_rate_pct=float(row['overdue_rate_pct']),
            #         completed_items=int(row['completed_items']),
            #         overdue_items=int(row['overdue_items']),
            #         pending_items=int(row['pending_items'])
            #     )
            #     for _, row in delivery_metrics_df.iterrows()
            # ]
            
            return SpendOverviewDashboard(
                timestamp=datetime.utcnow(),
                total_spend_usd=total_spend,
                vendor_count=len(spend_by_vendor),
                spend_by_vendor=spend_by_vendor,
                spend_trend=spend_trend,
                spend_by_material_group=spend_by_material,
                po_metrics=po_metrics,
                # delivery_metrics=delivery_metrics
            )
        
        except Exception as e:
            logger.error(f"Error aggregating spend overview: {str(e)}")
            raise
    
    # ========================================================================
    # VENDOR PARETO
    # ========================================================================
    
    def aggregate_vendor_pareto(self) -> VendorParetoDashboard:
        """Create vendor Pareto analysis dashboard"""
        
        try:
            pareto_df = self.fetcher.get_vendor_pareto()
            tier_summary_df = self.fetcher.get_vendor_tier_summary()
            
            # All vendors in Pareto order
            pareto_vendors = [
                ParetoVendor(
                    vendor_id=row['vendor_id'],
                    vendor_name=row['vendor_name'],
                    total_spend_usd=float(row['total_spend_usd']),
                    spend_pct_of_total=float(row['spend_pct_of_total']),
                    cumulative_spend_pct=float(row['cumulative_spend_pct']),
                    spend_rank=int(row['spend_rank']),
                    spend_tier=str(row['spend_tier'])
                )
                for _, row in pareto_df.iterrows()
            ]
            
            # Vendors making up 80% of spend
            top_80pct = [v for v in pareto_vendors if v.cumulative_spend_pct <= 80]
            
            # Tier summary
            tier_summary = [
                VendorTierMetrics(
                    spend_tier=str(row['spend_tier']),
                    vendor_count=int(row['vendor_count']),
                    total_spend_usd=float(row['total_spend_usd']),
                    avg_on_time_delivery_pct=float(row['avg_on_time_delivery_pct']) if row['avg_on_time_delivery_pct'] else 0,
                    percentage_of_total_spend=(float(row['total_spend_usd']) / pareto_df['total_spend_usd'].sum() * 100)
                )
                for _, row in tier_summary_df.iterrows()
            ]
            
            return VendorParetoDashboard(
                timestamp=datetime.utcnow(),
                pareto_vendors=pareto_vendors,
                tier_summary=tier_summary,
                top_vendors_80pct_spend=top_80pct
            )
        
        except Exception as e:
            logger.error(f"Error aggregating vendor Pareto: {str(e)}")
            raise
    
    # ========================================================================
    # INVENTORY HEALTH
    # ========================================================================
    
    def aggregate_inventory_health(self) -> InventoryHealthDashboard:
        """Create inventory health dashboard"""
        
        try:
            inventory_df = self.fetcher.get_inventory_status()
            heatmap_df = self.fetcher.get_inventory_heatmap_data()
            # action_items_df = self.fetcher.get_action_items()
            
            # Inventory status
            stock_levels = [
                MaterialInventoryStatus(
                    material_number=row['material_number'],
                    # material_name=row.get('material_name'),
                    plant=row['plant'],
                    total_stock_qty=float(row['total_stock_qty']),
                    available_qty=float(row['available_qty']),
                    stockout_risk=StockoutRiskLevel(row['stockout_risk']),
                    avg_replenishment_days=float(row['avg_replenishment_days']) if row.get('avg_replenishment_days') else None,
                    open_po_inbound_qty=float(row['open_po_inbound_qty']),
                    coverage_status=row['coverage_status'],
                    days_since_last_movement=row['days_since_last_movement']
                )
                for _, row in inventory_df.iterrows()
            ]
            
            # Heatmap data
            heatmap_data = [
                InventoryHeatmapCell(
                    material_number=row['material_number'],
                    # material_name=row['material_name'],
                    plant=row['plant'],
                    value=float(row['risk_score']),
                    risk_level=StockoutRiskLevel(row['stockout_risk'])
                )
                for _, row in heatmap_df.iterrows()
            ]
            
            # # Action items
            # action_items = [
            #     ActionItem(
            #         material_number=row['material_number'],
            #         material_name=row['material_name'],
            #         plant=row['plant'],
            #         action_priority=ActionPriority(row['action_priority']),
            #         action_type=row['action_type'],
            #         recommended_action=row['recommended_action']
            #     )
            #     for _, row in action_items_df.iterrows()
            # ]
            
            # Count by risk level
            critical_count = len([s for s in stock_levels if s.stockout_risk == StockoutRiskLevel.CRITICAL])
            at_risk_count = len([s for s in stock_levels if s.stockout_risk == StockoutRiskLevel.HIGH_RISK])
            
            return InventoryHealthDashboard(
                timestamp=datetime.utcnow(),
                total_materials=len(stock_levels),
                critical_items_count=critical_count,
                at_risk_items_count=at_risk_count,
                stock_levels=stock_levels,
                heatmap_data=heatmap_data,
                # action_items=action_items,
                replenishment_timeline=[]
            )
        
        except Exception as e:
            logger.error(f"Error aggregating inventory health: {str(e)}")
            raise
    
    # ========================================================================
    # MANUFACTURING ANALYTICS
    # ========================================================================
    
    def aggregate_manufacturing_analytics(self) -> ManufacturingAnalyticsDashboard:
        """Create manufacturing analytics dashboard"""
        
        try:
            production_df = self.fetcher.get_production_metrics()
            overdue_df = self.fetcher.get_overdue_orders()
            
            # Production metrics
            production_metrics = [
                ProductionMetrics(
                    material_number=row['material_number'],
                    # material_name=row.get('material_name'),
                    plant=row['plant'],
                    avg_production_completion=float(row['avg_production_completion']),
                    overdue_production_orders=int(row['overdue_production_orders']),
                    supply_demand_balance=float(row['supply_demand_balance']),
                    supply_demand_status=row['supply_demand_status'],
                    customer_fulfillment_rate=float(row['customer_fulfillment_rate']),
                    avg_defect_rate_pct=float(row['avg_defect_rate_pct']) if row['avg_defect_rate_pct'] else None,
                    avg_quality_score=float(row['avg_quality_score']) if row['avg_quality_score'] else None,
                    open_customer_demand=float(row['open_customer_demand'])
                )
                for _, row in production_df.iterrows()
            ]
            
            # Overdue orders
            overdue_metrics = [
                ProductionMetrics(
                    material_number=row['material_number'],
                    # material_name=row.get('material_name'),
                    plant=row['plant'],
                    avg_production_completion=float(row['avg_production_completion']),
                    overdue_production_orders=int(row['overdue_production_orders']),
                    supply_demand_balance=0,
                    supply_demand_status="Overdue",
                    customer_fulfillment_rate=float(row['customer_fulfillment_rate']),
                    open_customer_demand=0
                )
                for _, row in overdue_df.iterrows()
            ]
            
            # Calculate averages
            avg_completion = production_df['avg_production_completion'].mean()
            avg_fulfillment = production_df['customer_fulfillment_rate'].mean()
            
            return ManufacturingAnalyticsDashboard(
                timestamp=datetime.utcnow(),
                avg_production_completion=avg_completion,
                fulfillment_rate=avg_fulfillment,
                production_metrics=production_metrics,
                overdue_orders=overdue_metrics,
                supply_demand_balance=production_metrics,
                quality_scatter=[]
            )
        
        except Exception as e:
            logger.error(f"Error aggregating manufacturing analytics: {str(e)}")
            raise
    
    # ========================================================================
    # PO CYCLE TIME
    # ========================================================================
    
    def aggregate_po_cycle_time(self) -> POCycleTimeDashboard:
        """Create PO cycle time dashboard"""
        
        try:
            cycle_time_df = self.fetcher.get_vendor_cycle_times()
            delivery_buckets_df = self.fetcher.get_delivery_buckets()
            
            # Vendor cycle times
            vendor_cycle_times = [
                VendorCycleTime(
                    vendor_id=row['vendor_id'],
                    vendor_name=row['vendor_name'],
                    avg_po_to_delivery_days=float(row['avg_po_to_delivery_days']),
                    median_cycle_days=float(row['median_cycle_days']),
                    min_cycle_days=float(row['min_cycle_days']),
                    max_cycle_days=float(row['max_cycle_days']),
                    p90_cycle_days=float(row['p90_cycle_days']),
                    on_time_delivery_pct=float(row['on_time_delivery_pct']),
                    overdue_rate_pct=float(row['overdue_rate_pct']),
                    cycle_time_rating=row['cycle_time_rating'],
                    po_count=int(row['po_count']),
                    line_item_count=int(row['line_item_count'])
                )
                for _, row in cycle_time_df.iterrows()
            ]
            
            # Delivery buckets
            delivery_buckets = [
                DeliveryBucket(
                    vendor_id=row['vendor_id'],
                    vendor_name=row['vendor_name'],
                    within_7_days=int(row['within_7_days']),
                    within_8_14_days=int(row['within_8_14_days']),
                    within_15_30_days=int(row['within_15_30_days']),
                    over_30_days=int(row['over_30_days'])
                )
                for _, row in delivery_buckets_df.iterrows()
            ]
            
            # Calculate overall averages
            avg_cycle = cycle_time_df['avg_po_to_delivery_days'].mean()
            median_cycle = cycle_time_df['median_cycle_days'].median()
            
            return POCycleTimeDashboard(
                timestamp=datetime.utcnow(),
                avg_cycle_days_overall=avg_cycle,
                median_cycle_days_overall=median_cycle,
                vendor_cycle_times=vendor_cycle_times,
                on_time_vs_overdue=[],
                delivery_buckets=delivery_buckets,
                cycle_time_distribution={}
            )
        
        except Exception as e:
            logger.error(f"Error aggregating PO cycle time: {str(e)}")
            raise
    
    # ========================================================================
    # UNIFIED DASHBOARD
    # ========================================================================
    
    def aggregate_complete_dashboard(self, include_sections: List[str] = None) -> DashboardResponse:
        """
        Create complete dashboard with all sections.
        
        Args:
            include_sections: List of sections to include 
                ['spend', 'vendor_pareto', 'inventory', 'manufacturing', 'po_cycle']
                If None, includes all.
        """
        
        if include_sections is None:
            include_sections = ['spend', 'vendor_pareto', 'inventory', 'manufacturing', 'po_cycle']
        
        dashboard = DashboardResponse(timestamp=datetime.utcnow())
        
        if 'spend' in include_sections:
            dashboard.spend_overview = self.aggregate_spend_overview()
        
        if 'vendor_pareto' in include_sections:
            dashboard.vendor_pareto = self.aggregate_vendor_pareto()
        
        if 'inventory' in include_sections:
            dashboard.inventory_health = self.aggregate_inventory_health()
        
        if 'manufacturing' in include_sections:
            dashboard.manufacturing_analytics = self.aggregate_manufacturing_analytics()
        
        if 'po_cycle' in include_sections:
            dashboard.po_cycle_time = self.aggregate_po_cycle_time()
        
        return dashboard