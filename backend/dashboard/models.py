# dashboard_models.py
"""
Pydantic models for dashboard visualizations.
These models define the exact data shape the frontend expects.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime

# ============================================================================
# ENUMS
# ============================================================================

# class SpendTier(str, Enum):
#     """Vendor spend tiers"""
#     TIER_A = "Tier A"
#     TIER_B = "Tier B"
#     TIER_C = "Tier C"


class StockoutRiskLevel(str, Enum):
    """Inventory risk levels"""
    LOW_RISK = "LOW_RISK"
    MEDIUM_RISK = "MEDIUM_RISK"
    HIGH_RISK = "HIGH_RISK"
    CRITICAL = "CRITICAL"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    HIGH = "HIGH"


class ActionPriority(str, Enum):
    """Action priority levels"""
    URGENT = "URGENT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


# ============================================================================
# SPEND OVERVIEW MODELS
# ============================================================================

class SpendByVendor(BaseModel):
    """Single vendor's spend data"""
    vendor_id: float | str | Any
    vendor_name: Any
    vendor_category: str
    vendor_country: str
    total_spend_usd: float
    
    class Config:
        json_schema_extra = {
            "example": {
                "vendor_id": "VND-001",
                "vendor_name": "Acme Supplies",
                "vendor_category": "Raw Materials",
                "vendor_country": "USA",
                "total_spend_usd": 125000.50
            }
        }


class SpendTrendPoint(BaseModel):
    """Monthly spend trend point"""
    year: int
    month: int
    month_name: str = Field(description="e.g., 'Jan', 'Feb'")
    total_spend_usd: float


class SpendByMaterialGroup(BaseModel):
    """Spend aggregated by material group"""
    material_group: Any
    total_spend_usd: float
    percentage_of_total: float


class POMetrics(BaseModel):
    """Purchase order metrics"""
    vendor_id: str | float | Any
    vendor_name: Any
    po_count: int
    line_item_count: int
    avg_po_value_usd: float


# class DeliveryMetrics(BaseModel):
#     """Delivery performance metrics"""
#     vendor_id: str | float
#     vendor_name: str
#     on_time_delivery_pct: float
#     overdue_rate_pct: float
#     completed_items: int
#     overdue_items: int
#     pending_items: int


class SpendOverviewDashboard(BaseModel):
    """Complete spend overview dashboard"""
    timestamp: datetime
    total_spend_usd: float
    vendor_count: int
    
    # Visualizations
    spend_by_vendor: List[SpendByVendor] = Field(description="Top 20 vendors by spend")
    spend_trend: List[SpendTrendPoint] = Field(description="Monthly spend trend")
    spend_by_material_group: List[SpendByMaterialGroup]
    po_metrics: List[POMetrics]
    # delivery_metrics: List[DeliveryMetrics]


# ============================================================================
# VENDOR PARETO MODELS
# ============================================================================

class ParetoVendor(BaseModel):
    """Single vendor in Pareto analysis"""
    vendor_id: str | float | Any
    vendor_name: Any
    total_spend_usd: float
    spend_pct_of_total: float
    cumulative_spend_pct: float
    spend_rank: int
    spend_tier: str


class VendorTierMetrics(BaseModel):
    """Aggregated metrics by tier"""
    spend_tier: str
    vendor_count: int
    total_spend_usd: float
    avg_on_time_delivery_pct: float
    percentage_of_total_spend: float


class VendorParetoDashboard(BaseModel):
    """Vendor Pareto analysis"""
    timestamp: datetime
    pareto_vendors: List[ParetoVendor] = Field(description="All vendors ranked by spend")
    tier_summary: List[VendorTierMetrics]
    top_vendors_80pct_spend: List[ParetoVendor] = Field(description="Vendors making up 80% of spend")


# ============================================================================
# INVENTORY HEALTH MODELS
# ============================================================================

class MaterialInventoryStatus(BaseModel):
    """Single material's inventory status"""
    material_number: str | float
    # material_name: Optional[str]
    plant: str | float
    total_stock_qty: float
    available_qty: float
    stockout_risk: StockoutRiskLevel
    avg_replenishment_days: Optional[float] = None
    open_po_inbound_qty: float
    coverage_status: str  # "Covered" or "At-Risk"
    days_since_last_movement: Any


class InventoryHeatmapCell(BaseModel):
    """Single cell for heatmap visualization"""
    material_number: str | float
    # material_name: str
    plant: str | Any
    value: float  # stock level or risk score
    risk_level: StockoutRiskLevel


# class ActionItem(BaseModel):
#     """Action item for inventory"""
#     material_number: str
#     material_name: str
#     plant: str
#     action_priority: ActionPriority
#     action_type: str  # "URGENT_REPLENISH", "REDUCE_STOCK", "REVIEW_OBSOLETE"
#     recommended_action: str


class InventoryHealthDashboard(BaseModel):
    """Inventory health overview"""
    timestamp: datetime
    total_materials: int
    critical_items_count: int
    at_risk_items_count: int
    
    # Visualizations
    stock_levels: List[MaterialInventoryStatus]
    heatmap_data: List[InventoryHeatmapCell]
    # action_items: List[ActionItem] = Field(description="Top priority actions")
    replenishment_timeline: List[Dict[str, Any]] = Field(description="Upcoming replenishments")


# ============================================================================
# MANUFACTURING PERFORMANCE MODELS
# ============================================================================

class ProductionMetrics(BaseModel):
    """Production metrics for a material"""
    material_number: str | float
    # material_name: Optional[str]
    plant: str | Any
    avg_production_completion: float  # percentage
    overdue_production_orders: int
    supply_demand_balance: float  # positive = surplus, negative = deficit
    supply_demand_status: str  # "Balanced", "Surplus", "Deficit"
    customer_fulfillment_rate: float  # percentage
    avg_defect_rate_pct: Optional[float] = None
    avg_quality_score: Optional[float] = None
    open_customer_demand: float


class ManufacturingAnalyticsDashboard(BaseModel):
    """Manufacturing performance overview"""
    timestamp: datetime
    avg_production_completion: float
    fulfillment_rate: float
    
    # Visualizations
    production_metrics: List[ProductionMetrics]
    overdue_orders: List[ProductionMetrics] = Field(description="Materials with overdue production")
    supply_demand_balance: List[ProductionMetrics]
    quality_scatter: List[Dict[str, float]] = Field(description="Defect vs Quality score")


# ============================================================================
# PO CYCLE TIME MODELS
# ============================================================================

class VendorCycleTime(BaseModel):
    """PO cycle time for a vendor"""
    vendor_id: str | float | Any
    vendor_name: Any
    avg_po_to_delivery_days: float
    median_cycle_days: float
    min_cycle_days: float
    max_cycle_days: float
    p90_cycle_days: float
    on_time_delivery_pct: float
    overdue_rate_pct: float
    cycle_time_rating: str  # "Excellent", "Good", "Fair", "Poor"
    po_count: int
    line_item_count: int


class DeliveryBucket(BaseModel):
    """Delivery time buckets"""
    vendor_id: str | float | Any
    vendor_name: Any
    within_7_days: int
    within_8_14_days: int
    within_15_30_days: int
    over_30_days: int


class POCycleTimeDashboard(BaseModel):
    """PO cycle time analysis"""
    timestamp: datetime
    avg_cycle_days_overall: float
    median_cycle_days_overall: float
    
    # Visualizations
    vendor_cycle_times: List[VendorCycleTime]
    on_time_vs_overdue: List[Dict[str, Any]]
    delivery_buckets: List[DeliveryBucket]
    cycle_time_distribution: Dict[str, Any] = Field(description="Box plot data")


# ============================================================================
# UNIFIED DASHBOARD
# ============================================================================

class DashboardResponse(BaseModel):
    """Complete dashboard response"""
    timestamp: datetime
    spend_overview: Optional[SpendOverviewDashboard] = None
    vendor_pareto: Optional[VendorParetoDashboard] = None
    inventory_health: Optional[InventoryHealthDashboard] = None
    manufacturing_analytics: Optional[ManufacturingAnalyticsDashboard] = None
    po_cycle_time: Optional[POCycleTimeDashboard] = None
    
    class Config:
        json_schema_extra = {
            "description": "Complete insights dashboard with all visualizations"
        }