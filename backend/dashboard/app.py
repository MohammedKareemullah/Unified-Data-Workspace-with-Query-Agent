# dashboard_api.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from aggregate import DashboardAggregator
from models import SpendOverviewDashboard, InventoryHealthDashboard, POCycleTimeDashboard, VendorParetoDashboard, ManufacturingAnalyticsDashboard, DashboardResponse
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create app
app = FastAPI(
    title="Insights Dashboard API",
    description="Real-time dashboards for procurement insights",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize aggregator
aggregator = DashboardAggregator()

# ============================================================================
# HEALTH & INFO
# ============================================================================

@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/dashboards/info")
async def dashboard_info():
    """Available dashboards"""
    return {
        "available_dashboards": [
            {
                "name": "spend_overview",
                "description": "Vendor spend analysis with trends",
                "endpoint": "/dashboards/spend"
            },
            {
                "name": "vendor_pareto",
                "description": "Pareto analysis of vendor concentration",
                "endpoint": "/dashboards/vendor-pareto"
            },
            {
                "name": "inventory_health",
                "description": "Inventory levels and stockout risks",
                "endpoint": "/dashboards/inventory"
            },
            {
                "name": "manufacturing_analytics",
                "description": "Production and fulfillment metrics",
                "endpoint": "/dashboards/manufacturing"
            },
            {
                "name": "po_cycle_time",
                "description": "Purchase order cycle time analysis",
                "endpoint": "/dashboards/po-cycle"
            },
            {
                "name": "complete_dashboard",
                "description": "All dashboards combined",
                "endpoint": "/dashboards/complete"
            }
        ]
    }


# ============================================================================
# INDIVIDUAL DASHBOARDS
# ============================================================================

@app.get("/dashboards/spend", response_model=SpendOverviewDashboard)
async def get_spend_dashboard():
    """Get spend overview dashboard"""
    try:
        return aggregator.aggregate_spend_overview()
    except Exception as e:
        logger.error(f"Error getting spend dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboards/vendor-pareto", response_model=VendorParetoDashboard)
async def get_vendor_pareto_dashboard():
    """Get vendor Pareto analysis"""
    try:
        return aggregator.aggregate_vendor_pareto()
    except Exception as e:
        logger.error(f"Error getting vendor Pareto: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboards/inventory", response_model=InventoryHealthDashboard)
async def get_inventory_dashboard():
    """Get inventory health dashboard"""
    try:
        return aggregator.aggregate_inventory_health()
    except Exception as e:
        logger.error(f"Error getting inventory dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboards/manufacturing", response_model=ManufacturingAnalyticsDashboard)
async def get_manufacturing_dashboard():
    """Get manufacturing analytics dashboard"""
    try:
        return aggregator.aggregate_manufacturing_analytics()
    except Exception as e:
        logger.error(f"Error getting manufacturing dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dashboards/po-cycle", response_model=POCycleTimeDashboard)
async def get_po_cycle_dashboard():
    """Get PO cycle time dashboard"""
    try:
        return aggregator.aggregate_po_cycle_time()
    except Exception as e:
        logger.error(f"Error getting PO cycle dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# UNIFIED DASHBOARD
# ============================================================================

@app.get("/dashboards/complete", response_model=DashboardResponse)
async def get_complete_dashboard(
    include_sections: Optional[str] = Query(
        None,
        description="Comma-separated list of sections: spend,vendor_pareto,inventory,manufacturing,po_cycle"
    )
):
    """
    Get complete dashboard with all visualizations.
    
    Optionally filter which sections to include for performance.
    """
    try:
        sections = None
        if include_sections:
            sections = [s.strip() for s in include_sections.split(',')]
        
        return aggregator.aggregate_complete_dashboard(include_sections=sections)
    except Exception as e:
        logger.error(f"Error getting complete dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

@app.post("/cache/clear")
async def clear_cache():
    """Clear all cached dashboard data"""
    try:
        aggregator.fetcher.clear_cache()
        return {"status": "success", "message": "Cache cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)