# s3_data_fetcher.py
"""
Fetch data from S3 gold tables using Athena or direct Parquet reading.
"""

import boto3
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
from functools import lru_cache
import json

logger = logging.getLogger(__name__)

class S3DataFetcher:
    """Fetch and cache data from S3 gold tables"""
    
    def __init__(self, region: str = 'ap-south-1'):
        self.s3_client = boto3.client('s3', region_name=region)
        self.athena_client = boto3.client('athena', region_name=region)
        self.athena_database = 'udw-gold-catalog'
        self.athena_output = 's3://udw-lake/athena_result/'
        self.gold_bucket = 'udw-gold-catalog'
        self.gold_prefix = 'gold/'
        
        # Cache with 1-hour TTL
        self.cache = {}
        self.cache_ttl = 3600
    
    # ========================================================================
    # SPEND ANALYTICS
    # ========================================================================
    
    def get_spend_by_vendor(self, limit: int = 20) -> pd.DataFrame:
        """
        Fetch vendor spend data.
        Filters: total_spend_usd > 0
        """
        query = f"""
        SELECT 
            vendor_id,
            vendor_name,
            vendor_category,
            vendor_country,
            SUM(CAST(total_spend_usd AS DECIMAL(15,2))) as total_spend_usd
        FROM "{self.athena_database}"."spend_analytics"
        WHERE total_spend_usd > 0
        GROUP BY vendor_id, vendor_name, vendor_category, vendor_country
        ORDER BY total_spend_usd DESC
        LIMIT {limit}
        """
        
        return self._execute_athena_query(query)
    
    def get_spend_trend(self) -> pd.DataFrame:
        """
        Fetch monthly spend trend (year-over-year).
        """
        query = f"""
        SELECT 
            spend_year as year,
            spend_month_num as month,
            SUM(CAST(total_spend_usd AS DECIMAL(15,2))) as total_spend_usd
        FROM "{self.athena_database}"."spend_analytics"
        WHERE total_spend_usd > 0
        GROUP BY spend_year, spend_month_num
        ORDER BY spend_year ASC, spend_month_num ASC
        """
        
        return self._execute_athena_query(query)
    
    def get_spend_by_material_group(self) -> pd.DataFrame:
        """
        Fetch spend aggregated by material group.
        """
        query = f"""
        SELECT 
            material_group,
            SUM(CAST(total_spend_usd AS DECIMAL(15,2))) as total_spend_usd
        FROM "{self.athena_database}"."spend_analytics"
        GROUP BY material_group
        ORDER BY total_spend_usd DESC
        """
        
        return self._execute_athena_query(query)
    
    def get_po_metrics(self, limit: int = 20) -> pd.DataFrame:
        """
        Fetch PO and line-item counts by vendor.
        """
        query = f"""
        SELECT 
            vendor_id,
            vendor_name,
            COUNT(DISTINCT po_count) as po_count,
            COUNT(*) as line_item_count,
            AVG(CAST(avg_po_value_usd AS DECIMAL(15,2))) as avg_po_value_usd
        FROM "{self.athena_database}"."spend_analytics"
        WHERE avg_po_value_usd > 0
        GROUP BY vendor_id, vendor_name
        ORDER BY po_count DESC
        LIMIT {limit}
        """
        
        return self._execute_athena_query(query)


# # -------------------------------------------------------------------------------------

#     def get_delivery_metrics(self) -> pd.DataFrame:
#         """
#         Fetch on-time delivery and overdue metrics.
#         """
#         query = f"""
#         SELECT 
#             vendor_id,
#             vendor_name,
#             on_time_delivery_pct,            
#             COUNT(CASE WHEN delivery_status = 'Completed' THEN 1 END) as completed_items,
#             COUNT(CASE WHEN delivery_status = 'Overdue' THEN 1 END) as overdue_items,
#             COUNT(CASE WHEN delivery_status = 'Pending' THEN 1 END) as pending_items
#         FROM "{self.athena_database}"."spend_analytics"
#         WHERE on_time_delivery_pct IS NOT NULL
#         GROUP BY vendor_id, vendor_name, on_time_delivery_pct
#         ORDER BY on_time_delivery_pct DESC
#         """
        
#         return self._execute_athena_query(query)
    
    # ========================================================================
    # VENDOR PARETO
    # ========================================================================
    
    def get_vendor_pareto(self) -> pd.DataFrame:
        """
        Fetch Pareto analysis with cumulative spend %.
        """
        query = f"""
        SELECT 
            vendor_id,
            vendor_name,
            total_spend_usd,
            spend_pct_of_total,
            cumulative_spend_pct,
            spend_rank,
            spend_tier
        FROM "{self.athena_database}"."spend_pareto"
        WHERE total_spend_usd > 0
        ORDER BY spend_rank ASC
        """
        
        return self._execute_athena_query(query)
    
    def get_vendor_tier_summary(self) -> pd.DataFrame:
        """
        Fetch summary metrics by vendor tier.
        """
        query = f"""
        SELECT 
            spend_tier,
            COUNT(DISTINCT vendor_id) as vendor_count,
            SUM(CAST(total_spend_usd AS DECIMAL(15,2))) as total_spend_usd,
            AVG(CAST(avg_on_time_delivery_pct AS DECIMAL(5,2))) as avg_on_time_delivery_pct
        FROM "{self.athena_database}"."spend_pareto"
        WHERE total_spend_usd > 0
        GROUP BY spend_tier
        ORDER BY spend_tier ASC
        """
        
        return self._execute_athena_query(query)
    
    # ========================================================================
    # INVENTORY HEALTH
    # ========================================================================
    
    def get_inventory_status(self, risk_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch material inventory status.
        Optionally filter by risk level.
        """
        risk_clause = f"AND stockout_risk = '{risk_filter}'" if risk_filter else ""
        
        query = f"""
        SELECT 
            material_number,            
            plant,
            total_stock_qty,
            available_qty,
            stockout_risk,
            avg_replenishment_days,
            open_po_inbound_qty,
            coverage_status,
            days_since_last_movement
        FROM "{self.athena_database}"."inventory_health"
        WHERE stockout_risk IS NOT NULL
        {risk_clause}
        ORDER BY stockout_risk DESC, days_since_last_movement DESC
        """
        
        return self._execute_athena_query(query)
    
    def get_inventory_heatmap_data(self) -> pd.DataFrame:
        """
        Fetch data for stockout risk heatmap.
        """
        query = f"""
        SELECT 
            material_number,        
            plant,
            CASE 
                WHEN stockout_risk = 'CRITICAL' THEN 4
                WHEN stockout_risk = 'HIGH_RISK' THEN 3
                WHEN stockout_risk = 'MEDIUM_RISK' THEN 2
                WHEN stockout_risk = 'LOW_RISK' THEN 1
                ELSE 0
            END as risk_score,
            stockout_risk
        FROM "{self.athena_database}"."inventory_health"
        WHERE stockout_risk IS NOT NULL
        ORDER BY material_number ASC, plant ASC
        """
        
        return self._execute_athena_query(query)
    
    # def get_action_items(self) -> pd.DataFrame:
    #     """
    #     Fetch priority action items for inventory.
    #     """
    #     query = f"""
    #     SELECT 
    #         material_number,            
    #         plant,
    #         action_priority,
    #         CASE 
    #             WHEN stockout_risk = 'CRITICAL' AND available_qty < reorder_point THEN 'URGENT_REPLENISH'
    #             WHEN total_stock_qty > (avg_monthly_consumption * 12) THEN 'REDUCE_STOCK'
    #             WHEN days_since_last_movement > 365 THEN 'REVIEW_OBSOLETE'
    #             ELSE 'MONITOR'
    #         END as action_type,
    #         CASE 
    #             WHEN stockout_risk = 'CRITICAL' THEN 'Urgent: Material at critical stock level'
    #             WHEN available_qty < reorder_point THEN 'High: Below reorder point'
    #             WHEN days_since_last_movement > 365 THEN 'Review: No movement for 1+ year'
    #             ELSE 'Monitor: Status OK'
    #         END as recommended_action
    #     FROM "{self.athena_database}"."inventory_health"
    #     WHERE action_priority IS NOT NULL
    #     ORDER BY 
    #         CASE 
    #             WHEN action_priority = 'URGENT' THEN 1
    #             WHEN action_priority = 'HIGH' THEN 2
    #             WHEN action_priority = 'MEDIUM' THEN 3
    #             ELSE 4
    #         END ASC,
    #         days_since_last_movement DESC
    #     LIMIT 20
    #     """
        
    #     return self._execute_athena_query(query)
    
    # ========================================================================
    # MANUFACTURING ANALYTICS
    # ========================================================================
    
    def get_production_metrics(self) -> pd.DataFrame:
        """
        Fetch production completion and quality metrics.
        """
        query = f"""
        SELECT 
            material_number,            
            plant,
            avg_production_completion,
            overdue_production_orders,
            supply_demand_balance,
            supply_demand_status,
            customer_fulfillment_rate,
            COALESCE(avg_defect_rate_pct, 0) as avg_defect_rate_pct,
            COALESCE(avg_quality_score, 0) as avg_quality_score,
            open_customer_demand
        FROM "{self.athena_database}"."manufacturing_analytics"
        WHERE avg_production_completion IS NOT NULL
        ORDER BY avg_production_completion DESC
        """
        
        return self._execute_athena_query(query)
    
    def get_overdue_orders(self) -> pd.DataFrame:
        """
        Fetch materials with overdue production orders.
        """
        query = f"""
        SELECT 
            material_number,            
            plant,
            overdue_production_orders,
            avg_production_completion,
            customer_fulfillment_rate
        FROM "{self.athena_database}"."manufacturing_analytics"
        WHERE overdue_production_orders > 0
        ORDER BY overdue_production_orders DESC
        LIMIT 20
        """
        
        return self._execute_athena_query(query)
    
    # ========================================================================
    # PO CYCLE TIME
    # ========================================================================
    
    def get_vendor_cycle_times(self) -> pd.DataFrame:
        """
        Fetch PO cycle time metrics by vendor.
        """
        query = f"""
        SELECT 
            vendor_id,
            vendor_name,
            avg_po_to_delivery_days,
            median_cycle_days,
            min_cycle_days,
            max_cycle_days,
            p90_cycle_days,
            on_time_delivery_pct,
            overdue_rate_pct,
            CASE 
                WHEN avg_po_to_delivery_days <= 7 THEN 'Excellent'
                WHEN avg_po_to_delivery_days <= 14 THEN 'Good'
                WHEN avg_po_to_delivery_days <= 30 THEN 'Fair'
                ELSE 'Poor'
            END as cycle_time_rating,
            po_count,
            line_item_count
        FROM "{self.athena_database}"."po_cycle_time"
        WHERE avg_po_to_delivery_days IS NOT NULL AND avg_po_to_delivery_days >= 0
        ORDER BY avg_po_to_delivery_days ASC
        """
        
        return self._execute_athena_query(query)
    
    def get_delivery_buckets(self) -> pd.DataFrame:
        """
        Fetch delivery time buckets (7d, 14d, 30d, 30d+).
        """
        query = f"""
        SELECT 
            vendor_id,
            vendor_name,
            within_7_days,
            within_8_14_days,
            within_15_30_days,
            over_30_days
        FROM "{self.athena_database}"."po_cycle_time"
        ORDER BY (within_7_days + within_8_14_days + within_15_30_days + over_30_days) DESC
        """
        
        return self._execute_athena_query(query)
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _execute_athena_query(self, query: str, cache_key: Optional[str] = None) -> pd.DataFrame:
        """Execute Athena query and return as DataFrame"""
        
        # Check cache
        if cache_key and cache_key in self.cache:
            cached_data, timestamp = self.cache[cache_key]
            if datetime.utcnow() - timestamp < timedelta(seconds=self.cache_ttl):
                logger.info(f"Returning cached data for {cache_key}")
                return cached_data
        
        try:
            # Execute query
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.athena_database},
                ResultConfiguration={'OutputLocation': self.athena_output},
                WorkGroup='primary'
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Poll for completion
            while True:
                exec_response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                status = exec_response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    raise Exception(f"Query {status}: {exec_response['QueryExecution']['Status'].get('StateChangeReason')}")
                
                import time
                time.sleep(0.5)
            
            # Get results
            results = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            # Convert to DataFrame
            df = self._athena_to_dataframe(results)
            
            # Cache result
            if cache_key:
                self.cache[cache_key] = (df, datetime.utcnow())
            
            return df
        
        except Exception as e:
            logger.error(f"Athena query failed: {str(e)}")
            raise
    
    def _athena_to_dataframe(self, results: Dict[str, Any]) -> pd.DataFrame:
        """Convert Athena results to pandas DataFrame"""
        
        columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        
        for row in results['ResultSet']['Rows'][1:]:  # Skip header
            row_data = {}
            for i, col in enumerate(columns):
                value = row['Data'][i].get('VarCharValue', '')
                # Try to convert to numeric if possible
                if value:
                    try:
                        row_data[col] = float(value)
                    except ValueError:
                        row_data[col] = value
                else:
                    row_data[col] = None
            rows.append(row_data)
        
        return pd.DataFrame(rows)
    
    def clear_cache(self):
        """Clear all cached data"""
        self.cache.clear()
        logger.info("Cache cleared")