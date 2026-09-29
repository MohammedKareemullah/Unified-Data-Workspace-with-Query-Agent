# schema_tools.py
"""
Schema discovery tools for dynamic table introspection.
These tools allow Claude to understand your actual data structure
and generate queries without hardcoding field names.
"""

import boto3
import json
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from strands import tool
from decimal import Decimal

# Initialize AWS clients
athena_client = boto3.client('athena', region_name='ap-south-1')
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

# Configuration
ATHENA_DATABASE = 'udw-gold-catalog'
ATHENA_OUTPUT_BUCKET = 's3://udw-lake/athena_result/'
SCHEMA_CACHE_TABLE = 'athena-schema-cache'  # DynamoDB table for caching
SCHEMA_CACHE_TTL = 86400  # 24 hours in seconds

# ============================================================================
# TOOL 1: Get All Available Tables
# ============================================================================
@tool(
    description="List all available tables in the procurement analytics database. "
    "Use this to see what data sources are available.",
)
def get_available_tables() -> dict:
    """
    List all tables in the Athena database.
    
    Returns:
        dict with list of table names and their descriptions


    examples=[
            "What tables do we have in the database?",
            "Show me all available data tables"
        ]
    """
    
    # Try to get from cache first
    cached = _get_schema_cache_item("all_tables")
    if cached:
        print("[INFO] Returning tables from cache")
        return cached['data']
    
    # Query information_schema for tables
    query = f"""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = '{ATHENA_DATABASE}'
    ORDER BY table_name
    """
    
    try:
        results = _execute_athena_query(query)
        
        tables = [r['table_name'] for r in results]
        
        response = {
            "status": "success",
            "database": ATHENA_DATABASE,
            "table_count": len(tables),
            "tables": tables
        }
        
        # Cache the result
        _set_schema_cache_item("all_tables", response)
        
        return response
    
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list tables: {str(e)}",
            "tables": []
        }


# ============================================================================
# TOOL 2: Get Table Schema (Columns & Data Types)
# ============================================================================
@tool(
    description="Get detailed schema information for a specific table. "
    "Shows all available columns, their data types, and nullability. "
    "Use this before generating queries to understand what fields exist.",

)
def get_table_schema(table_name: str) -> dict:
    """
    Get detailed column information for a table.
    
    Args:
        table_name: Name of the table to inspect
    
    Returns:
        dict with columns, data types, and sample data

    examples=[
        "Show me the schema of the vendor_scorecard table",
        "What columns are available in the spend_analytics table?",
        "Get schema for inventory_health table"
    ]
    """
    
    # Try cache first
    cache_key = f"schema:{table_name}"
    cached = _get_schema_cache_item(cache_key)
    if cached:
        print(f"[INFO] Returning schema for {table_name} from cache")
        return cached['data']
    
    # Query information_schema for column details
    query = f"""
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns
    WHERE table_schema = '{ATHENA_DATABASE}'
    AND table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    
    try:
        columns = _execute_athena_query(query)
        
        if not columns:
            return {
                "status": "error",
                "table_name": table_name,
                "message": f"Table {table_name} not found"
            }
        
        # Get row count
        count_query = f""" SELECT COUNT(*) as row_count FROM "{ATHENA_DATABASE}"."{table_name}" """
        try:
            count_result = _execute_athena_query(count_query)
            row_count = count_result[0]['row_count'] if count_result else 0
        except:
            row_count = "Unknown"
        
        # Get sample data
        sample_query = f""" SELECT * FROM "{ATHENA_DATABASE}"."{table_name}" LIMIT 5 """
        try:
            sample_data = _execute_athena_query(sample_query)
        except:
            sample_data = []
        
        response = {
            "status": "success",
            "table_name": table_name,
            "row_count": row_count,
            "columns": [
                {
                    "name": col['column_name'],
                    "data_type": col['data_type'],
                    "nullable": col['is_nullable'] == 'YES',
                    "default": col['column_default']
                }
                for col in columns
            ],
            "sample_data": sample_data[:3] if sample_data else []
        }
        
        # Cache the schema
        _set_schema_cache_item(cache_key, response)
        
        return response
    
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "message": f"Failed to get schema: {str(e)}"
        }


# ============================================================================
# TOOL 3: Query Column Statistics (Non-Null Columns)
# ============================================================================
@tool(
    description="Get statistics about which columns have data. "
    "Shows non-null count, unique values, and data quality metrics for each column. "
    "Use this to understand data availability before querying.",

)
def get_column_statistics(table_name: str, limit: int = 10) -> dict:
    """
    Get column-level statistics including non-null counts.
    
    Args:
        table_name: Table to analyze
        limit: Number of columns to show
    
    Returns:
        dict with column stats


        examples=[
        "Which columns in vendor_scorecard have the most data?",
        "Show me data quality stats for the spend_analytics table",
        "Which fields are commonly populated in inventory_health?"
    ]
    """
    
    try:
        # First get the schema to know all columns
        schema_response = get_table_schema(table_name)
        
        if schema_response['status'] != 'success':
            return schema_response
        
        columns = schema_response['columns']
        
        # Build a query that counts non-nulls for each column
        column_stats = []
        
        for col in columns:
            col_name = col['name']
            col_type = col['data_type']
            
            # Count non-nulls
            count_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT({col_name}) as non_null_count,
                COUNT(DISTINCT {col_name}) as unique_count,
                CAST(COUNT({col_name}) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as percent_populated
            FROM "{ATHENA_DATABASE}"."{table_name}"
            """
            
            try:
                result = _execute_athena_query(count_query)
                if result:
                    stats = result[0]
                    column_stats.append({
                        "column_name": col_name,
                        "data_type": col_type,
                        "total_rows": stats.get('total_rows', 0),
                        "non_null_count": stats.get('non_null_count', 0),
                        "unique_count": stats.get('unique_count', 0),
                        "percent_populated": stats.get('percent_populated', 0),
                        "recommendation": "Good" if stats.get('percent_populated', 0) > 80 else "Sparse" if stats.get('percent_populated', 0) < 20 else "Fair"
                    })
            except Exception as e:
                print(f"[WARNING] Could not get stats for column {col_name}: {str(e)}")
                column_stats.append({
                    "column_name": col_name,
                    "data_type": col_type,
                    "status": "error"
                })
        
        # Sort by percent populated
        column_stats.sort(key=lambda x: x.get('percent_populated', 0), reverse=True)
        
        return {
            "status": "success",
            "table_name": table_name,
            "column_statistics": column_stats[:limit],
            "total_columns": len(columns),
            "recommendation": "Use columns with > 80% population for reliable queries"
        }
    
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "message": f"Failed to get statistics: {str(e)}"
        }


# ============================================================================
# TOOL 4: Execute Custom SQL Query
# ============================================================================
@tool(
    description="Execute a SQL query that you construct based on discovered schema. "
    "Use this AFTER exploring tables with schema tools. "
    "LLM generates the SQL dynamically based on available columns.",

)
def execute_custom_query(
    sql_query: str,
    limit_results: int = 100
) -> dict:
    """
    Execute a custom SQL query safely.
    
    Args:
        sql_query: SQL query to execute
        limit_results: Max rows to return
    
    Returns:
        dict with query results

        examples=[
        "Execute: SELECT vendor_id, vendor_name FROM vendor_scorecard LIMIT 10",
        "Run a query to find top 5 vendors by score"
    ]
    """
    
    # Safety checks
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'INSERT', 'UPDATE']
    sql_upper = sql_query.upper()
    
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            return {
                "status": "error",
                "message": f"Query contains {keyword} - read-only queries only"
            }
    
    # Ensure database context
    if ATHENA_DATABASE not in sql_query:
        # Try to prefix table names
        pass
    
    try:
        results = _execute_athena_query(sql_query)
        
        return {
            "status": "success",
            "row_count": len(results),
            "data": results[:limit_results],
            "query": sql_query,
            "note": f"Returned {len(results)} rows (limited to {limit_results})"
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": f"Query execution failed: {str(e)}",
            "query": sql_query
        }


# ============================================================================
# TOOL 5: Get Query Template (Helper for Claude)
# ============================================================================
@tool(
    description="Get SQL query templates based on your analysis goal. "
    "Templates show common query patterns using actual table/column names from your schema.",
)
def get_query_template(analysis_type: str) -> dict:
    """
    Provide SQL templates based on analysis type.
    
    Args:
        analysis_type: Type of analysis needed
    
    Returns:
        dict with template queries and explanation

        examples=[
        "Show me a template for finding vendors with low quality scores",
        "Get a template for calculating YTD spend by vendor"
    ]
    """
    
    templates = {
        "vendor_performance": {
            "description": "Find vendors with specific performance criteria",
            "template": """
SELECT 
    vendor_id, 
    vendor_name, 
    overall_score, 
    on_time_delivery_rate, 
    quality_score, 
    cost_competitiveness
FROM {database}.vendor_scorecard
WHERE overall_score < 70
ORDER BY overall_score ASC
LIMIT 10
            """,
            "usage": "Modify WHERE clause to match your criteria. Check column names with get_table_schema('vendor_scorecard')"
        },
        
        "spend_analysis": {
            "description": "Analyze spending by category or vendor",
            "template": """
SELECT 
    category,
    vendor_id,
    SUM(spend_amount) as total_spend,
    COUNT(*) as transaction_count,
    AVG(unit_price) as avg_price
FROM {database}.spend_analytics
WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE)
GROUP BY category, vendor_id
ORDER BY total_spend DESC
LIMIT 20
            """,
            "usage": "Verify column names exist with get_table_schema('spend_analytics'). Adjust date filter and grouping as needed."
        },
        
        "inventory_status": {
            "description": "Check inventory levels and reorder status",
            "template": """
SELECT 
    material_id,
    material_name,
    quantity_on_hand,
    inventory_status,
    days_of_supply
FROM {database}.inventory_health
WHERE inventory_status IN ('Low', 'Critical')
ORDER BY days_of_supply ASC
LIMIT 20
            """,
            "usage": "Use get_table_schema('inventory_health') to confirm available columns"
        },
        
        "vendor_join": {
            "description": "Join vendor scorecard with spend data",
            "template": """
SELECT 
    v.vendor_id,
    v.vendor_name,
    v.overall_score,
    SUM(s.spend_amount) as ytd_spend
FROM {database}.vendor_scorecard v
LEFT JOIN {database}.spend_analytics s 
    ON v.vendor_id = s.vendor_id
GROUP BY v.vendor_id, v.vendor_name, v.overall_score
ORDER BY ytd_spend DESC
            """,
            "usage": "Verify table names and column names before executing"
        }
    }
    
    if analysis_type.lower() not in templates:
        return {
            "status": "error",
            "available_templates": list(templates.keys()),
            "message": f"Unknown type '{analysis_type}'. Choose from available templates."
        }
    
    template = templates[analysis_type.lower()]
    template['template'] = template['template'].format(database=ATHENA_DATABASE)
    
    return {
        "status": "success",
        "analysis_type": analysis_type,
        "template": template
    }


# ============================================================================
# HELPER FUNCTIONS (not exposed as tools)
# ============================================================================

def _execute_athena_query(query: str, max_retries: int = 30) -> List[dict]:
    """Execute Athena query and return results."""
    
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_BUCKET},
            WorkGroup='primary'
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Poll for completion
        for attempt in range(max_retries):
            exec_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = exec_response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                results_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
                rows = results_response['ResultSet']['Rows']
                headers = [col['Name'] for col in results_response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                
                results = []
                for row in rows[1:]:  # Skip header
                    result_dict = {}
                    for i, header in enumerate(headers):
                        value = row['Data'][i].get('VarCharValue', '')
                        if value:
                            try:
                                result_dict[header] = float(value)
                            except ValueError:
                                result_dict[header] = value
                        else:
                            result_dict[header] = None
                    results.append(result_dict)
                
                return results
            
            elif status in ['FAILED', 'CANCELLED']:
                error_msg = exec_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Query {status}: {error_msg}")
            
            import time
            time.sleep(1)
        
        raise Exception("Query timeout")
    
    except Exception as e:
        print(f"[ERROR] Athena query failed: {str(e)}")
        raise


def _get_schema_cache_item(key: str) -> Optional[dict]:
    """Get cached schema from DynamoDB."""
    
    try:
        table = dynamodb.Table(SCHEMA_CACHE_TABLE)
        response = table.get_item(Key={'cache_key': key})
        
        if 'Item' in response:
            item = response['Item']
            # Check if expired
            expiry = item.get('expiry', 0)
            if datetime.now().timestamp() < expiry:
                return item
        
        return None
    except Exception as e:
        print(f"[WARNING] Could not retrieve cache: {str(e)}")
        return None


def _convert_floats(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))

    if isinstance(obj, dict):
        return {k: _convert_floats(v) for k, v in obj.items()}    

    return obj

def _set_schema_cache_item(key: str, data: dict) -> None:
    """Cache schema in DynamoDB."""

    data = _convert_floats(data)
    
    try:
        table = dynamodb.Table(SCHEMA_CACHE_TABLE)
        expiry = datetime.now().timestamp() + SCHEMA_CACHE_TTL
        
        table.put_item(Item={
            'cache_key': key,
            'data': data,
            'expiry': Decimal(str(expiry)),
            'created_at': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"[WARNING] Could not cache schema: {str(e)}")