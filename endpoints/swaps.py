from fastapi import APIRouter, Path, HTTPException, Query
from typing import List, Dict, Any
import json
import logging

from db.connection import execute_query
from models.responses import SuccessResponse, ErrorResponse

# Configure logger
logger = logging.getLogger("api-swaps")

# Debug function to log SQL queries with their parameters
def debug_sql(query, params=None):
    if params:
        # Replace placeholders with their values for debugging
        param_idx = 0
        debug_query = query
        for param in params:
            if isinstance(param, str):
                replacement = f"'{param}'"
            else:
                replacement = str(param)
            debug_query = debug_query.replace("%s", replacement, 1)
            param_idx += 1
        logger.debug(f"Executing SQL: {debug_query}")
    else:
        logger.debug(f"Executing SQL: {query}")
    return query

# Router definition
router = APIRouter(
    prefix="/swaps",
    tags=["swaps"],
    responses={404: {"model": ErrorResponse}}
)

@router.get("", response_model=SuccessResponse)
async def get_recent_swaps(
    limit: int = Query(50, description="Number of swaps to return (default: 50)"),
    offset: int = Query(0, description="Pagination offset"),
    start_date: str = Query(None, description="Filter by start date (format: YYYY-MM-DD)"),
    end_date: str = Query(None, description="Filter by end date (format: YYYY-MM-DD)"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get the most recent swap transactions, limited to the specified number.
    
    - **limit**: Number of swaps to return (default: 50)
    - **offset**: Pagination offset
    - **start_date**: Filter by start date (format: YYYY-MM-DD)
    - **end_date**: Filter by end date (format: YYYY-MM-DD)
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: limit={limit}, offset={offset}, start_date={start_date}, end_date={end_date}")
    
    # Build the WHERE clause based on date filters
    where_clause = ""
    query_params = []
    
    if start_date:
        # Convert YYYY-MM-DD to Unix timestamp
        where_clause += " WHERE CAST(block_time AS BIGINT) >= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD'))"
        query_params.append(start_date)
        
    if end_date:
        if where_clause:
            where_clause += " AND CAST(block_time AS BIGINT) <= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD') + INTERVAL '1 day' - INTERVAL '1 second')"
        else:
            where_clause += " WHERE CAST(block_time AS BIGINT) <= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD') + INTERVAL '1 day' - INTERVAL '1 second')"
        query_params.append(end_date)
    
    # Count total swaps
    count_query = f"SELECT COUNT(*) FROM swaps{where_clause}"
    count_result = execute_query(
        debug_sql(count_query, query_params) if debug else count_query,
        query_params
    )
    total = count_result[0]["count"] if count_result else 0
    
    # Get recent swaps
    swaps_query = f"""
    SELECT tx_id, user_address, block_time, swap_details
    FROM swaps{where_clause}
    ORDER BY block_time DESC, tx_id
    LIMIT %s OFFSET %s
    """
    
    # Add limit and offset to params
    params = query_params + [limit, offset]
    swaps_result = execute_query(
        debug_sql(swaps_query, params) if debug else swaps_query,
        params
    )
    
    swaps_data = []
    if swaps_result:
        for row in swaps_result:
            swap_data = {
                "tx_id": row["tx_id"],
                "user_address": row["user_address"],
                "block_time": row["block_time"],
                "swap_details": row["swap_details"]
            }
            swaps_data.append(swap_data)
    
    return {
        "status": "success",
        "data": swaps_data,
        "total": total,
        "limit": limit,
        "offset": offset
    }

@router.get("/contract/{contract_principal}", response_model=SuccessResponse)
async def get_swaps_by_contract(
    contract_principal: str = Path(..., description="Contract principal to filter by"),
    user_address: str = Query(None, description="Optional user address to filter by"),
    limit: int = Query(50, description="Number of swaps to return (default: 50)"),
    offset: int = Query(0, description="Pagination offset"),
    start_date: str = Query(None, description="Filter by start date (format: YYYY-MM-DD)"),
    end_date: str = Query(None, description="Filter by end date (format: YYYY-MM-DD)"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get swap transactions containing the specified contract principal in swap_details.
    
    - **contract_principal**: Contract principal to filter by
    - **user_address**: Optional user address to filter by
    - **limit**: Number of swaps to return (default: 50)
    - **offset**: Pagination offset
    - **start_date**: Filter by start date (format: YYYY-MM-DD)
    - **end_date**: Filter by end date (format: YYYY-MM-DD)
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: contract_principal={contract_principal}, user_address={user_address}, " +
                    f"limit={limit}, offset={offset}, start_date={start_date}, end_date={end_date}")
    
    # Method 2: Text search across all JSONB (more flexible but potentially slower)
    # Use EXISTS and JSON path operations to check if any element in the array contains the contract_principal
    filter_condition = """
    (
        swap_details::text ILIKE %s
        OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(swap_details) as swap_item
            WHERE 
                swap_item->>'in_asset' LIKE %s
                OR swap_item->>'out_asset' LIKE %s
                OR swap_item->>'contract_address' LIKE %s
        )
    )
    """
    # Create search parameters for each field we want to check
    contract_like = f"%{contract_principal}%"
    
    # Build the WHERE clause based on filters
    where_clause = f"WHERE {filter_condition}"
    query_params = [contract_like, contract_like, contract_like, contract_like]
    
    # Add user_address filter if provided
    if user_address:
        where_clause += " AND user_address = %s"
        query_params.append(user_address)
    
    if start_date:
        where_clause += " AND CAST(block_time AS BIGINT) >= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD'))"
        query_params.append(start_date)
        
    if end_date:
        where_clause += " AND CAST(block_time AS BIGINT) <= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD') + INTERVAL '1 day' - INTERVAL '1 second')"
        query_params.append(end_date)
    
    # Count total swaps with this contract
    count_query = f"SELECT COUNT(*) FROM swaps {where_clause}"
    count_result = execute_query(
        debug_sql(count_query, query_params) if debug else count_query,
        query_params
    )
    total = count_result[0]["count"] if count_result else 0
    
    # Get filtered swaps
    swaps_query = f"""
    SELECT tx_id, user_address, block_time, swap_details
    FROM swaps
    {where_clause}
    ORDER BY block_time DESC, tx_id
    LIMIT %s OFFSET %s
    """
    
    # Add limit and offset to params
    params = query_params + [limit, offset]
    swaps_result = execute_query(
        debug_sql(swaps_query, params) if debug else swaps_query,
        params
    )
    
    swaps_data = []
    if swaps_result:
        for row in swaps_result:
            swap_data = {
                "tx_id": row["tx_id"],
                "user_address": row["user_address"],
                "block_time": row["block_time"],
                "swap_details": row["swap_details"]
            }
            swaps_data.append(swap_data)
    
    return {
        "status": "success",
        "data": swaps_data,
        "total": total,
        "limit": limit,
        "offset": offset
    }

@router.get("/user/{user_address}", response_model=SuccessResponse)
async def get_swaps_by_user(
    user_address: str = Path(..., description="User address to filter by"),
    limit: int = Query(50, description="Number of swaps to return (default: 50)"),
    offset: int = Query(0, description="Pagination offset"),
    start_date: str = Query(None, description="Filter by start date (format: YYYY-MM-DD)"),
    end_date: str = Query(None, description="Filter by end date (format: YYYY-MM-DD)"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get swap transactions for a specific user address.
    
    - **user_address**: User address to filter by
    - **limit**: Number of swaps to return (default: 50)
    - **offset**: Pagination offset
    - **start_date**: Filter by start date (format: YYYY-MM-DD)
    - **end_date**: Filter by end date (format: YYYY-MM-DD)
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: user_address={user_address}, limit={limit}, offset={offset}, start_date={start_date}, end_date={end_date}")
    
    # Build the WHERE clause based on filters
    where_clause = "WHERE user_address = %s"
    query_params = [user_address]
    
    if start_date:
        where_clause += " AND CAST(block_time AS BIGINT) >= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD'))"
        query_params.append(start_date)
        
    if end_date:
        where_clause += " AND CAST(block_time AS BIGINT) <= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD') + INTERVAL '1 day' - INTERVAL '1 second')"
        query_params.append(end_date)
    
    # Count total swaps for this user
    count_query = f"SELECT COUNT(*) FROM swaps {where_clause}"
    count_result = execute_query(
        debug_sql(count_query, query_params) if debug else count_query,
        query_params
    )
    total = count_result[0]["count"] if count_result else 0
    
    # Get user's swaps
    swaps_query = f"""
    SELECT tx_id, user_address, block_time, swap_details
    FROM swaps
    {where_clause}
    ORDER BY block_time DESC, tx_id
    LIMIT %s OFFSET %s
    """
    
    # Add limit and offset to params
    params = query_params + [limit, offset]
    swaps_result = execute_query(
        debug_sql(swaps_query, params) if debug else swaps_query,
        params
    )
    
    swaps_data = []
    if swaps_result:
        for row in swaps_result:
            swap_data = {
                "tx_id": row["tx_id"],
                "user_address": row["user_address"],
                "block_time": row["block_time"],
                "swap_details": row["swap_details"]
            }
            swaps_data.append(swap_data)
    
    return {
        "status": "success",
        "data": swaps_data,
        "total": total,
        "limit": limit,
        "offset": offset
    }

@router.get("/filter", response_model=SuccessResponse)
async def filter_swaps(
    token_x: str = Query(None, description="Filter by token_x in swap_details"),
    token_y: str = Query(None, description="Filter by token_y in swap_details"),
    min_amount: float = Query(None, description="Filter by minimum amount in swap_details"),
    max_amount: float = Query(None, description="Filter by maximum amount in swap_details"),
    limit: int = Query(50, description="Number of swaps to return (default: 50)"),
    offset: int = Query(0, description="Pagination offset"),
    start_date: str = Query(None, description="Filter by start date (format: YYYY-MM-DD)"),
    end_date: str = Query(None, description="Filter by end date (format: YYYY-MM-DD)"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Filter swap transactions based on details in the swap_details JSONB field.
    
    - **token_x**: Filter by token_x in swap_details
    - **token_y**: Filter by token_y in swap_details
    - **min_amount**: Filter by minimum amount in swap_details
    - **max_amount**: Filter by maximum amount in swap_details
    - **limit**: Number of swaps to return (default: 50)
    - **offset**: Pagination offset
    - **start_date**: Filter by start date (format: YYYY-MM-DD)
    - **end_date**: Filter by end date (format: YYYY-MM-DD)
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: token_x={token_x}, token_y={token_y}, min_amount={min_amount}, " +
                     f"max_amount={max_amount}, limit={limit}, offset={offset}, " +
                     f"start_date={start_date}, end_date={end_date}")
    
    # Build the WHERE clause based on filters
    where_conditions = []
    query_params = []
    
    # Date filters
    if start_date:
        where_conditions.append("CAST(block_time AS BIGINT) >= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD'))")
        query_params.append(start_date)
        
    if end_date:
        where_conditions.append("CAST(block_time AS BIGINT) <= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD') + INTERVAL '1 day' - INTERVAL '1 second')")
        query_params.append(end_date)
    
    # JSONB filters - adapt these based on your actual swap_details structure
    if token_x:
        # This assumes token_x is stored as a key in swap_details
        # Adjust the JSONB path according to your data structure
        where_conditions.append("(swap_details->>'token_x' = %s OR swap_details::text ILIKE %s)")
        query_params.append(token_x)
        query_params.append(f"%\"token_x\":\"{token_x}\"%")
    
    if token_y:
        # Similar approach for token_y
        where_conditions.append("(swap_details->>'token_y' = %s OR swap_details::text ILIKE %s)")
        query_params.append(token_y)
        query_params.append(f"%\"token_y\":\"{token_y}\"%")
    
    if min_amount is not None:
        # For numeric values in JSONB, we need to cast them to numeric
        # This assumes 'amount' is a key in your swap_details
        where_conditions.append("(swap_details->>'amount')::numeric >= %s")
        query_params.append(min_amount)
    
    if max_amount is not None:
        where_conditions.append("(swap_details->>'amount')::numeric <= %s")
        query_params.append(max_amount)
    
    # Combine all conditions
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    
    # Count total swaps matching filters
    count_query = f"SELECT COUNT(*) FROM swaps {where_clause}"
    count_result = execute_query(
        debug_sql(count_query, query_params) if debug else count_query,
        query_params
    )
    total = count_result[0]["count"] if count_result else 0
    
    # Get filtered swaps
    swaps_query = f"""
    SELECT tx_id, user_address, block_time, swap_details
    FROM swaps
    {where_clause}
    ORDER BY block_time DESC, tx_id
    LIMIT %s OFFSET %s
    """
    
    # Add limit and offset to params
    params = query_params + [limit, offset]
    swaps_result = execute_query(
        debug_sql(swaps_query, params) if debug else swaps_query,
        params
    )
    
    swaps_data = []
    if swaps_result:
        for row in swaps_result:
            swap_data = {
                "tx_id": row["tx_id"],
                "user_address": row["user_address"],
                "block_time": row["block_time"],
                "swap_details": row["swap_details"]
            }
            swaps_data.append(swap_data)
    
    return {
        "status": "success",
        "data": swaps_data,
        "total": total,
        "limit": limit,
        "offset": offset
    }

@router.get("/stats", response_model=SuccessResponse)
async def get_swap_stats(
    period: str = Query("day", description="Aggregation period (day, week, month)"),
    start_date: str = Query(None, description="Filter by start date (format: YYYY-MM-DD)"),
    end_date: str = Query(None, description="Filter by end date (format: YYYY-MM-DD)"),
    token: str = Query(None, description="Filter by specific token"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get statistical information about swap transactions.
    
    - **period**: Aggregation period (day, week, month)
    - **start_date**: Filter by start date (format: YYYY-MM-DD)
    - **end_date**: Filter by end date (format: YYYY-MM-DD)
    - **token**: Filter by specific token
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: period={period}, start_date={start_date}, end_date={end_date}, token={token}")
    
    # Determine date truncation based on period
    trunc_function = "day"  # Default
    if period == "week":
        trunc_function = "week"
    elif period == "month":
        trunc_function = "month"
    
    # Build the WHERE clause based on filters
    where_conditions = []
    query_params = []
    
    if start_date:
        where_conditions.append("CAST(block_time AS BIGINT) >= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD'))")
        query_params.append(start_date)
        
    if end_date:
        where_conditions.append("CAST(block_time AS BIGINT) <= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD') + INTERVAL '1 day' - INTERVAL '1 second')")
        query_params.append(end_date)
    
    if token:
        # Filter for swaps involving the specified token (in any position)
        where_conditions.append("swap_details::text ILIKE %s")
        query_params.append(f"%{token}%")
    
    # Combine all conditions
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    
    # Get stats by time period
    stats_query = f"""
    SELECT 
        date_trunc('{trunc_function}', TO_TIMESTAMP(CAST(block_time AS BIGINT))) as time_period,
        COUNT(*) as swap_count,
        COUNT(DISTINCT user_address) as unique_users
    FROM swaps
    {where_clause}
    GROUP BY time_period
    ORDER BY time_period DESC
    """
    
    stats_result = execute_query(
        debug_sql(stats_query, query_params) if debug else stats_query,
        query_params
    )
    
    # Get total stats
    total_query = f"""
    SELECT 
        COUNT(*) as total_swaps,
        COUNT(DISTINCT user_address) as total_unique_users,
        COUNT(DISTINCT tx_id) as total_transactions
    FROM swaps
    {where_clause}
    """
    
    total_result = execute_query(
        debug_sql(total_query, query_params) if debug else total_query,
        query_params
    )
    
    # Format and return results
    stats_data = []
    if stats_result:
        for row in stats_result:
            period_data = {
                "period": row["time_period"].strftime('%Y-%m-%d') if row["time_period"] else None,
                "swap_count": row["swap_count"],
                "unique_users": row["unique_users"]
            }
            stats_data.append(period_data)
    
    total_stats = {}
    if total_result and total_result[0]:
        total_stats = {
            "total_swaps": total_result[0]["total_swaps"],
            "total_unique_users": total_result[0]["total_unique_users"],
            "total_transactions": total_result[0]["total_transactions"]
        }
    
    return {
        "status": "success",
        "data": {
            "period_stats": stats_data,
            "total_stats": total_stats
        }
    }

@router.get("/address-contract", response_model=SuccessResponse)
async def get_swaps_by_address_and_contract(
    user_address: str = Query(None, description="User address to filter by"),
    contract_principal: str = Query(None, description="Contract principal to filter by"),
    limit: int = Query(50, description="Number of swaps to return (default: 50)"),
    offset: int = Query(0, description="Pagination offset"),
    start_date: str = Query(None, description="Filter by start date (format: YYYY-MM-DD)"),
    end_date: str = Query(None, description="Filter by end date (format: YYYY-MM-DD)"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get swap transactions with flexible filtering by address and/or contract.
    
    - **user_address**: User address to filter by (optional)
    - **contract_principal**: Contract principal to filter by (optional)
    - **limit**: Number of swaps to return (default: 50)
    - **offset**: Pagination offset
    - **start_date**: Filter by start date (format: YYYY-MM-DD)
    - **end_date**: Filter by end date (format: YYYY-MM-DD)
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: user_address={user_address}, contract_principal={contract_principal}, " +
                     f"limit={limit}, offset={offset}, start_date={start_date}, end_date={end_date}")
    
    # Build the WHERE clause based on filters
    where_conditions = []
    query_params = []
    
    # User address filter
    if user_address:
        where_conditions.append("user_address = %s")
        query_params.append(user_address)
    
    # Contract principal filter
    if contract_principal:
        where_conditions.append("""
        (
            swap_details::text ILIKE %s
            OR EXISTS (
                SELECT 1
                FROM jsonb_array_elements(swap_details) as swap_item
                WHERE 
                    swap_item->>'in_asset' LIKE %s
                    OR swap_item->>'out_asset' LIKE %s
                    OR swap_item->>'contract_address' LIKE %s
            )
        )
        """)
        contract_like = f"%{contract_principal}%"
        query_params.extend([contract_like, contract_like, contract_like, contract_like])
    
    # Date filters
    if start_date:
        where_conditions.append("CAST(block_time AS BIGINT) >= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD'))")
        query_params.append(start_date)
        
    if end_date:
        where_conditions.append("CAST(block_time AS BIGINT) <= EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD') + INTERVAL '1 day' - INTERVAL '1 second')")
        query_params.append(end_date)
    
    # Combine all conditions
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    
    # If no filters are provided, we add a small time filter to avoid returning all records
    if not where_conditions:
        where_clause = "WHERE block_time >= NOW() - INTERVAL '7 days'"
    
    # Count total swaps matching filters
    count_query = f"SELECT COUNT(*) FROM swaps {where_clause}"
    count_result = execute_query(
        debug_sql(count_query, query_params) if debug else count_query,
        query_params
    )
    total = count_result[0]["count"] if count_result else 0
    
    # Get filtered swaps
    swaps_query = f"""
    SELECT tx_id, user_address, block_time, swap_details
    FROM swaps
    {where_clause}
    ORDER BY block_time DESC, tx_id
    LIMIT %s OFFSET %s
    """
    
    # Add limit and offset to params
    params = query_params + [limit, offset]
    swaps_result = execute_query(
        debug_sql(swaps_query, params) if debug else swaps_query,
        params
    )
    
    swaps_data = []
    if swaps_result:
        for row in swaps_result:
            swap_data = {
                "tx_id": row["tx_id"],
                "user_address": row["user_address"],
                "block_time": row["block_time"],
                "swap_details": row["swap_details"]
            }
            swaps_data.append(swap_data)
    
    return {
        "status": "success",
        "data": swaps_data,
        "total": total,
        "limit": limit,
        "offset": offset
    } 