from fastapi import APIRouter, Query
import logging

from db.connection import execute_query
from models.responses import SuccessResponse, ErrorResponse

# Configure logger
logger = logging.getLogger("api-prices")

# Router definition
router = APIRouter(
    prefix="/prices",
    tags=["prices"],
    responses={404: {"model": ErrorResponse}}
)

@router.get("", response_model=SuccessResponse)
async def get_prices(
    contract_principal: str = Query(None, description="Filter by specific contract principal"),
    limit: int = Query(1000, description="Number of price entries to return (default: 1000)"),
    offset: int = Query(0, description="Pagination offset"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get price information for tokens.
    
    - **contract_principal**: Optional filter by specific contract principal
    - **limit**: Number of price entries to return (default: 1000)
    - **offset**: Pagination offset
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: contract_principal={contract_principal}, limit={limit}, offset={offset}")
    
    # Build the WHERE clause based on filters
    where_clause = ""
    query_params = []
    
    if contract_principal:
        where_clause = "WHERE contract_principal = %s"
        query_params.append(contract_principal)
    
    # Count total prices
    count_query = f"SELECT COUNT(*) FROM wprices {where_clause}"
    count_result = execute_query(
        count_query,
        query_params,
        db_config={
            "host": "167.172.109.180",
            "port": "5432",
            "database": "defi_tracker_core",
            "user": "defi_tracker_user",
            "password": "corekraxel2234"
        }
    )
    total = count_result[0]["count"] if count_result else 0
    
    # Get prices
    prices_query = f"""
    SELECT contract_principal, price, tvl
    FROM wprices
    {where_clause}
    ORDER BY updated_at DESC
    LIMIT %s OFFSET %s
    """
    
    # Add limit and offset to params
    params = query_params + [limit, offset]
    prices_result = execute_query(
        prices_query,
        params,
        db_config={
            "host": "167.172.109.180",
            "port": "5432",
            "database": "defi_tracker_core",
            "user": "defi_tracker_user",
            "password": "corekraxel2234"
        }
    )
    
    prices_data = []
    if prices_result:
        for row in prices_result:
            price_data = {
                "contract_principal": row["contract_principal"],
                "price": row["price"],
                "tvl": row["tvl"]
            }
            prices_data.append(price_data)
    
    return {
        "status": "success",
        "data": prices_data,
        "total": total,
        "limit": limit,
        "offset": offset
    }

@router.get("/latest", response_model=SuccessResponse)
async def get_latest_prices(
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get the latest price information for all tokens.
    
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Request for latest prices")
    
    # Get latest price for each contract_principal
    prices_query = """
    WITH latest_prices AS (
        SELECT DISTINCT ON (contract_principal) 
            contract_principal, price, tvl, updated_at
        FROM wprices
        ORDER BY contract_principal, updated_at DESC
    )
    SELECT contract_principal, price, tvl
    FROM latest_prices
    ORDER BY contract_principal
    """
    
    prices_result = execute_query(
        prices_query,
        [],
        db_config={
            "host": "167.172.109.180",
            "port": "5432",
            "database": "defi_tracker_core",
            "user": "defi_tracker_user",
            "password": "corekraxel2234"
        }
    )
    
    prices_data = []
    if prices_result:
        for row in prices_result:
            price_data = {
                "contract_principal": row["contract_principal"],
                "price": row["price"],
                "tvl": row["tvl"]
            }
            prices_data.append(price_data)
    
    return {
        "status": "success",
        "data": prices_data
    }

@router.get("/{contract_principal}", response_model=SuccessResponse)
async def get_price_history(
    contract_principal: str,
    limit: int = Query(30, description="Number of price entries to return (default: 30)"),
    offset: int = Query(0, description="Pagination offset"),
    debug: bool = Query(False, description="Show debug info in logs")
):
    """
    Get price history for a specific contract principal.
    
    - **contract_principal**: Contract principal to get price history for
    - **limit**: Number of price entries to return (default: 30)
    - **offset**: Pagination offset
    - **debug**: Enable debug mode to see SQL queries in logs
    """
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Request params: contract_principal={contract_principal}, limit={limit}, offset={offset}")
    
    # Count total prices for this contract
    count_query = "SELECT COUNT(*) FROM wprices WHERE contract_principal = %s"
    count_result = execute_query(
        count_query,
        [contract_principal],
        db_config={
            "host": "167.172.109.180",
            "port": "5432",
            "database": "defi_tracker_core",
            "user": "defi_tracker_user",
            "password": "corekraxel2234"
        }
    )
    total = count_result[0]["count"] if count_result else 0
    
    # Get price history
    prices_query = """
    SELECT contract_principal, price, tvl, created_at
    FROM wprices
    WHERE contract_principal = %s
    ORDER BY created_at DESC
    LIMIT %s OFFSET %s
    """
    
    params = [contract_principal, limit, offset]
    prices_result = execute_query(
        prices_query,
        params,
        db_config={
            "host": "167.172.109.180",
            "port": "5432",
            "database": "defi_tracker_core",
            "user": "defi_tracker_user",
            "password": "corekraxel2234"
        }
    )
    
    prices_data = []
    if prices_result:
        for row in prices_result:
            price_data = {
                "contract_principal": row["contract_principal"],
                "price": row["price"],
                "tvl": row["tvl"],
                "timestamp": row["created_at"].isoformat() if row["created_at"] else None
            }
            prices_data.append(price_data)
    
    return {
        "status": "success",
        "data": prices_data,
        "total": total,
        "limit": limit,
        "offset": offset
    } 