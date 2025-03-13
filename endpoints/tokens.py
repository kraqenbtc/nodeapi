from fastapi import APIRouter, Path, HTTPException, Query
from typing import List, Dict, Any, Optional

from db.connection import execute_query
from models.responses import TokenResponse, SuccessResponse, ErrorResponse

# Router definition
router = APIRouter(
    prefix="/tokens",
    tags=["tokens"],
    responses={404: {"model": ErrorResponse}}
)

@router.get("", response_model=SuccessResponse)
async def list_tokens(
    limit: int = Query(20, description="Pagination limit (default: 20)"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    List all tokens with pagination.
    
    - **limit**: Maximum number of records to return (default: 20)
    - **offset**: Pagination offset
    """
    # Get total count
    count_query = "SELECT COUNT(*) as total FROM tokens"
    count_result = execute_query(count_query)
    total = count_result[0]['total'] if count_result else 0
    
    # Query tokens
    tokens_query = """
    SELECT 
        contract_principal,
        asset_identifier,
        name,
        symbol,
        image_uri,
        decimals_from_contract,
        total_supply_from_contract
    FROM tokens
    ORDER BY symbol, name
    LIMIT %s OFFSET %s
    """
    
    tokens_results = execute_query(tokens_query, (limit, offset))
    
    return SuccessResponse(
        data=tokens_results,
        meta={
            "total": total,
            "limit": limit,
            "offset": offset
        }
    )

@router.get("/{contract_principal}", response_model=SuccessResponse)
async def get_token(
    contract_principal: str = Path(..., description="Contract principal")
):
    """
    Get token details by contract principal.
    
    - **contract_principal**: The token contract principal
    """
    # Get token info
    token_query = """
    SELECT 
        contract_principal,
        asset_identifier,
        name,
        symbol,
        image_uri,
        decimals_from_contract,
        total_supply_from_contract
    FROM tokens
    WHERE contract_principal = %s
    """
    token_results = execute_query(token_query, (contract_principal,))
    
    if not token_results:
        raise HTTPException(status_code=404, detail=f"Token with contract principal {contract_principal} not found")
    
    token = token_results[0]
    
    return SuccessResponse(
        data=token,
        meta={}
    ) 