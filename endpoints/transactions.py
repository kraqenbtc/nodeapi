from fastapi import APIRouter, Path, HTTPException, Query
from typing import List, Dict, Any
import json

from db.connection import execute_query
from models.responses import TransactionResponse, EventResponse, SuccessResponse, ErrorResponse

# Router definition
router = APIRouter(
    prefix="/transactions",
    tags=["transactions"],
    responses={404: {"model": ErrorResponse}}
)

@router.get("/{tx_id}", response_model=SuccessResponse)
async def get_transaction(
    tx_id: str = Path(..., description="Transaction ID"),
    include_events: bool = Query(True, description="Include events in response")
):
    """
    Get transaction details and its events by transaction ID.
    
    - **tx_id**: The transaction ID
    - **include_events**: If set to true, includes related events in the response
    """
    # Get transaction info
    tx_query = """
    SELECT tx_id, block_height, raw_data, events_processed, created_at
    FROM transactions
    WHERE tx_id = %s
    """
    tx_results = execute_query(tx_query, (tx_id,))
    
    if not tx_results:
        raise HTTPException(status_code=404, detail=f"Transaction {tx_id} not found")
    
    transaction = tx_results[0]
    
    # Convert JSON to dictionary if it's a string
    if isinstance(transaction['raw_data'], str):
        transaction['raw_data'] = json.loads(transaction['raw_data'])
    
    events = []
    
    # Get events if requested
    if include_events:
        events_query = """
        SELECT tx_id, event_index, event_type, event_data, created_at
        FROM events
        WHERE tx_id = %s
        ORDER BY event_index
        """
        events_results = execute_query(events_query, (tx_id,))
        
        for event in events_results:
            # Convert JSON to dictionary if it's a string
            if isinstance(event['event_data'], str):
                event['event_data'] = json.loads(event['event_data'])
            events.append(event)
    
    # Prepare response
    response_data = {
        **transaction,
        "events": events
    }
    
    return SuccessResponse(
        data=response_data,
        meta={
            "events_count": len(events)
        }
    )


@router.get("", response_model=SuccessResponse)
async def list_transactions(
    block_height: int = Query(None, description="Filter by block height"),
    limit: int = Query(50, description="Pagination limit"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    List transactions with filtering and pagination.
    
    - **block_height**: Optional filter by block height
    - **limit**: Maximum number of records to return
    - **offset**: Pagination offset
    """
    # Get total count
    count_query = "SELECT COUNT(*) as total FROM transactions"
    count_params = []
    
    # Add filter
    if block_height is not None:
        count_query += " WHERE block_height = %s"
        count_params.append(block_height)
    
    count_result = execute_query(count_query, tuple(count_params) if count_params else None)
    total = count_result[0]['total'] if count_result else 0
    
    # Query transactions
    tx_query = """
    SELECT tx_id, block_height, events_processed, created_at
    FROM transactions
    """
    
    tx_params = []
    
    # Add filter
    if block_height is not None:
        tx_query += " WHERE block_height = %s"
        tx_params.append(block_height)
    
    # Add sorting and pagination
    tx_query += " ORDER BY block_height DESC, tx_id LIMIT %s OFFSET %s"
    tx_params.extend([limit, offset])
    
    tx_results = execute_query(tx_query, tuple(tx_params))
    
    return SuccessResponse(
        data=tx_results,
        meta={
            "total": total,
            "limit": limit,
            "offset": offset
        }
    )


@router.get("/block/{block_height}", response_model=SuccessResponse)
async def get_transactions_by_block(
    block_height: int = Path(..., description="Block height"),
    limit: int = Query(50, description="Pagination limit"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    Get transactions by block height.
    
    - **block_height**: Block height
    - **limit**: Maximum number of records to return
    - **offset**: Pagination offset
    """
    # Get total count for the block
    count_query = "SELECT COUNT(*) as total FROM transactions WHERE block_height = %s"
    count_result = execute_query(count_query, (block_height,))
    total = count_result[0]['total'] if count_result else 0
    
    if total == 0:
        raise HTTPException(status_code=404, detail=f"No transactions found for block {block_height}")
    
    # Query transactions
    tx_query = """
    SELECT tx_id, block_height, events_processed, created_at
    FROM transactions
    WHERE block_height = %s
    ORDER BY tx_id
    LIMIT %s OFFSET %s
    """
    
    tx_results = execute_query(tx_query, (block_height, limit, offset))
    
    return SuccessResponse(
        data=tx_results,
        meta={
            "block_height": block_height,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    )


@router.get("/address/{address}", response_model=SuccessResponse)
async def get_transactions_by_address(
    address: str = Path(..., description="Blockchain address"),
    limit: int = Query(50, description="Pagination limit"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    Get latest transactions by address.
    
    - **address**: Blockchain address (e.g., ST...)
    - **limit**: Maximum number of records to return (default: 50)
    - **offset**: Pagination offset
    """
    # Sadece sender_address ile eşleşen işlemleri ara
    tx_query = """
    SELECT tx_id, block_height, events_processed, created_at
    FROM transactions
    WHERE raw_data->>'sender_address' = %s
    ORDER BY block_height DESC, tx_id
    LIMIT %s OFFSET %s
    """
    
    # Execute query with address parameters
    tx_results = execute_query(tx_query, (address, limit, offset))
    
    # Count total for pagination info
    count_query = """
    SELECT COUNT(*) as total
    FROM transactions
    WHERE raw_data->>'sender_address' = %s
    """
    
    count_result = execute_query(count_query, (address,))
    total = count_result[0]['total'] if count_result else 0
    
    if not tx_results:
        return SuccessResponse(
            data=[],
            meta={
                "address": address,
                "total": 0,
                "limit": limit,
                "offset": offset
            }
        )
    
    return SuccessResponse(
        data=tx_results,
        meta={
            "address": address,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    ) 