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
    Get transaction details by transaction ID.
    
    - **tx_id**: Transaction ID
    - **include_events**: If set to true, includes related events in the response
    """
    # Get transaction data without JOIN
    tx_query = """
    SELECT 
        tx_id, 
        block_height, 
        events_processed,
        (raw_data->>'block_time')::integer as block_time,
        raw_data->>'fee_rate' as fee_rate,
        raw_data->>'sender_address' as sender_address,
        raw_data->>'tx_type' as tx_type,
        CASE 
            WHEN raw_data->>'tx_type' = 'contract_call' THEN raw_data->'contract_call'->>'function_name'
            ELSE NULL
        END as function_name,
        COALESCE((raw_data->>'event_count')::integer, 0) as event_count,
        raw_data
    FROM transactions
    WHERE tx_id = %s
    """
    
    tx_result = execute_query(tx_query, (tx_id,))
    
    if not tx_result:
        raise HTTPException(status_code=404, detail=f"Transaction {tx_id} not found")
    
    transaction = dict(tx_result[0])
    
    # Get event count in a separate query
    event_count_query = "SELECT COUNT(*) as count FROM events WHERE tx_id = %s"
    event_count_result = execute_query(event_count_query, (tx_id,))
    
    if event_count_result:
        transaction['event_count'] = event_count_result[0]['count']
    
    # Initialize events
    events = []
    
    # Get events for this transaction if requested
    if include_events:
        events_query = """
        SELECT 
            id,
            event_index,
            event_type,
            contract_id,
            tx_id,
            block_height,
            raw_data
        FROM events
        WHERE tx_id = %s
        ORDER BY event_index
        """
        
        events = execute_query(events_query, (tx_id,))
        
        # Process raw_data for each event if needed
        for event in events:
            if isinstance(event.get('raw_data'), str):
                try:
                    event['raw_data'] = json.loads(event['raw_data'])
                except:
                    pass
    
    # Return both transaction and its events
    return SuccessResponse(
        data={
            "transaction": transaction,
            "events": events
        },
        meta={
            "events_count": len(events)
        }
    )


@router.get("", response_model=SuccessResponse)
async def list_transactions(
    block_height: int = Query(None, description="Filter by block height"),
    limit: int = Query(20, description="Pagination limit (default: 20)"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    List transactions with filtering and pagination.
    
    - **block_height**: Optional filter by block height
    - **limit**: Maximum number of records to return (default: 20)
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
    
    # Use a simpler query without JOIN to avoid timeout
    tx_query = """
    SELECT 
        tx_id, 
        block_height, 
        events_processed,
        (raw_data->>'block_time')::integer as block_time,
        raw_data->>'fee_rate' as fee_rate,
        raw_data->>'sender_address' as sender_address,
        raw_data->>'tx_type' as tx_type,
        CASE 
            WHEN raw_data->>'tx_type' = 'contract_call' THEN raw_data->'contract_call'->>'function_name'
            ELSE NULL
        END as function_name,
        COALESCE((raw_data->>'event_count')::integer, 0) as event_count
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
    
    # If we have results and not too many, we can fetch accurate event counts
    if tx_results and len(tx_results) <= 20:
        # Get tx_ids for all transactions
        tx_ids = [tx['tx_id'] for tx in tx_results]
        tx_ids_str = ','.join([f"'{tx_id}'" for tx_id in tx_ids])
        
        # Get event counts in a single query
        if tx_ids:
            event_count_query = f"""
            SELECT tx_id, COUNT(*) as count 
            FROM events 
            WHERE tx_id IN ({tx_ids_str})
            GROUP BY tx_id
            """
            event_counts = execute_query(event_count_query)
            
            # Convert to dictionary for easy lookup
            event_count_dict = {ec['tx_id']: ec['count'] for ec in event_counts}
            
            # Update event counts in results
            for tx in tx_results:
                tx_id = tx['tx_id']
                if tx_id in event_count_dict:
                    tx['event_count'] = event_count_dict[tx_id]
    
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
    limit: int = Query(20, description="Pagination limit (default: 20)"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    Get transactions by block height.
    
    - **block_height**: Block height
    - **limit**: Maximum number of records to return (default: 20)
    - **offset**: Pagination offset
    """
    # Get total count for the block
    count_query = "SELECT COUNT(*) as total FROM transactions WHERE block_height = %s"
    count_result = execute_query(count_query, (block_height,))
    total = count_result[0]['total'] if count_result else 0
    
    if total == 0:
        raise HTTPException(status_code=404, detail=f"No transactions found for block {block_height}")
    
    # Use a simpler query without JOIN to avoid timeout
    tx_query = """
    SELECT 
        tx_id, 
        block_height, 
        events_processed,
        (raw_data->>'block_time')::integer as block_time,
        raw_data->>'fee_rate' as fee_rate,
        raw_data->>'sender_address' as sender_address,
        raw_data->>'tx_type' as tx_type,
        CASE 
            WHEN raw_data->>'tx_type' = 'contract_call' THEN raw_data->'contract_call'->>'function_name'
            ELSE NULL
        END as function_name,
        COALESCE((raw_data->>'event_count')::integer, 0) as event_count
    FROM transactions
    WHERE block_height = %s
    ORDER BY tx_id
    LIMIT %s OFFSET %s
    """
    
    tx_results = execute_query(tx_query, (block_height, limit, offset))
    
    # If we have results and not too many, we can fetch accurate event counts
    if tx_results and len(tx_results) <= 20:
        # Get tx_ids for all transactions
        tx_ids = [tx['tx_id'] for tx in tx_results]
        tx_ids_str = ','.join([f"'{tx_id}'" for tx_id in tx_ids])
        
        # Get event counts in a single query
        if tx_ids:
            event_count_query = f"""
            SELECT tx_id, COUNT(*) as count 
            FROM events 
            WHERE tx_id IN ({tx_ids_str})
            GROUP BY tx_id
            """
            event_counts = execute_query(event_count_query)
            
            # Convert to dictionary for easy lookup
            event_count_dict = {ec['tx_id']: ec['count'] for ec in event_counts}
            
            # Update event counts in results
            for tx in tx_results:
                tx_id = tx['tx_id']
                if tx_id in event_count_dict:
                    tx['event_count'] = event_count_dict[tx_id]
    
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
    limit: int = Query(20, description="Pagination limit (default: 20)"),
    offset: int = Query(0, description="Pagination offset")
):
    """
    Get latest transactions by address.
    
    - **address**: Blockchain address (e.g., ST...)
    - **limit**: Maximum number of records to return (default: 20)
    - **offset**: Pagination offset
    """
    # Use a simpler query without JOIN to avoid timeout
    tx_query = """
    SELECT 
        tx_id, 
        block_height, 
        events_processed,
        (raw_data->>'block_time')::integer as block_time,
        raw_data->>'fee_rate' as fee_rate,
        raw_data->>'sender_address' as sender_address,
        raw_data->>'tx_type' as tx_type,
        CASE 
            WHEN raw_data->>'tx_type' = 'contract_call' THEN raw_data->'contract_call'->>'function_name'
            ELSE NULL
        END as function_name,
        COALESCE((raw_data->>'event_count')::integer, 0) as event_count
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
    
    # If we have results and not too many, we can fetch accurate event counts
    if tx_results and len(tx_results) <= 20:
        # Get tx_ids for all transactions
        tx_ids = [tx['tx_id'] for tx in tx_results]
        tx_ids_str = ','.join([f"'{tx_id}'" for tx_id in tx_ids])
        
        # Get event counts in a single query
        if tx_ids:
            event_count_query = f"""
            SELECT tx_id, COUNT(*) as count 
            FROM events 
            WHERE tx_id IN ({tx_ids_str})
            GROUP BY tx_id
            """
            event_counts = execute_query(event_count_query)
            
            # Convert to dictionary for easy lookup
            event_count_dict = {ec['tx_id']: ec['count'] for ec in event_counts}
            
            # Update event counts in results
            for tx in tx_results:
                tx_id = tx['tx_id']
                if tx_id in event_count_dict:
                    tx['event_count'] = event_count_dict[tx_id]
    
    return SuccessResponse(
        data=tx_results,
        meta={
            "address": address,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    ) 