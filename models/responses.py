from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class EventResponse(BaseModel):
    """Event response model"""
    tx_id: str
    event_index: int
    event_type: str
    event_data: Dict[str, Any]
    created_at: Optional[datetime] = None

class TransactionResponse(BaseModel):
    """Transaction response model"""
    tx_id: str
    block_height: int
    raw_data: Dict[str, Any]
    events_processed: bool
    block_time: Optional[int] = None  # Unix timestamp for block_time
    fee_rate: Optional[str] = None  # Fee rate from transaction
    event_count: Optional[int] = 0  # Number of events
    sender_address: Optional[str] = None  # Sender address
    tx_type: Optional[str] = None  # Transaction type (contract_call, token_transfer, etc)
    function_name: Optional[str] = None  # Function name for contract_call transactions
    events: Optional[List[EventResponse]] = []

class ErrorResponse(BaseModel):
    """Error response model"""
    status: str = "error"
    message: str
    detail: Optional[str] = None

class SuccessResponse(BaseModel):
    """Success response model"""
    status: str = "success"
    data: Any
    meta: Optional[Dict[str, Any]] = Field(default_factory=dict)

class TokenResponse(BaseModel):
    """Token response model"""
    contract_principal: str
    asset_identifier: Optional[str] = None
    name: Optional[str] = None
    symbol: Optional[str] = None
    image_uri: Optional[str] = None
    decimals_from_contract: Optional[float] = None
    total_supply_from_contract: Optional[float] = None 