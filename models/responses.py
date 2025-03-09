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
    created_at: Optional[datetime] = None
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