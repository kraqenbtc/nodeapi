# endpoints paketi 
from fastapi import APIRouter
from .blocks import router as blocks_router
from .debug import router as debug_router
from .transactions import router as transactions_router
from .tokens import router as tokens_router

# Create main API router
api_router = APIRouter()

# Include all sub-routers
api_router.include_router(blocks_router, prefix="/blocks", tags=["blocks"])
api_router.include_router(transactions_router, prefix="/transactions", tags=["transactions"])
api_router.include_router(tokens_router, tags=["tokens"])
api_router.include_router(debug_router, prefix="/debug", tags=["debug"])

__all__ = ["api_router"] 