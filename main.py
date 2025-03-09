import os
import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# Import endpoints
from endpoints.transactions import router as transactions_router
from models.responses import ErrorResponse

# Load .env file
load_dotenv()

# Create API application
app = FastAPI(
    title="Kraxel API",
    description="Kraxel blockchain data API",
    version="1.0.0",
)

# CORS settings
origins = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Error handler
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            message="Internal server error",
            detail=str(exc)
        ).dict(),
    )

# Root endpoint
@app.get("/")
async def root():
    return {
        "name": "Kraxel API",
        "version": "1.0.0",
        "description": "Blockchain data API"
    }

# Health check endpoint
@app.get("/health")
async def health():
    return {"status": "ok"}

# Add routers
app.include_router(transactions_router)

# Start application (if run directly)
if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 8000))
    debug = os.getenv("DEBUG", "False").lower() in ["true", "1", "t"]
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=debug
    ) 