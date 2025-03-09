# Kraxel API

Kraxel blockchain data API. This API provides HTTP endpoints for querying and viewing blockchain data.

## Features

- View transaction details and events by transaction ID
- List transactions with filtering and pagination
- View transactions by block height
- View latest transactions by blockchain address

## Setup

### Requirements

- Python 3.8+
- PostgreSQL

### Steps

1. Clone the repository:
```bash
git clone [repo_url]
cd kraxel-api
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# OR
venv\Scripts\activate  # Windows
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Edit the `.env` file:
```bash
cp .env.example .env
# Edit the .env file with your settings
```

## Running

```bash
python main.py
```

The API will run at http://localhost:8000 by default.

You can access the Swagger documentation at http://localhost:8000/docs.

## API Endpoints

### Transactions

- `GET /transactions/{tx_id}` - Get transaction details and events by ID
- `GET /transactions` - List transactions with filtering and pagination
- `GET /transactions/block/{block_height}` - Get transactions by block height
- `GET /transactions/address/{address}` - Get latest transactions by blockchain address

### Health Check

- `GET /health` - Check if the API is running properly

## Example Usage

### Query a transaction

```bash
curl -X GET "http://localhost:8000/transactions/0x12345abcdef" -H "accept: application/json"
```

### List transactions

```bash
curl -X GET "http://localhost:8000/transactions?limit=10&offset=0" -H "accept: application/json"
```

### Query transactions by block

```bash
curl -X GET "http://localhost:8000/transactions/block/12345" -H "accept: application/json"
```

### Query transactions by address

```bash
curl -X GET "http://localhost:8000/transactions/address/ST1PQHQKV0RJXZFY1DGX8MNSNYVE3VGZJSRTPGZGM" -H "accept: application/json"
``` 