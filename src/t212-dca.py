import base64
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin
from uuid import uuid4

import awswrangler as wr
import boto3
from botocore.exceptions import ClientError
import httpx

# Logging Configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration Constants
DATABASE = "financials"
TRADE_LOG_TABLE_NAME = "trade_log"
SECRET_NAME = "prod/financial-dataflow/trading212"
DOMAIN = "https://live.trading212.com/api/v0/"
ENDPOINT = "equity/orders/market"

TICKER = "VWRPl_EQ"
EQUITY = 15
MAX_DRAWDOWN_THRESHOLD = -1.0
MIN_DRAWDOWN_THRESHOLD = -2.5


# ==========================================
# HELPER FUNCTIONS (WITH TRY/EXCEPT)
# ==========================================

def get_credentials(secret_name: str):
    """Fetch API tokens from AWS Secrets Manager."""
    try:
        logger.info("Fetching secrets: %s", secret_name)
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response.get("SecretString", "{}"))
        return secret.get("T212_API_TOKEN"), secret.get("T212_SECRET_TOKEN")
    except ClientError as e:
        logger.error("AWS Secrets Manager error: %s", e)
        return None, None
    except Exception as e:
        logger.error("Unexpected error fetching secrets: %s", e)
        return None, None


def get_latest_position(ticker: str, database: str):
    """Fetch latest position metrics from Athena."""
    sql = f"""
    SELECT daily_value_change_pct, current_value, avg_value_7d
    FROM fact_t212_positions
    WHERE ticker = '{ticker}'
    ORDER BY ingested_timestamp DESC
    LIMIT 1;
    """
    try:
        logger.info("Fetching position data for ticker: %s", ticker)
        df = wr.athena.read_sql_query(sql=sql, database=database)
        if df.empty:
            logger.warning("Athena query returned 0 rows for ticker: %s", ticker)
            return None

        return {
            "change_pct": float(df["daily_value_change_pct"].iloc[0]),
            "current_value": float(df["current_value"].iloc[0]),
            "avg_value_7d": float(df["avg_value_7d"].iloc[0]),
        }
    except Exception as e:
        logger.error("Athena query failed: %s", e)
        return None


def execute_market_order(domain: str, endpoint: str, ticker: str, equity: float, api_token: str, secret_token: str):
    """Trigger market order via Trading 212 API."""
    credentials = f"{api_token}:{secret_token}"
    token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    
    headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json"
    }
    payload = {"ticker": ticker, "value": equity}
    url = urljoin(domain, endpoint)

    try:
        logger.info("Placing market order — Asset: %s, Equity: %s", ticker, equity)
        with httpx.Client() as client:
            response = client.post(url, json=payload, headers=headers, timeout=10.0)
            response.raise_for_status()
            return response.json() if response.content else {"status": "success"}
    except httpx.HTTPError as e:
        logger.error("HTTP error during market order placement: %s", e)
        return None


def log_trade(table_name: str, ticker: str, value: float):
    """Record executed trade entry into DynamoDB."""
    trade = {
        "trade_id": str(uuid4()),
        "vendor": "trading-212",
        "asset": ticker,
        "value": value,
        "quantity": "",
        "trader": "T212 DCA Automation",
        "created_datetime": datetime.now(timezone.utc).isoformat()
    }
    try:
        logger.info("Writing trade log to DynamoDB table: %s", table_name)
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        table.put_item(Item=trade)
        return trade
    except Exception as e:
        logger.error("DynamoDB write failed: %s", e)
        return None


# ==========================================
# MAIN ORCHESTRATOR
# ==========================================

def main(event=None, context=None):
    """Core DCA logic execution."""
    # 1. Credentials
    api_token, secret_token = get_credentials(SECRET_NAME)
    if not api_token or not secret_token:
        return {"statusCode": 500, "body": "Failed to retrieve credentials"}

    # 2. Athena Position Data
    metrics = get_latest_position(TICKER, DATABASE)
    if not metrics:
        return {"statusCode": 500, "body": "Failed to retrieve position metrics"}

    change_pct = metrics["change_pct"]
    current_value = metrics["current_value"]
    avg_value_7d = metrics["avg_value_7d"]

    # 3. DCA Rule Checks
    if not (MIN_DRAWDOWN_THRESHOLD <= change_pct <= MAX_DRAWDOWN_THRESHOLD):
        logger.info("Drawdown %.2f%% outside [%.2f%%, %.2f%%]. Skipping buy.",
                    change_pct, MIN_DRAWDOWN_THRESHOLD, MAX_DRAWDOWN_THRESHOLD)
        return {"statusCode": 200, "body": "Drawdown condition not met"}

    if current_value >= avg_value_7d:
        logger.info("Current value (%.2f) >= 7d avg (%.2f). Skipping buy.", current_value, avg_value_7d)
        return {"statusCode": 200, "body": "7d Average condition not met"}

    # 4. Trigger Market Order
    order_result = execute_market_order(DOMAIN, ENDPOINT, TICKER, EQUITY, api_token, secret_token)
    if not order_result:
        return {"statusCode": 500, "body": "Market order execution failed"}

    # 5. Log Trade
    trade = log_trade(TRADE_LOG_TABLE_NAME, TICKER, EQUITY)
    if not trade:
        logger.warning("Order was placed, but failed to log trade to DynamoDB.")

    return {"statusCode": 200, "body": json.dumps({"status": "order_executed", "trade": trade})}


# ==========================================
# AWS LAMBDA ENTRYPOINT
# ==========================================

def lambda_handler(event, context):
    return main(event, context)