import base64
import json
import logging
import ssl
from datetime import datetime, timezone
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from uuid import uuid4

import awswrangler as wr
import boto3
from botocore.exceptions import ClientError

# Attempt to load certifi for local SSL certificate verification (Mac fix)
try:
    import certifi
    ssl_context = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    ssl_context = ssl._create_unverified_context()

# Logging Configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration Constants
DATABASE = "financials"
TRADE_LOG_TABLE_NAME = "trade_log"
SECRET_NAME = "prod/financial/t212-dca-automation"
DOMAIN = "https://live.trading212.com/api/v0/"
ENDPOINT = "equity/orders/market"

TICKER = "VWRPl_EQ"
EQUITY = 10
MAX_DRAWDOWN_THRESHOLD = -1.0
MIN_DRAWDOWN_THRESHOLD = -2.5


# ==========================================
# HELPER FUNCTIONS
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
    """Fetch latest position metrics and current price from Athena."""
    sql = f"""
    SELECT daily_value_change_pct, current_value, avg_value_7d, current_price
    FROM fact_t212_positions
    WHERE ticker = '{ticker}'
    ORDER BY ingested_timestamp DESC
    LIMIT 1;
    """
    try:
        logger.info("Fetching position data for ticker: %s", ticker)
        df = wr.athena.read_sql_query(sql=sql, database=database, s3_output="s3://financial-dataflow/query-results/")
        if df.empty:
            logger.warning("Athena query returned 0 rows for ticker: %s", ticker)
            return None

        # Extract current_price column if available in fact table
        current_price = float(df["current_price"].iloc[0]) if "current_price" in df.columns else None

        return {
            "change_pct": float(df["daily_value_change_pct"].iloc[0]),
            "current_value": float(df["current_value"].iloc[0]),
            "avg_value_7d": float(df["avg_value_7d"].iloc[0]),
            "current_price": current_price
        }
    except Exception as e:
        logger.error("Athena query failed: %s", e)
        return None


def execute_market_order(domain: str, endpoint: str, api_token: str, secret_token:str, payload: dict):
    """Trigger market order via Trading 212 API using urllib."""
    # headers = {
    #     "Authorization": api_token,
    #     "Content-Type": "application/json"
    # }
    credentials = f"{api_token}:{secret_token}"
    token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    
    headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json"
    }
    url = urljoin(domain, endpoint)
    json_data = json.dumps(payload).encode("utf-8")

    try:
        logger.info("Placing market order — Payload: %s", payload)
        request = Request(
            url,
            data=json_data,
            headers=headers,
            method="POST"
        )

        with urlopen(request, timeout=10, context=ssl_context) as response:
            res_body = response.read().decode("utf-8")
            result = json.loads(res_body) if res_body else {"status": "success"}
            logger.info("Order response: %s", result)
            return result
    except Exception as e:
        logger.error("HTTP error during market order placement: %s", e)
        return None


def log_trade(table_name: str, ticker: str, value: float, quantity: float):
    """Record executed trade entry with calculated quantity into DynamoDB."""
    trade = {
        "trade_id": str(uuid4()),
        "vendor": "trading-212",
        "asset": ticker,
        "value": value,
        "quantity": str(round(quantity, 6)) if quantity else "",
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
    if not api_token:
        return {"statusCode": 500, "body": "Failed to retrieve credentials"}

    # 2. Athena Position Data
    metrics = get_latest_position(TICKER, DATABASE)
    if not metrics:
        return {"statusCode": 500, "body": "Failed to retrieve position metrics"}

    change_pct = metrics["change_pct"]
    current_value = metrics["current_value"]
    avg_value_7d = metrics["avg_value_7d"]
    current_price = metrics.get("current_price")

    # Calculate expected quantity: Quantity = Equity Amount / Current Price
    calculated_quantity = (EQUITY / current_price) if (current_price and current_price > 0) else 0.0
    logger.info("Calculated quantity: %s", calculated_quantity)

    # 3. DCA Rule Checks
    # if not (MIN_DRAWDOWN_THRESHOLD <= change_pct <= MAX_DRAWDOWN_THRESHOLD):
    #     logger.info("Drawdown %.2f%% outside [%.2f%%, %.2f%%]. Skipping buy.",
    #                 change_pct, MIN_DRAWDOWN_THRESHOLD, MAX_DRAWDOWN_THRESHOLD)
    #     return {"statusCode": 200, "body": "Drawdown condition not met"}

    # if current_value >= avg_value_7d:
    #     logger.info("Current value (%.2f) >= 7d avg (%.2f). Skipping buy.", current_value, avg_value_7d)
    #     return {"statusCode": 200, "body": "7d Average condition not met"}

    # 4. Prepare & Trigger Market Order
    payload = {
        "ticker": TICKER,
        "quantity": round(calculated_quantity, 6),
        "extendedHours": False
    }
    
    order_result = execute_market_order(DOMAIN, ENDPOINT, api_token, secret_token, payload)
    if not order_result:
        return {"statusCode": 500, "body": "Market order execution failed"}

    # 5. Log Trade
    trade = log_trade(TRADE_LOG_TABLE_NAME, TICKER, EQUITY, calculated_quantity)
    if not trade:
        logger.warning("Order was placed, but failed to log trade to DynamoDB.")

    return {"statusCode": 200, "body": json.dumps({"status": "order_executed", "trade": trade})}


# ==========================================
# AWS LAMBDA ENTRYPOINT
# ==========================================

def lambda_handler(event, context):
    return main(event, context)


if __name__ == "__main__":
    print("Starting local execution...")
    result = main()
    print("Execution Result:", result)