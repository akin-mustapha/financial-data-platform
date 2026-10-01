import base64
import json
import logging
import ssl
from datetime import datetime, timezone
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from uuid import uuid4
import awswrangler as wr
import boto3
from botocore.exceptions import ClientError

# Attempt to load certifi for local SSL certificate verification (Mac fix)
try:
    import certifi

    ssl_context = ssl.create_default_context(
        cafile=certifi.where()
    )
except ImportError:
    ssl_context = ssl._create_unverified_context()

# Logging Configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration Constants
region_name = "eu-west-1"
SECRET_NAME = "prod/financial/t212-dca-automation"

DATABASE = "financials"
TRADE_LOG_TABLE_NAME = "trade_log"

DOMAIN = "https://live.trading212.com/api/v0/"
ENDPOINT = "equity/orders/market"

TICKER = "VWRPl_EQ"
EQUITY = 5
MAX_DRAWDOWN_THRESHOLD = -1.0
MIN_DRAWDOWN_THRESHOLD = -2.5

NOTIFICATION_EMAIL = "akinkunmimustapha1@gmail.com"

session = boto3.session.Session()

# ==========================================
# HELPER FUNCTIONS
# ==========================================


def get_secret(secret_id: str):
    """Fetch API tokens from AWS Secrets Manager."""
    logger.info(
        "[get_secret] Retrieving API secret_id: %s",
        secret_id,
        extra={
            "event": "secrets_retrieval",
            "secret_id": secret_id,
            "function": get_secret.__name__,
        },
    )

    try:

        client = session.client(
            service_name="secretsmanager",
            region_name=region_name,
        )

        response = client.get_secret_value(
            SecretId=secret_id
        )

        logger.info(
            "[get_secret] Secrets retrieved successfully"
        )

    except ClientError as e:
        logger.error(
            "[get_secret] AWS Secrets Manager error: %s", e
        )
        raise

    except Exception as e:
        logger.error(
            "[get_secret] Failed to retrieve credentials: %s",
            e,
        )
        raise

    secret = json.loads(response["SecretString"])

    return (
        secret.get("T212_API_TOKEN"),
        secret.get("T212_SECRET_TOKEN"),
    )


def get_latest_position(ticker: str, database: str):
    """Fetch latest position metrics and current price from Athena."""

    logger.info(
        "[get_latest_position] Fetching position data for ticker: %s",
        ticker,
    )

    sql = f"""
    SELECT daily_value_change_pct, current_value, avg_value_7d, current_price
    FROM fact_t212_positions
    WHERE ticker = '{ticker}'
    ORDER BY ingested_timestamp DESC
    LIMIT 1;
    """

    try:
        df = wr.athena.read_sql_query(
            sql=sql,
            database=database,
            s3_output="s3://financial-dataflow/query-results/",
        )
        if df.empty:
            logger.warning(
                "Athena query returned 0 rows for ticker: %s",
                ticker,
            )
            return None

        # Extract current_price column if available in fact table
        current_price = (
            float(df["current_price"].iloc[0])
            if "current_price" in df.columns
            else None
        )

        logger.info(
            "[get_latest_position] Fetched position data for ticker: %s",
            ticker,
        )

        return {
            "change_pct": float(
                df["daily_value_change_pct"].iloc[0]
            ),
            "current_value": float(
                df["current_value"].iloc[0]
            ),
            "avg_value_7d": float(
                df["avg_value_7d"].iloc[0]
            ),
            "current_price": current_price,
        }
    except Exception as e:
        logger.error(
            "[get_latest_position] Athena query failed: %s",
            e,
        )
        return None


def execute_market_order(
    domain: str,
    endpoint: str,
    api_token: str,
    secret_token: str,
    payload: dict,
):
    """Trigger market order via Trading 212 API using urllib."""

    logger.info(
        "[execute_market_order] Executing market order: %s",
        endpoint,
        extra={"domain": domain, "endpoint": endpoint},
    )

    credentials = f"{api_token}:{secret_token}"
    token = base64.b64encode(
        credentials.encode("utf-8")
    ).decode("utf-8")

    headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    url = urljoin(domain, endpoint)
    json_data = json.dumps(payload).encode("utf-8")

    try:

        request = Request(
            url=url,
            data=json_data,
            headers=headers,
            method="POST",
        )

        with urlopen(
            request, timeout=10, context=ssl_context
        ) as response:

            res_body = response.read().decode("utf-8")

            logger.info(
                "[execute_market_order] SUCCESS",
                extra={
                    "domain": domain,
                    "endpoint": endpoint,
                },
            )

            return (
                json.loads(res_body)
                if res_body
                else {"status": "success"}
            )

    except HTTPError as e:
        error_body = e.read().decode(
            "utf-8", errors="replace"
        )

        logger.error(
            "[execute_market_order] Trading 212 HTTP %s: %s",
            e.code,
            error_body,
        )

        return None

    except URLError as e:
        logger.error(
            "[execute_market_order] Network error: %s", e
        )

        return None

    except Exception:
        logger.exception(
            "[execute_market_order] Unexpected error placing market order"
        )

        return None


def log_trade(
    table_name: str,
    ticker: str,
    value: float,
    quantity: float,
    order_result,
):
    """Record executed trade entry with calculated quantity into DynamoDB."""

    logger.info(
        "[log_trade] Loggin Trade, table: %s",
        table_name,
    )

    trade = {
        "trade_id": str(uuid4()),
        "vendor": "trading-212",
        "trader": "T212 DCA Automation",
        "asset": ticker,
        "value": value,
        "quantity": (
            str(round(quantity, 6)) if quantity else ""
        ),
        "additional_info": json.dumps(order_result),
        "created_datetime": datetime.now(
            timezone.utc
        ).isoformat(),
    }
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        table.put_item(Item=trade)
        return trade
    except Exception as e:
        logger.error(
            "[log_trade] DynamoDB write failed: %s", e
        )
        return None


def send_notification(
    recipient: str,
    ticker: str,
    equity: float,
    quantity: float,
    change_pct: float,
    current_price: float,
    order_result: dict,
):
    """Send an email notification after a successful market order."""
    logger.info(
        f"[{send_notification.__name__}] Sending trade notification to: %s",
        recipient,
    )

    ses_client = boto3.client(
        "ses",
        region_name=region_name,
    )

    order_id = order_result.get("id", "N/A")

    subject = f"T212 DCA Order Executed: {ticker}"

    body = f"""
Trading 212 DCA order executed successfully.

Asset: {ticker}
Investment: €{equity:.2f}
Quantity: {quantity:.6f}
Price used for calculation: €{current_price:.4f}
Daily change: {change_pct:.2f}%

Trading 212 Order ID: {order_id}

Order response:
{json.dumps(order_result, indent=2)}
"""

    try:

        response = ses_client.send_email(
            Source=NOTIFICATION_EMAIL,
            Destination={
                "ToAddresses": [recipient],
            },
            Message={
                "Subject": {
                    "Data": subject,
                    "Charset": "UTF-8",
                },
                "Body": {
                    "Text": {
                        "Data": body,
                        "Charset": "UTF-8",
                    }
                },
            },
        )

        logger.info(
            f"[{send_notification.__name__}] Notification sent successfully. SES MessageId: %s",
            response.get("MessageId"),
        )

        return response

    except ClientError as e:
        logger.error(
            f"[{send_notification.__name__}] SES notification failed: %s",
            e,
        )
        return None

    except Exception:
        logger.exception(
            f"[{send_notification.__name__}] Unexpected error sending notification"
        )
        return None


# ==========================================
# MAIN ORCHESTRATOR
# ==========================================


def main(event=None, context=None):
    """Core DCA logic execution."""

    logger.info("=" * 60)
    logger.info("Trading212 DCA Execution")
    logger.info("=" * 60)

    # 1. Credentials
    api_token, secret_token = get_secret(SECRET_NAME)
    if not api_token:
        return {
            "statusCode": 500,
            "body": "Failed to retrieve credentials",
        }

    # 2. Athena Position Data
    metrics = get_latest_position(TICKER, DATABASE)
    if not metrics:
        return {
            "statusCode": 500,
            "body": "Failed to retrieve position metrics",
        }

    change_pct = metrics["change_pct"]
    current_value = metrics["current_value"]
    avg_value_7d = metrics["avg_value_7d"]
    current_price = metrics.get("current_price")

    # Calculate expected quantity: Quantity = Equity Amount / Current Price
    calculated_quantity = (
        (EQUITY / current_price)
        if (current_price and current_price > 0)
        else 0.0
    )
    logger.info(
        "Calculated quantity: %s", calculated_quantity
    )

    # 3. DCA Rule Checks
    if not (
        MIN_DRAWDOWN_THRESHOLD
        <= change_pct
        <= MAX_DRAWDOWN_THRESHOLD
    ):
        logger.info(
            "Drawdown %.2f%% outside [%.2f%%, %.2f%%]. Skipping buy.",
            change_pct,
            MIN_DRAWDOWN_THRESHOLD,
            MAX_DRAWDOWN_THRESHOLD,
        )
        return {
            "statusCode": 200,
            "body": "Drawdown condition not met",
        }

    if current_value >= avg_value_7d:
        logger.info(
            "Current value (%.2f) >= 7d avg (%.2f). Skipping buy.",
            current_value,
            avg_value_7d,
        )
        return {
            "statusCode": 200,
            "body": "7d Average condition not met",
        }

    # 4. Prepare & Trigger Market Order
    payload = {
        "ticker": TICKER,
        "quantity": round(calculated_quantity, 4),
        "extendedHours": False,
    }

    order_result = execute_market_order(
        DOMAIN, ENDPOINT, api_token, secret_token, payload
    )

    if not order_result:
        return {
            "statusCode": 500,
            "body": "Market order execution failed",
        }

    # 5. Log Trade
    trade = log_trade(
        TRADE_LOG_TABLE_NAME,
        TICKER,
        EQUITY,
        calculated_quantity,
        order_result,
    )

    if not trade:
        logger.warning(
            "Order was placed, but failed to log trade to DynamoDB."
        )

    # 6. Send notification
    notification = send_notification(
        recipient=NOTIFICATION_EMAIL,
        ticker=TICKER,
        equity=EQUITY,
        quantity=calculated_quantity,
        change_pct=change_pct,
        current_price=current_price,
        order_result=order_result,
    )

    if not notification:
        logger.warning(
            "Trade was executed, but notification failed."
        )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "status": "order_executed",
                "trade": trade,
                "notification_sent": notification
                is not None,
            }
        ),
    }


# ==========================================
# AWS LAMBDA ENTRYPOINT
# ==========================================


def lambda_handler(event, context):
    return main(event, context)


if __name__ == "__main__":
    print("Starting local execution...")
    result = main()
    print("Execution Result:", result)
