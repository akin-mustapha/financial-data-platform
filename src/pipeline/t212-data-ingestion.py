import json
import base64
import logging
import time

from datetime import datetime, timezone
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


API_URL = "https://live.trading212.com/api/v0"

secret_name = "prod/financial-dataflow/trading212"
region_name = "eu-west-1"


_ENDPOINTS = {
    "account": {
        "endpoint": "equity/account/summary",
        "bucket_name": "financial-dataflow",
        "key": "data/bronze/trading212/account/",
        # Account summary returns a single flat object, not a list or a
        # paginated {"items": [...]} envelope.
        "response_shape": "object",
    },
    "positions": {
        "endpoint": "equity/positions",
        "bucket_name": "financial-dataflow",
        "key": "data/bronze/trading212/positions/",
        "response_shape": "list",
    },
    # "dividends": "equity/history/dividends",
    # "orders": "equity/history/orders",
    # "transactions": "equity/history/transactions",
}

session = boto3.session.Session()


def get_secret():
    try:
        client = session.client(
            service_name="secretsmanager",
            region_name=region_name,
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError as e:
        logger.error(
            "[SECRETS] Failed to retrieve credentials: %s",
            e
        )
        raise

    secret = json.loads(
        get_secret_value_response["SecretString"]
    )

    return (
        secret.get("T212_API_TOKEN"),
        secret.get("T212_SECRET_TOKEN")
    )


def fetch_endpoint(
    url: str,
    header,
    response_shape: str = "list",
) -> tuple[list[dict], float]:
    """
    Retrieve data from a Trading212 endpoint.

    response_shape tells us how to interpret the payload, since different
    endpoints return genuinely different shapes:
      - "list":   payload is already a JSON array (e.g. positions)
      - "object": payload is a single flat JSON object (e.g. account
                  summary) — wrap it in a list so downstream code always
                  works with list[dict], regardless of endpoint.
      - "items":  payload is a paginated envelope: {"items": [...], ...}
    """
    start = time.perf_counter()
    try:
        request = Request(
            url,
            headers=header,
            method="GET"
        )

        with urlopen(request, timeout=10) as response:
            payload = response.read().decode("utf-8")
            result = json.loads(payload)

        if isinstance(result, list):
            records = result
        elif response_shape == "object":
            records = [result]
        elif response_shape == "items":
            records = result.get("items", [])
        else:
            # Unexpected shape for what was declared — don't silently
            # return an empty list and hide the mismatch; surface it.
            raise ValueError(
                f"Unexpected response shape for endpoint (declared="
                f"{response_shape!r}, got type={type(result).__name__})"
            )

        duration = time.perf_counter() - start

        logger.info(
            "[API] Data retrieved successfully | "
            "records=%d | duration=%.2fs",
            len(records),
            duration
        )

        return records, duration

    except Exception as e:
        duration = time.perf_counter() - start
        logger.error(
            "[API] Failed to retrieve data from %s | "
            "duration=%.2fs | error=%s",
            url,
            duration,
            e
        )

        raise


def save_to_s3(
    data: list[dict],
    bucket_name: str,
    key: str
) -> float:
    """Save data to S3."""

    start = time.perf_counter()
    count_records = len(data)

    client = session.client(
        service_name="s3",
        region_name=region_name,
    )

    try:

        now = datetime.now(timezone.utc)

        data = [
            {
                **pos,
                "ingested_timestamp": str(now),
                "ingested_date": now.date().isoformat(),
            }
            for pos in data
        ]

        body = "\n".join(
            json.dumps(record)
            for record in data
        )

        logger.info(
            "[S3] Uploading data | "
            "records=%d | destination=s3://%s/%s",
            count_records,
            bucket_name,
            key
        )

        client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType="application/json"
        )

        duration = time.perf_counter() - start

        logger.info(
            "[S3] Data written successfully | "
            "records=%d | duration=%.2fs",
            count_records,
            duration
        )
        return duration

    except ClientError as e:
        duration = time.perf_counter() - start

        logger.error(
            "[S3] Upload failed | "
            "records=%d | duration=%.2fs | error=%s",
            count_records,
            duration,
            e
        )

        # Dead Letter
        client.put_object(
            Bucket=bucket_name,
            Key=f"dead-letters/{key}",
            Body=body,
            ContentType="application/json"
        )

        logger.error(
            "[S3] Data written to dead-letter location | "
            "destination=s3://%s/dead-letters/%s",
            bucket_name,
            key
        )
        raise


def lambda_handler(event, context):
    logger.info("=" * 60)
    logger.info("Trading212 Pipeline Execution")
    logger.info("=" * 60)

    pipeline_start = time.perf_counter()

    # ---------------------------------------------------------
    # Secrets
    # ---------------------------------------------------------

    logger.info("[SECRETS] Retrieving API credentials")

    API_TOKEN, SECRET_TOKEN = get_secret()

    logger.info("[SECRETS] Credentials retrieved successfully")

    # ---------------------------------------------------------
    # Trading212 API
    # ---------------------------------------------------------

    credentials = f"{API_TOKEN}:{SECRET_TOKEN}"

    token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    header = {"Authorization": f"Basic {token}"}

    # Per-endpoint metrics, keyed by endpoint name, so nothing gets
    # overwritten when there's more than one endpoint in _ENDPOINTS.
    endpoint_metrics = {}

    for k, v in _ENDPOINTS.items():

        logger.info("")

        endpoint = v.get("endpoint")
        bucket_name = v.get("bucket_name")
        response_shape = v.get("response_shape", "list")

        url = urljoin(f"{API_URL}/", endpoint)

        res, api_duration = fetch_endpoint(url, header, response_shape)

        record_count = len(res)

        # ---------------------------------------------------------
        # S3
        # ---------------------------------------------------------

        now = datetime.now(timezone.utc)

        # Bronze uses plain nested year/month/day folders, not Hive-style
        # partitions — bronze is never cataloged in Glue, so there's no
        # discovery benefit to key=value paths here, only for silver/gold.
        key = (
            f"{v.get('key')}"
            f"{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{k}_"
            f"{now.strftime('%Y%m%dT%H%M%S')}.json"
        )

        s3_duration = save_to_s3(
            res,
            bucket_name,
            key
        )

        endpoint_metrics[k] = {
            "records": record_count,
            "api_duration": api_duration,
            "s3_duration": s3_duration,
        }

    # ---------------------------------------------------------
    # Execution Summary
    # ---------------------------------------------------------

    total_duration = time.perf_counter() - pipeline_start
    total_records = sum(m["records"] for m in endpoint_metrics.values())

    logger.info("")
    logger.info("-" * 60)
    logger.info("Execution Summary")
    logger.info("-" * 60)

    for name, m in endpoint_metrics.items():
        logger.info(
            "%-20s API %4d records  %.2fs  |  S3 %.2fs",
            name,
            m["records"],
            m["api_duration"],
            m["s3_duration"],
        )

    logger.info("-" * 60)

    logger.info(
        "Total                        %4d records    %.2fs",
        total_records,
        total_duration
    )

    logger.info("=" * 60)
    logger.info("Trading212 Pipeline Completed Successfully")
    logger.info("=" * 60)

    return {
        "statusCode": 200,
        "records": total_records,
        "endpoints": endpoint_metrics,
        "duration_seconds": round(total_duration, 2)
    }