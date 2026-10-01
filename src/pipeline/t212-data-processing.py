"""
Glue Python Shell job: Trading212 positions, silver -> gold.

Builds a small star schema in the gold zone:
  - fact_positions: one row per position per ingestion snapshot
                     (measures + FKs to dim_asset/dim_date), plus
                     unrealized_profit_loss_pct computed once here so
                     every downstream query doesn't re-derive it
  - dim_asset:       one row per ticker, sourced from the static asset
                     mapping file (SCD Type 1 -- always overwritten with
                     the latest mapping, no history kept), plus a
                     derived is_etf flag
  - dim_date:        standard calendar dimension, one row per day

Incremental with watermarking + explicit backfill, matching the
bronze -> silver job's conventions. Only fact_positions is
date-partitioned; both dimensions are small (tens of rows for
dim_asset, one row per calendar day for dim_date) and are rewritten
in full on every run rather than partitioned or appended.

Backfill: pass --START_DATE (YYYY-MM-DD) as a job parameter to
reprocess a historical date range. --END_DATE is optional and
defaults to today if omitted. Backfill runs do NOT move the
watermark.

Job setup (Python Shell, not Spark):
- Python version: 3.9
- Job parameters:
    --JOB_NAME                   financial-dataflow-silver-to-gold
    --additional-python-modules  awswrangler==3.*,pandas,pyarrow
- Max capacity: 0.0625 or 1 DPU is plenty at this data volume.
"""

import json
import logging
from datetime import date, timedelta, datetime, timezone
from typing import List, Optional

import boto3
import pandas as pd
import awswrangler as wr

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
INPUT_PATH = "s3://financial-dataflow/data/silver/trading212/positions/"  # ingested_date=YYYY-MM-DD/ partitions
STATE_PATH = "s3://financial-dataflow/data/gold/_state/watermark.json"
MAPPING_BUCKET = "financial-dataflow"
MAPPING_KEY = "resources/asset_mapping.json"
GLUE_DATABASE = "financials"

FACT_TABLE = "fact_t212_positions"
FACT_PATH = "s3://financial-dataflow/data/gold/fact_t212_positions/"

DIM_ASSET_TABLE = "dim_t212_asset"
DIM_ASSET_PATH = "s3://financial-dataflow/data/gold/dim_t212_asset/"

DIM_DATE_TABLE = "dim_date"
DIM_DATE_PATH = "s3://financial-dataflow/data/gold/dim_date/"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ---------------------------------------------------------------------
# Watermark helpers (same pattern as bronze -> silver, separate state file)
# ---------------------------------------------------------------------
def get_watermark() -> Optional[date]:
    try:
        state = wr.s3.read_json(STATE_PATH, lines=False)
        return pd.to_datetime(state["last_processed_date"].iloc[0]).date()
    except Exception:
        logger.info(
            "No watermark found at %s — treating this as the first run", STATE_PATH
        )
        return None


def set_watermark(new_date: date) -> None:
    wr.s3.to_json(
        df=pd.DataFrame([{"last_processed_date": new_date.isoformat()}]),
        path=STATE_PATH,
    )
    logger.info("Watermark advanced to %s", new_date)


def dates_to_process(from_date, to_date, is_backfill) -> List[date]:
    if is_backfill:
        start = pd.to_datetime(from_date).date()
        end = pd.to_datetime(to_date).date() if to_date else date.today()
        logger.info(
            "Backfill mode: %s to %s (watermark will NOT be updated)", start, end
        )
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]

    # today is always included and reprocessed, even if the watermark
    # already covers it -- the Lambda ingests up to three times a day
    # (open/midday/close, see scheduler.tf), so today's fact_positions
    # row isn't final until the day's last run; each run recomputes it
    # from whatever silver snapshots exist so far (collapse_to_daily),
    # and overwrite_partitions replaces the partition each time.
    watermark = get_watermark()
    today = date.today()
    start = today if watermark is None else min(watermark + timedelta(days=1), today)
    return [start + timedelta(days=i) for i in range((today - start).days + 1)]


# ---------------------------------------------------------------------
# Read silver
# ---------------------------------------------------------------------
def read_silver(dates: List[date]) -> pd.DataFrame:
    """Read silver Parquet for the given ingested_date partitions."""
    target_dates = {d.isoformat() for d in dates}
    logger.info("Reading silver partitions for dates: %s", sorted(target_dates))
    try:
        df = wr.s3.read_parquet(
            path=INPUT_PATH,
            dataset=True,
            partition_filter=lambda part: part.get("ingested_date") in target_dates,
        )
    except wr.exceptions.NoFilesFound:
        logger.warning("No silver data found for target dates")
        return pd.DataFrame()
    logger.info("Silver row count for this run: %d", len(df))
    return df


def read_silver_lookback_day(before: date) -> pd.DataFrame:
    """Read the single silver partition immediately preceding the run's
    target dates, so per-ticker day-over-day deltas (build_fact_positions)
    have a prior close to compare against even on incremental runs that
    only touch one day at a time."""
    lookback_date = before - timedelta(days=1)
    return read_silver([lookback_date])


# ---------------------------------------------------------------------
# dim_asset
# ---------------------------------------------------------------------
def build_dim_asset() -> pd.DataFrame:
    """Build dim_asset from the static ticker -> metadata mapping file.

    SCD Type 1: this dimension is fully overwritten on every run, so a
    correction to asset_mapping.json is reflected immediately and
    applies retroactively when joined against historical fact rows.
    No version history is kept.
    """
    logger.info("Loading asset mapping from s3://%s/%s", MAPPING_BUCKET, MAPPING_KEY)
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=MAPPING_BUCKET, Key=MAPPING_KEY)
    asset_mapping = json.loads(response["Body"].read())

    rows = [
        {
            "ticker": v["trading212_ticker"],
            "symbol": k,
            "name": v["name"],
            "sector": v["sector"],
            "industry": v["industry"],
            "asset_type": v["asset_type"],
            "is_etf": v["asset_type"] == "etf",
        }
        for k, v in asset_mapping["assets"].items()
    ]
    return pd.DataFrame(rows)


DEFAULT_ASSET = {
    "symbol": None,
    "name": None,
    "sector": "Unknown",
    "industry": "Unknown",
    "asset_type": "Unknown",
    "is_etf": False,
}


def reconcile_unmapped_tickers(
    fact_tickers: pd.Series, dim_asset: pd.DataFrame
) -> pd.DataFrame:
    """Append placeholder dim_asset rows for tickers seen in silver but
    absent from asset_mapping.json, so fact_positions never references
    a ticker that doesn't exist in dim_asset."""
    known = set(dim_asset["ticker"])
    unmapped = sorted(set(fact_tickers.dropna()) - known)
    if not unmapped:
        return dim_asset

    logger.warning(
        "%d ticker(s) in silver have no asset mapping entry: %s",
        len(unmapped),
        unmapped,
    )
    placeholders = pd.DataFrame([{"ticker": t, **DEFAULT_ASSET} for t in unmapped])
    return pd.concat([dim_asset, placeholders], ignore_index=True)


# ---------------------------------------------------------------------
# date_id: the shared fact/dim_date key, derived identically on both
# sides so a fact row's date_id is always guaranteed to resolve
# against dim_date -- computing it independently in two places risked
# the two derivations silently drifting apart.
# ---------------------------------------------------------------------
def to_date_id(dates) -> pd.Series:
    # pd.Series(dates) normalizes both plain Series and DatetimeIndex
    # (e.g. from pd.date_range) to a Series first, since .dt is a
    # Series-only accessor -- DatetimeIndex exposes the same
    # .strftime() directly on itself, not via .dt.
    return pd.to_datetime(pd.Series(dates)).dt.strftime("%Y%m%d").astype("int64")


# ---------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------
def build_dim_date(start: date, end: date) -> pd.DataFrame:
    """Standard calendar dimension, one row per day in [start, end]."""
    days = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date_id": to_date_id(days),
            "full_date": days.date,
            "year": days.year,
            "month": days.month,
            "month_name": days.strftime("%B"),
            "day": days.day,
            "day_of_week": days.dayofweek,  # Monday=0
            "day_name": days.strftime("%A"),
            "quarter": days.quarter,
            "is_weekend": days.dayofweek >= 5,
        }
    )


def merge_dim_date(new_rows: pd.DataFrame) -> pd.DataFrame:
    """Merge newly-needed date rows into the existing dim_date table,
    de-duplicated by date_id. dim_date only ever grows."""
    try:
        existing = wr.s3.read_parquet(path=DIM_DATE_PATH, dataset=True)
    except wr.exceptions.NoFilesFound:
        existing = pd.DataFrame(columns=new_rows.columns)

    combined = pd.concat([existing, new_rows], ignore_index=True)
    combined = combined.drop_duplicates(subset="date_id").sort_values("date_id")
    return combined.reset_index(drop=True)


# ---------------------------------------------------------------------
# fact_positions
# ---------------------------------------------------------------------


def build_fact_positions(from_date: str, to_date: str) -> pd.DataFrame:
    """Narrow silver down to the fact grain: FKs + measures only.
    Asset attributes (name, sector, industry, ...) live in dim_asset
    and are reached via a join on ticker, not duplicated here.

    df_lookback supplies one extra prior day per ticker (read separately
    in main via read_silver_lookback_day) purely so price_change /
    daily_return_pct have a previous close to diff against on
    incremental runs; its rows are dropped again before returning."""

    sql = f"""

    WITH account_summary AS (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY ingested_date
                ORDER BY ingested_timestamp DESC
            ) AS rn,
            ingested_date,
            total_investment_cost,
            unrealized_profit_loss AS account_pnl
        FROM silver_t212_account_summary
        WHERE (
            ingested_date >= date_parse('{from_date}', '%Y-%m-%d') - interval '364' day
        AND ingested_date <= date_parse('{to_date}', '%Y-%m-%d')
        )
    ),

    positions AS (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY ingested_date, ticker
                ORDER BY ingested_timestamp DESC
            ) AS rn,
            name,
            ticker,
            ingested_date,
            ingested_timestamp,
            quantity,
            current_price,
            current_value,
            unrealized_profit_loss AS pnl
        FROM silver_t212_positions
        WHERE (
            ingested_date >= date_parse('{from_date}', '%Y-%m-%d') - interval '364' day
        AND ingested_date <= date_parse('{to_date}', '%Y-%m-%d')
        )
    ),

    daily_positions AS (
        SELECT
            name,
            ticker,
            ingested_date,
            ingested_timestamp,
            quantity,
            current_price,
            current_value,
            pnl,

            LAG(current_price) OVER (
                PARTITION BY ticker
                ORDER BY ingested_date
            ) AS prev_price,

            LAG(current_value) OVER (
                PARTITION BY ticker
                ORDER BY ingested_date
            ) AS prev_value,

            AVG(current_value) OVER (
                PARTITION BY ticker
                ORDER BY ingested_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS avg_value_7d,

            AVG(current_value) OVER (
                PARTITION BY ticker
                ORDER BY ingested_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS avg_value_30d,

            AVG(current_value) OVER (
                PARTITION BY ticker
                ORDER BY ingested_date
                ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
            ) AS avg_value_90d,

            AVG(current_value) OVER (
                PARTITION BY ticker
                ORDER BY ingested_date
                ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
            ) AS avg_value_180d,

            AVG(current_value) OVER (
                PARTITION BY ticker
                ORDER BY ingested_date
                ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
            ) AS avg_value_365d

        FROM positions
        WHERE rn = 1
    )

    SELECT
        p.name,
        p.ticker,
        p.quantity,
        p.current_price,
        p.prev_price,
        p.current_price - p.prev_price AS daily_price_change,
        (p.current_price - p.prev_price) / NULLIF(p.prev_price, 0) * 100 AS daily_price_change_pct,
        p.current_value,
        p.prev_value,
        p.current_value - p.prev_value AS daily_value_change,
        (p.current_value - p.prev_value) / NULLIF(p.prev_value, 0) * 100 AS daily_value_change_pct,
        p.pnl,
        p.pnl / NULLIF(a.account_pnl, 0) * 100 AS account_pnl_pct,
        p.current_value / NULLIF(a.total_investment_cost, 0) * 100 AS weight_pct,
        avg_value_7d,
        avg_value_30d,
        avg_value_90d,
        avg_value_180d,
        avg_value_365d,
        p.ingested_date AS ingested_date,
        p.ingested_timestamp
    FROM daily_positions p
    INNER JOIN account_summary a
        ON p.ingested_date = a.ingested_date
        AND a.rn = 1
    WHERE prev_value IS NOT NULL
    AND p.ingested_date >= date_parse('{from_date}', '%Y-%m-%d')
    ORDER BY p.ingested_date ;
    """

    try:
        fact = wr.athena.read_sql_query(
            sql=sql,
            database=GLUE_DATABASE,
            s3_output="s3://financial-dataflow/query-results/",
        )

        return fact
    except Exception as e:
        logger.info(e, exc_info=True)
        raise e


def write_fact_positions(df: pd.DataFrame) -> None:
    logger.info(
        "Writing %d rows to %s (partitioned by ingested_date)", len(df), FACT_PATH
    )
    wr.s3.to_parquet(
        df=df,
        path=FACT_PATH,
        dataset=True,
        mode="overwrite_partitions",  # safe to rerun/backfill any date without duplicating
        partition_cols=["ingested_date"],
        database=GLUE_DATABASE,
        table=FACT_TABLE,
    )


def write_dim_asset(df: pd.DataFrame) -> None:
    logger.info(
        "Writing %d rows to %s (full overwrite, SCD Type 1)", len(df), DIM_ASSET_PATH
    )
    wr.s3.to_parquet(
        df=df,
        path=DIM_ASSET_PATH,
        dataset=True,
        mode="overwrite",
        database=GLUE_DATABASE,
        table=DIM_ASSET_TABLE,
    )


def write_dim_date(df: pd.DataFrame) -> None:
    logger.info("Writing %d rows to %s (full overwrite)", len(df), DIM_DATE_PATH)
    wr.s3.to_parquet(
        df=df,
        path=DIM_DATE_PATH,
        dataset=True,
        mode="overwrite",
        database=GLUE_DATABASE,
        table=DIM_DATE_TABLE,
    )


def main(event) -> None:

    from_date = event.get("from_date")
    to_date = event.get("to_date")
    is_backfill = bool(from_date)

    dates = dates_to_process(from_date, to_date, is_backfill)
    if not dates:
        logger.info("No new partitions to process. Exiting.")
        return

    df_silver = read_silver(dates)
    if df_silver.empty:
        logger.info("No silver data found for target dates. Exiting without writing.")
        return

    from_date = min(dates)
    to_date = max(dates)

    dim_asset = build_dim_asset()
    dim_asset = reconcile_unmapped_tickers(df_silver["ticker"], dim_asset)
    write_dim_asset(dim_asset)

    current_date = datetime.now(timezone.utc).date()

    new_dim_date_rows = build_dim_date(from_date, current_date)
    dim_date = merge_dim_date(new_dim_date_rows)
    write_dim_date(dim_date)

    fact_t212_positions = build_fact_positions(from_date, to_date)
    write_fact_positions(fact_t212_positions)

    if not is_backfill:
        # Deliberately max(dates) - 1, not max(dates): today (always
        # the last entry in dates, see dates_to_process) must stay
        # reprocessable by later runs the same day, so the watermark
        # only ever marks days that are fully in the past.
        set_watermark(max(dates) - timedelta(days=1))

    logger.info("Job complete. Dates processed: %s", [d.isoformat() for d in dates])


def lambda_handler(event, context):
    main(event)

    return {
        "statusCode": 200,
        # "records": total_records,
        # "endpoints": endpoint_metrics,
        # "duration_seconds": round(total_duration, 2)
    }


if __name__ == "__main__":
    event = {"from_date": "2026-06-15", "to_date": "2026-10-25"}
    main(event)
