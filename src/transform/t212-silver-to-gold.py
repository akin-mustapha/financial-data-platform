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

import sys
import json
import argparse
import logging
from datetime import date, timedelta
from typing import List, Optional

import boto3
import pandas as pd
import awswrangler as wr
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
INPUT_PATH = "s3://financial-dataflow/data/silver/trading212/positions/"  # ingested_date=YYYY-MM-DD/ partitions
STATE_PATH = "s3://financial-dataflow/data/gold/_state/positions_gold_watermark.json"
MAPPING_BUCKET = "financial-dataflow"
MAPPING_KEY = "resources/asset_mapping.json"
GLUE_DATABASE = "financials"

FACT_TABLE = "fact_positions"
FACT_PATH = "s3://financial-dataflow/data/gold/fact_positions/"

DIM_ASSET_TABLE = "dim_asset"
DIM_ASSET_PATH = "s3://financial-dataflow/data/gold/dim_asset/"

DIM_DATE_TABLE = "dim_date"
DIM_DATE_PATH = "s3://financial-dataflow/data/gold/dim_date/"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
logger = logging.getLogger(args["JOB_NAME"])
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Optional backfill window, parsed separately from getResolvedOptions
# since getResolvedOptions treats every listed arg as required.
_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument(
    "--START_DATE", default=None, help="YYYY-MM-DD, backfill start (inclusive)"
)
_parser.add_argument(
    "--END_DATE",
    default=None,
    help="YYYY-MM-DD, backfill end (inclusive). Defaults to today if omitted.",
)
BACKFILL_ARGS, _ = _parser.parse_known_args(sys.argv[1:])
IS_BACKFILL = bool(BACKFILL_ARGS.START_DATE)


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


def dates_to_process() -> List[date]:
    if IS_BACKFILL:
        start = pd.to_datetime(BACKFILL_ARGS.START_DATE).date()
        end = (
            pd.to_datetime(BACKFILL_ARGS.END_DATE).date()
            if BACKFILL_ARGS.END_DATE
            else date.today()
        )
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
FACT_MEASURE_COLS = [
    "avg_price_paid",
    "current_price",
    "quantity",
    "current_value",
    "fx_impact",
    "total_cost",
    "unrealized_profit_loss",
]


def collapse_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse to one row per (ticker, ingested_date). The Lambda
    ingests up to three times a day (open/midday/close, see
    scheduler.tf), so a day's silver partition can hold multiple
    snapshots per ticker.

    open_price is the price on that day's oldest snapshot, close_price
    the price on that day's newest -- re-running later the same day
    (e.g. the close run, after the open run already wrote a partial
    day) naturally supersedes open/close with whatever's known so far.
    All other measure columns (quantity, total_cost, unrealized P/L,
    ...) take the closing snapshot's values, since those describe the
    day's final state rather than an intraday delta.
    """
    ordered = df.sort_values("ingested_timestamp")
    grouped = ordered.groupby(["ticker", "ingested_date"], as_index=False)

    daily = grouped.last()
    daily["open_price"] = grouped["current_price"].first()["current_price"].values
    daily = daily.rename(columns={"current_price": "close_price"})
    return daily


def build_fact_positions(
    df_silver: pd.DataFrame, df_lookback: pd.DataFrame
) -> pd.DataFrame:
    """Narrow silver down to the fact grain: FKs + measures only.
    Asset attributes (name, sector, industry, ...) live in dim_asset
    and are reached via a join on ticker, not duplicated here.

    df_lookback supplies one extra prior day per ticker (read separately
    in main via read_silver_lookback_day) purely so price_change /
    daily_return_pct have a previous close to diff against on
    incremental runs; its rows are dropped again before returning."""
    cols = [
        "ticker",
        "ingested_date",
        "ingested_timestamp",
        "asset_currency",
    ] + FACT_MEASURE_COLS
    target_dates = set(df_silver["ingested_date"])
    lookback = (
        df_lookback[cols] if not df_lookback.empty else pd.DataFrame(columns=cols)
    )
    combined = pd.concat([lookback, df_silver[cols]], ignore_index=True).copy()
    combined = collapse_to_daily(combined)
    combined = combined.sort_values(["ticker", "ingested_date"])

    # NaN (not 0) when total_cost is 0/missing -- a 0% return would be
    # indistinguishable from an actual break-even position otherwise.
    combined["unrealized_profit_loss_pct"] = combined[
        "unrealized_profit_loss"
    ] / combined["total_cost"].replace(0, pd.NA)

    # Prior day's close per ticker. NaN (not 0) when there's no prior
    # day in scope (asset's first day, or a gap) -- a missing prior
    # close must not be read as a 0 price / -100% return.
    prev_close = combined.groupby("ticker")["close_price"].shift(1)
    combined["price_change"] = combined["close_price"] - prev_close
    combined["daily_return_pct"] = combined["price_change"] / prev_close.replace(
        0, pd.NA
    )

    fact = combined[combined["ingested_date"].isin(target_dates)].copy()
    fact["date_id"] = to_date_id(fact["ingested_date"])
    return fact


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


def main() -> None:
    dates = dates_to_process()
    if not dates:
        logger.info("No new partitions to process. Exiting.")
        return

    df_silver = read_silver(dates)
    if df_silver.empty:
        logger.info("No silver data found for target dates. Exiting without writing.")
        return

    dim_asset = build_dim_asset()
    dim_asset = reconcile_unmapped_tickers(df_silver["ticker"], dim_asset)
    write_dim_asset(dim_asset)

    new_dim_date_rows = build_dim_date(min(dates), max(dates))
    dim_date = merge_dim_date(new_dim_date_rows)
    write_dim_date(dim_date)

    df_lookback = read_silver_lookback_day(min(dates))
    fact_positions = build_fact_positions(df_silver, df_lookback)
    write_fact_positions(fact_positions)

    if not IS_BACKFILL:
        # Deliberately max(dates) - 1, not max(dates): today (always
        # the last entry in dates, see dates_to_process) must stay
        # reprocessable by later runs the same day, so the watermark
        # only ever marks days that are fully in the past.
        set_watermark(max(dates) - timedelta(days=1))

    logger.info("Job complete. Dates processed: %s", [d.isoformat() for d in dates])


if __name__ == "__main__":
    main()
