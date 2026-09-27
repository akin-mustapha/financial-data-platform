"""
Glue Python Shell job: Trading212 positions, bronze -> silver.

Incremental with watermarking + explicit backfill support.

Reads raw JSON positions dumps from date-partitioned bronze S3 prefixes
(expects layout: bronze-positions/ingested_date=YYYY-MM-DD/*.json),
flattens nested `instrument` / `walletImpact` objects, casts types, and
writes partitioned Parquet to the silver zone. The write call also
registers the table in the Glue Data Catalog, so a separate Glue
Crawler is not needed for this table.

Incremental logic:
- A watermark (last successfully processed ingested_date) is stored as
  a small JSON file in S3. Each normal run only reads bronze partitions
  newer than the watermark, then advances it.
- Glue's native Job Bookmarks feature would normally handle this, but
  Bookmarks are only supported on Spark ETL jobs, not Python Shell —
  hence this hand-rolled version.

Backfill:
- Pass --START_DATE (YYYY-MM-DD) as a job parameter to reprocess a
  historical date range. --END_DATE is optional and defaults to today
  if omitted, so a single --START_DATE reprocesses everything from
  that date through today. Backfill runs do NOT move the watermark,
  so they can't accidentally disturb normal incremental runs.
- Reprocessing any date is safe because writes use
  mode="overwrite_partitions": rerunning a date replaces that
  partition rather than duplicating rows.

Job setup (Python Shell, not Spark):
- Python version: 3.9
- Job parameters:
    --JOB_NAME                 positions-bronze-to-silver
    --additional-python-modules awswrangler==3.*,pandas,pyarrow
  (Python Shell jobs install pip packages via --additional-python-modules;
   no need to build/upload a wheel for common packages like these.)
- Max capacity: 0.0625 or 1 DPU is plenty at this data volume.
"""
import sys
import argparse
import logging
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Optional backfill window, parsed separately from getResolvedOptions
# since getResolvedOptions treats every listed arg as required.
_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--START_DATE", default=None, help="YYYY-MM-DD, backfill start (inclusive)")
_parser.add_argument("--END_DATE", default=None, help="YYYY-MM-DD, backfill end (inclusive). Defaults to today if omitted.")

BACKFILL_ARGS, _ = _parser.parse_known_args(sys.argv[1:])
IS_BACKFILL = bool(BACKFILL_ARGS.START_DATE)


# ---------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------
def get_watermark() -> Optional[date]:
    try:
        state = wr.s3.read_json(STATE_PATH, lines=False)
        return pd.to_datetime(state["last_processed_partition_date"].iloc[0]).date()
    except Exception:
        logger.info("No watermark found at %s — treating this as the first run", STATE_PATH)
        return None

def set_watermark(new_date: date) -> None:
    wr.s3.to_json(df=pd.DataFrame([{"last_processed_partition_date": new_date.isoformat()}]), path=STATE_PATH)
    logger.info("Watermark advanced to %s", new_date)

def dates_to_process(from_date, to_date, is_backfill) -> List[date]:
    if is_backfill:
        start = pd.to_datetime(from_date).date()
        end = (
            pd.to_datetime(to_date).date()
            if to_date
            else date.today()
        )
        logger.info("Backfill mode: %s to %s (watermark will NOT be updated)", start, end)
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]

    # today is always included and reprocessed, even if the watermark
    # already covers it -- the Lambda ingests up to three times a day
    # (open/midday/close, see scheduler.tf), so today's bronze
    # partition isn't complete until the day's last run; overwrite_
    # partitions makes re-flattening it each run safe.
    watermark = get_watermark()
    today = date.today()
    start = today if watermark is None else min(watermark + timedelta(days=1), today)
    return [start + timedelta(days=i) for i in range((today - start).days + 1)]


# ---------------------------------------------------------------------
# Read / transform / write
# ---------------------------------------------------------------------
def read_bronze(path, dates: List[date]) -> pd.DataFrame:
    """
    Read JSON objects for the given dates.

    Bronze is laid out as plain nested folders (year/month/day), not
    Hive-style key=value partitions — bronze is never cataloged in Glue,
    so there's no benefit to Hive-style here, and matching the Lambda's
    actual write path avoids an unnecessary migration.

    Each record is stamped with the partition date it was read from
    (_bronze_partition_date), rather than relying on the ingested_date
    field inside the JSON payload itself. The folder path is the source
    of truth for partitioning; the embedded field's format has drifted
    across bronze's history (plain date vs. tz-aware timestamp strings
    depending on when the Lambda wrote it), so deriving from the path
    sidesteps that inconsistency entirely instead of parsing around it.
    """
    frames = []
    for d in dates:
        p = f"{path}{d.year}/{d.month:02d}/{d.day:02d}/"
        try:
            part_df = wr.s3.read_json(path=p, lines=True)
            part_df["_bronze_partition_date"] = d.isoformat()
            frames.append(part_df)
            logger.info("Read partition %s (%d rows)", p, len(part_df))
        except NoFilesFound as e:
            logger.warning("No bronze data found for partition %s, skipping", p)
        except Exception:
            raise e
        
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    logger.info("Total bronze row count for this run: %d", len(df))
    return df

def write_silver(df: pd.DataFrame, path: str, database: str, table: str) -> None:
    """Write partitioned Parquet and register/update the Glue Catalog table."""
    logger.info("Writing %d rows to %s (partitioned by ingested_date)", len(df), path)
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",   # safe to rerun/backfill any date without duplicating
        partition_cols=["ingested_date"],
        database=database,             # writing database+table registers/updates the
        table=table,                   # Glue Data Catalog entry — no crawler needed
    )

def transform_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten nested instrument/walletImpact objects and cast types."""
    logger.info("Flattening nested JSON columns")
    flat = pd.json_normalize(df.to_dict(orient="records"), sep=".")
    
    # Columns that need numeric casting after flattening. Keys use dot
    # notation because pandas.json_normalize flattens nested dicts to
    # "parent.child" column names.
    DOUBLE_COLS = [
        "averagePricePaid",
        "currentPrice",
        "quantity",
        "quantityAvailableForTrading",
        "quantityInPies",
        "walletImpact.currentValue",
        "walletImpact.fxImpact",
        "walletImpact.totalCost",
        "walletImpact.unrealizedProfitLoss",
    ]

    for c in DOUBLE_COLS:
        if c in flat.columns:
            flat[c] = pd.to_numeric(flat[c], errors="coerce")
        else:
            logger.warning("Expected column missing from bronze data: %s", c)
            flat[c] = pd.NA

    def col(name: str) -> pd.Series:
        return flat[name] if name in flat.columns else pd.Series([None] * len(flat))

    result = pd.DataFrame({
        "name": col("instrument.name"),
        "ticker": col("instrument.ticker"),
        "isin": col("instrument.isin"),
        "created_at": pd.to_datetime(col("createdAt"), utc=True, errors="coerce"),
        "asset_currency": col("instrument.currency"),
        "avg_price_paid": col("averagePricePaid").astype("float64"),
        "current_price": col("currentPrice").astype("float64"),
        "quantity": col("quantity").astype("float64"),
        "quantity_available_for_trading": col("quantityAvailableForTrading").astype("float64"),
        "quantity_in_pies": col("quantityInPies").astype("float64"),
        "account_currency": col("walletImpact.currency"),
        "current_value": col("walletImpact.currentValue").astype("float64"),
        "fx_impact": col("walletImpact.fxImpact").astype("float64"),
        "total_cost": col("walletImpact.totalCost").astype("float64"),
        "unrealized_profit_loss": col("walletImpact.unrealizedProfitLoss").astype("float64"),
        "ingested_timestamp": pd.to_datetime(col("ingested_timestamp"), utc=True, errors="coerce"),
        "ingested_date": pd.to_datetime(col("_bronze_partition_date"), errors="coerce").dt.date,
    })
    return result

def transform_account_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten nested instrument/walletImpact objects and cast types."""
    logger.info("Flattening nested JSON columns")
    flat = pd.json_normalize(df.to_dict(orient="records"), sep=".")

    # Columns that need numeric casting after flattening. Keys use dot
    # notation because pandas.json_normalize flattens nested dicts to
    # "parent.child" column names.
    DOUBLE_COLS = [
        "totalValue",
        "cash.availableToTrade",
        "cash.reservedForOrders",
        "cash.inPies",
        "investments.currentValue",
        "investments.totalCost",
        "investments.realizedProfitLoss",
        "investments.unrealizedProfitLoss",
    ]
    
    for c in DOUBLE_COLS:
        if c in flat.columns:
            flat[c] = pd.to_numeric(flat[c], errors="coerce")
        else:
            logger.warning("Expected column missing from bronze data: %s", c)
            flat[c] = pd.NA

    def col(name: str) -> pd.Series:
        return flat[name] if name in flat.columns else pd.Series([None] * len(flat))

    result = pd.DataFrame({
        "id": col("id"),
        "currency": col("currency"),
        "total_value": col("totalValue"),
        "cash_available_to_trade": col("cash.availableToTrade"),
        "cash_reserved_for_orders": col("cash.reservedForOrders"),
        "cash_in_pies": col("cash.inPies"),
        "total_value_investmented": col("investments.currentValue"),
        "total_investment_cost": col("investments.totalCost"),
        "realized_profit_loss": col("investments.realizedProfitLoss"),
        "unrealized_profit_loss": col("investments.unrealizedProfitLoss"),
        "ingested_timestamp": pd.to_datetime(col("ingested_timestamp"), utc=True, errors="coerce"),
        "ingested_date": pd.to_datetime(col("ingested_date"), errors="coerce").dt.date,
    })
    return result

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
CONFIG = {
        "positions": {
            "input_path": "s3://financial-dataflow/data/bronze/trading212/positions/",  # ingested_date=YYYY-MM-DD/ partitions
            "output_path": "s3://financial-dataflow/data/silver/trading212/positions/",
            "glue_database": "financials",
            "glue_table": "silver_t212_positions",
            "transform": transform_positions
        },
        "account_summary": {
            "input_path": "s3://financial-dataflow/data/bronze/trading212/account/",
            "output_path": "s3://financial-dataflow/data/silver/trading212/account_summary/",
            "glue_database": "financials",
            "glue_table": "silver_t212_account_summary",
            "transform": transform_account_summary
        }
    
    }

STATE_PATH = "s3://financial-dataflow/data/silver/trading212/_state/watermark.json"

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main(event) -> None:
    
    from_date = event.get("from_date")
    to_date = event.get("to_date")
    is_backfill = bool(from_date)
    
    dates = dates_to_process(from_date, to_date, is_backfill)
    logger.info(f"Processing data for {len(dates)} days")
    if not dates:
        logger.info("No new partitions to process. Exiting.")
        return
    
    for key, mapping in CONFIG.items():
        input_path = mapping.get("input_path")
        output_path = mapping.get("output_path")
        glue_database = mapping.get("glue_database")
        glue_table = mapping.get("glue_table")
        
        logger.info(f"Reading {key} bronze data")
        df_bronze = read_bronze(input_path, dates)
        
        
        if df_bronze.empty:
            logger.info(f"No {key} bronze data found for target dates. Exiting without writing.")
            continue
        
        transform = mapping.get("transform")
        df_silver = transform(df_bronze)
        
        write_silver(df_silver, output_path, glue_database, glue_table)

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
    event = {
    "from_date": "2026-08-15",
    "to_date": "2026-08-25"
    }
    main(event)
    