import logging
from datetime import datetime, timezone
import boto3
import pandas as pd
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ses_client = boto3.client("ses", region_name="eu-west-1")
EMAIL = "akinkunmimustapha1@gmail.com"


def exec_sql(sql_statement: str, db: str = "financials") -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql=sql_statement,
        database=db,
        s3_output="s3://financial-dataflow/athena-results/"  # Ensure S3 output path is set
    )


def main():
    # Set target portfolio / cash values
    total_portfolio_equity = 100.0  # Total portfolio value in USD/GBP
    target_weight = 6.00            # Desired 6% target weight per asset
    
    current_date = datetime.now(timezone.utc).date()

    sql = f"""
    SELECT 
        name,
        ticker,
        weight_pct,
        daily_value_change_pct,
        current_value,
        avg_value_7d,
        pnl,
        ingested_timestamp
    FROM fact_t212_positions
    WHERE
        ingested_date = DATE '{current_date}'
        AND daily_value_change_pct <= -1
        AND current_value < avg_value_7d 
    ORDER BY daily_value_change_pct ASC, weight_pct ASC
    """

    logger.info("Executing Athena query for dip targets...")
    df = exec_sql(sql)

    if df.empty:
        logger.info("No dip opportunities found today. Sending summary email.")
        email_body = f"No position dip targets found for {current_date}."
    else:
        # Calculate target dollar value and required rebalance amount
        df["target_value"] = total_portfolio_equity * (target_weight / 100.0)
        df["rebalance_amount"] = df["target_value"] - df["current_value"]

        # Select & format columns for the notification
        df_summary = df[["ticker", "current_value", "weight_pct", "target_value", "rebalance_amount"]]
        
        # Convert DataFrame to clean string table for email text
        email_body = f"Dip Opportunities Identified ({current_date}):\n\n" + df_summary.to_string(index=False)

    # Send SES Email
    logger.info("Sending SES notification email...")
    ses_client.send_email(
        Source=EMAIL,
        Destination={
            "ToAddresses": [EMAIL]
        },
        Message={
            "Subject": {
                "Data": f"Portfolio Notification - {current_date}"
            },
            "Body": {
                "Text": {
                    "Data": email_body  # Now a valid string!
                }
            }
        }
    )
    logger.info("Email sent successfully.")


if __name__ == "__main__":
    main()