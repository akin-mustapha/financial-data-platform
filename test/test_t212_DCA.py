from unittest.mock import patch, MagicMock
import pytest

from src.t212_DCA.service import get_current_holdings



def test_load_current_holdings():
    # Mock the S3 client and its response
    holdings = [
        {"Key": "data/bronze/trading212/positions/ingested_date=2026-09-14/file1.csv", "LastModified": "2026-09-14T12:00:00Z"},
        {"Key": "data/bronze/trading212/positions/ingested_date=2026-09-14/file2.csv", "LastModified": "2026-09-14T13:00:00Z"},
    ]
    
    current_holdings = get_current_holdings(holdings=holdings, sort_by="LastModified")
    
    # Assert that the most recent holding is returned
    assert current_holdings["Key"] == "data/bronze/trading212/positions/ingested_date=2026-09-14/file2.csv"
    assert current_holdings["LastModified"] == "2026-09-14T13:00:00Z"