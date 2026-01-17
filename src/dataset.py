from datetime import datetime, timedelta
import os

from dotenv import load_dotenv
from loguru import logger
import pandas as pd
import requests
import typer

# Import project config (Paths are centralized here)
from src.config import RAW_DATA_DIR

# Initialize CLI app
app = typer.Typer()

# Load env vars (for API Key)
load_dotenv()


def fetch_eia_data(api_key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Helper function to hit the EIA API with Pagination."""
    url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

    all_records = []
    offset = 0
    batch_size = 5000  # EIA API Limit

    logger.info(f"Fetching data from {start_date} to {end_date}...")

    while True:
        params = {
            "api_key": api_key,
            "frequency": "hourly",
            "data[0]": "value",
            "facets[respondent][]": "CISO",  # California Grid (Change if needed)
            "facets[type][]": "D",  # D = Demand
            "start": start_date,
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": batch_size,
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API Request failed: {e}")
            raise

        if "response" not in data or "data" not in data["response"]:
            logger.error("Invalid API response structure")
            break

        records = data["response"]["data"]

        if not records:
            break

        all_records.extend(records)
        logger.info(f"Fetched batch of {len(records)} rows (Total so far: {len(all_records)})")

        # If we got fewer rows than the limit, we reached the end
        if len(records) < batch_size:
            break

        # Otherwise, move the offset pointer forward
        offset += batch_size

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    # Cleaning
    df = df[["period", "value"]]
    df.rename(columns={"period": "timestamp", "value": "demand"}, inplace=True)
    return df


@app.command()
def main(output_filename: str = "demand_history.csv", days_back: int = 2):
    """
    Fetches electricity demand data and OVERWRITES the raw file (for clean backfill).
    """
    # 1. Get API Key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        logger.error("EIA_API_KEY not found in environment.")
        raise typer.Exit(code=1)

    # 2. Define Dates
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # 3. Fetch Data (With Pagination)
    try:
        new_data = fetch_eia_data(api_key, start_date, end_date)
        logger.success(f"Total rows fetched: {len(new_data)}")
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise typer.Exit(code=1)

    if new_data.empty:
        logger.warning("No data found.")
        return

    # 4. Save to RAW_DATA_DIR (Overwrite mode for backfill consistency)
    output_path = RAW_DATA_DIR / output_filename

    # We use 'w' (write) mode to ensure we have a clean file with just the history we requested
    new_data.to_csv(output_path, mode="w", header=True, index=False)

    logger.success(f"Full history saved to {output_path}")


if __name__ == "__main__":
    app()
