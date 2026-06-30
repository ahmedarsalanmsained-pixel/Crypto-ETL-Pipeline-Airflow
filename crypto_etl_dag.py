"""
ETL Pipeline for Top 10 Cryptocurrency Data
Assignment: Big Data Computing – Apache Airflow ETL
"""

from datetime import datetime, timedelta
import json
import csv
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────
# Default DAG arguments
# ─────────────────────────────────────────────
default_args = {
    'owner': 'Arsalan',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

dag = DAG(
    dag_id='Crypto_ETL',
    description='ETL pipeline for Top 10 Crypto prices using Binance API',
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['crypto', 'binance', 'etl'],
)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
# Top 10 Cryptocurrencies by Market Cap (USDT pairs)
SYMBOLS = [
    "BTCUSDT",   # Bitcoin
    "ETHUSDT",   # Ethereum
    "BNBUSDT",   # Binance Coin
    "SOLUSDT",   # Solana
    "XRPUSDT",   # XRP
    "DOGEUSDT",  # Dogecoin
    "ADAUSDT",   # Cardano
    "AVAXUSDT",  # Avalanche
    "SHIBUSDT",  # Shiba Inu
    "DOTUSDT",   # Polkadot
]

SYMBOL_NAMES = {
    "BTCUSDT":  "Bitcoin",
    "ETHUSDT":  "Ethereum",
    "BNBUSDT":  "Binance Coin",
    "SOLUSDT":  "Solana",
    "XRPUSDT":  "XRP",
    "DOGEUSDT": "Dogecoin",
    "ADAUSDT":  "Cardano",
    "AVAXUSDT": "Avalanche",
    "SHIBUSDT": "Shiba Inu",
    "DOTUSDT":  "Polkadot",
}

BINANCE_BASE_URL = "https://api.binance.com/api/v3"
OUTPUT_DIR  = "/home/saleem/airflow/output"
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, "crypto_data.csv")
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "crypto_data.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# TASK 1 – EXTRACT
# ─────────────────────────────────────────────
def extract_crypto_data(ti):
    """
    Extract real-time ticker data from Binance public API.
    Uses /api/v3/ticker/24hr endpoint — NO API KEY required.
    Pushes raw records list to XCom key 'raw_data'.
    """
    import requests

    raw_records  = []
    fetch_time   = datetime.now().isoformat()

    for symbol in SYMBOLS:
        try:
            url      = f"{BINANCE_BASE_URL}/ticker/24hr"
            params   = {"symbol": symbol}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            record = {
                "symbol":              symbol,
                "name":                SYMBOL_NAMES.get(symbol, symbol),
                "price":               data.get("lastPrice"),
                "open":                data.get("openPrice"),
                "high":                data.get("highPrice"),
                "low":                 data.get("lowPrice"),
                "prev_close":          data.get("prevClosePrice"),
                "price_change":        data.get("priceChange"),
                "price_change_pct":    data.get("priceChangePercent"),
                "volume":              data.get("volume"),
                "quote_volume":        data.get("quoteVolume"),
                "trades_count":        data.get("count"),
                "weighted_avg_price":  data.get("weightedAvgPrice"),
                "fetch_timestamp":     fetch_time,
            }
            raw_records.append(record)
            logging.info("Extracted %s (%s): price=%s",
                         symbol, SYMBOL_NAMES[symbol], data.get("lastPrice"))

        except requests.exceptions.RequestException as exc:
            logging.error("HTTP error fetching %s: %s", symbol, exc)
        except (KeyError, ValueError) as exc:
            logging.error("Parsing error for %s: %s", symbol, exc)

    if not raw_records:
        raise ValueError("Extraction failed: no records retrieved.")

    ti.xcom_push(key='raw_data', value=raw_records)
    logging.info("Extraction complete. %d records pushed to XCom.", len(raw_records))


# ─────────────────────────────────────────────
# TASK 2 – TRANSFORM
# ─────────────────────────────────────────────
def transform_crypto_data(ti):
    """
    Apply 4 transformations to raw crypto data:
      1. Data Type Conversion  – cast strings to float/int
      2. Data Cleaning         – round values, handle nulls
      3. Filtering             – drop records with missing/zero price
      4. Feature Engineering   – price_range, volatility_pct, trade_intensity,
                                 vwap_deviation, market_signal
    Pushes cleaned records to XCom key 'transformed_data'.
    """
    raw_records = ti.xcom_pull(key='raw_data', task_ids='extract')
    if not raw_records:
        raise ValueError("No raw data found in XCom.")

    transformed = []

    for rec in raw_records:

        # ── Transformation 1: Data Type Conversion ───────────────────────
        try:
            price             = float(rec["price"])             if rec.get("price")            else None
            open_p            = float(rec["open"])              if rec.get("open")             else None
            high              = float(rec["high"])              if rec.get("high")             else None
            low               = float(rec["low"])               if rec.get("low")              else None
            prev_close        = float(rec["prev_close"])        if rec.get("prev_close")       else None
            price_change      = float(rec["price_change"])      if rec.get("price_change")     else None
            price_change_pct  = float(rec["price_change_pct"])  if rec.get("price_change_pct") else None
            volume            = float(rec["volume"])            if rec.get("volume")           else None
            quote_volume      = float(rec["quote_volume"])      if rec.get("quote_volume")     else None
            trades_count      = int(rec["trades_count"])        if rec.get("trades_count")     else None
            weighted_avg      = float(rec["weighted_avg_price"])if rec.get("weighted_avg_price") else None

        except (ValueError, TypeError) as e:
            logging.warning("Type conversion error for %s: %s — skipping.", rec.get("symbol"), e)
            continue

        # ── Transformation 2: Filtering ──────────────────────────────────
        if price is None or price == 0:
            logging.warning("Dropping %s — price is zero or missing.", rec.get("symbol"))
            continue

        # ── Transformation 3: Data Cleaning ──────────────────────────────
        price            = round(price, 6)
        open_p           = round(open_p, 6)           if open_p           else None
        high             = round(high, 6)             if high             else None
        low              = round(low, 6)              if low              else None
        prev_close       = round(prev_close, 6)       if prev_close       else None
        price_change     = round(price_change, 6)     if price_change     else None
        price_change_pct = round(price_change_pct, 4) if price_change_pct else None
        volume           = round(volume, 4)           if volume           else None
        quote_volume     = round(quote_volume, 2)     if quote_volume     else None
        weighted_avg     = round(weighted_avg, 6)     if weighted_avg     else None

        # ── Transformation 4: Feature Engineering ────────────────────────

        # 4a. Price Range (intraday spread)
        price_range = round(high - low, 6) if high and low else None

        # 4b. Volatility % (range as % of open)
        volatility_pct = round((price_range / open_p) * 100, 4) \
                         if price_range and open_p else None

        # 4c. Trade Intensity (avg trade size in USDT)
        trade_intensity = round(quote_volume / trades_count, 4) \
                          if quote_volume and trades_count else None

        # 4d. VWAP Deviation (how far current price is from weighted avg)
        vwap_deviation = round(((price - weighted_avg) / weighted_avg) * 100, 4) \
                         if weighted_avg and weighted_avg != 0 else None

        # 4e. Volatility Label
        if volatility_pct is not None:
            if volatility_pct < 1:
                volatility_label = "Low"
            elif volatility_pct < 3:
                volatility_label = "Medium"
            elif volatility_pct < 6:
                volatility_label = "High"
            else:
                volatility_label = "Extreme"
        else:
            volatility_label = "Unknown"

        # 4f. Market Signal (simple momentum signal)
        if price_change_pct is not None:
            if price_change_pct > 2:
                market_signal = "Strong Buy"
            elif price_change_pct > 0:
                market_signal = "Buy"
            elif price_change_pct > -2:
                market_signal = "Sell"
            else:
                market_signal = "Strong Sell"
        else:
            market_signal = "Neutral"

        cleaned = {
            "symbol":           rec["symbol"],
            "name":             rec["name"],
            "fetch_timestamp":  rec["fetch_timestamp"],
            "price":            price,
            "open":             open_p,
            "high":             high,
            "low":              low,
            "prev_close":       prev_close,
            "price_change":     price_change,
            "price_change_pct": price_change_pct,
            "volume":           volume,
            "quote_volume":     quote_volume,
            "trades_count":     trades_count,
            "weighted_avg":     weighted_avg,
            # Engineered features
            "price_range":      price_range,
            "volatility_pct":   volatility_pct,
            "trade_intensity":  trade_intensity,
            "vwap_deviation":   vwap_deviation,
            "volatility_label": volatility_label,
            "market_signal":    market_signal,
        }
        transformed.append(cleaned)
        logging.info("Transformed %s: price=%.4f change=%.2f%% signal=%s volatility=%s",
                     rec["symbol"], price, price_change_pct or 0,
                     market_signal, volatility_label)

    if not transformed:
        raise ValueError("Transform produced zero valid records.")

    ti.xcom_push(key='transformed_data', value=transformed)
    logging.info("Transformation complete. %d valid records.", len(transformed))


# ─────────────────────────────────────────────
# TASK 3 – LOAD
# ─────────────────────────────────────────────
def load_crypto_data(ti):
    """
    Load transformed records to:
      - Append-mode CSV  → crypto_data.csv
      - Append-mode JSON → crypto_data.json (newline-delimited)
    """
    transformed = ti.xcom_pull(key='transformed_data', task_ids='transform')
    if not transformed:
        raise ValueError("No transformed data found in XCom.")

    fieldnames  = list(transformed[0].keys())
    file_exists = os.path.isfile(OUTPUT_CSV)

    # ── CSV ───────────────────────────────────────────────────────────────
    with open(OUTPUT_CSV, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(transformed)
    logging.info("CSV saved → %s  (%d rows appended)", OUTPUT_CSV, len(transformed))

    # ── JSON (newline-delimited) ──────────────────────────────────────────
    with open(OUTPUT_JSON, "a") as jsonfile:
        for record in transformed:
            jsonfile.write(json.dumps(record) + "\n")
    logging.info("JSON saved → %s  (%d records appended)", OUTPUT_JSON, len(transformed))
    logging.info("Load task finished successfully.")


# ─────────────────────────────────────────────
# OPERATORS & DEPENDENCIES
# ─────────────────────────────────────────────
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_crypto_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_crypto_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_crypto_data,
    dag=dag,
)

extract_task >> transform_task >> load_task
