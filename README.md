# CCXT OHLCV Downloader

Incremental downloader of OHLCV candles from Binance and Crypto.com with CSV export, resume support, gap detection and repair system.

---

## Features

- Incremental OHLCV download (no full re-download)
- Resume from partial CSV files
- Full + partial CSV management
- Multi-exchange support (Binance, Crypto.com)
- Automatic gap detection in historical data
- Gap repair (re-download missing candles)
- BTC conversion for selected pairs
- Batch processing with rate-limit handling
- Debug logging system
- Safe interruption handling (Ctrl+E)

---

## Requirements

- Python 3.10+
- ccxt
- pandas==2.3.3

Install dependencies:

pip install ccxt pandas==2.3.3

---

## Important

This project is NOT compatible with pandas 3.x.

If pandas >= 3.0 is detected, the program will stop execution.

Reason:
- Copy-on-Write behavior changes
- dtype changes
- datetime conversion differences

---

## Configuration

Edit inside script:

TIMEFRAME = "1m"
START_DATE = "2021-12-31 23:00:00"

BINANCE_SYMBOLS = ["BTC/EUR", "ETH/EUR", "XRP/EUR"]
CRYPTOCOM_SYMBOLS = ["CRO/BTC"]

BATCH_LIMIT = 1000
PARTIAL_EVERY = 10
DEBUG = True

---

## Usage

Run downloader:

python script.py

---

Format partial files:

python script.py -format

---

Verify data integrity:

python script.py -verify

---

Repair missing data:

python script.py -restore

---

Automatic mode:

python script.py -auto

---

Update BTC-related pairs:

python script.py -updatebtc

---

## How it works

1. Downloads OHLCV candles via CCXT
2. Saves data in partial CSV files
3. Periodically consolidates into full CSV
4. Supports resume after interruption
5. Detects missing timestamps (gaps)
6. Optionally re-downloads missing data

---

## CSV Format

timestamp;close

- timestamp = formatted datetime
- close = price (optionally BTC converted)

---

## Project Structure

CCXT Downloader
│
├── script.py
├── BTC.csv
├── *_partial.csv
├── *.csv (final datasets)
└── log.txt

---

## Notes

- Use Ctrl+E to safely stop execution
- Do NOT use Ctrl+C
- Designed for large historical datasets
- Optimized for incremental execution

---

## License

This project uses the MIT License.

The ccxt library is also licensed under MIT:
https://github.com/ccxt/ccxt

Uses:
- ccxt (MIT)
- pandas (BSD)