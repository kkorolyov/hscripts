import argparse
from datetime import datetime
import sys

from yfinance import Ticker

parser = argparse.ArgumentParser(
    description="Fetches and appends current stock closing prices",
    formatter_class=argparse.RawTextHelpFormatter,
)
parser.add_argument(
    "tickers",
    type=str,
    nargs="*",
    help="tickers to fetch",
)
parser.add_argument(
    "--output", "-o", type=str, required=True, help="ledger file to write output to"
)


def fetch(ticker):
    print(f"fetching ticker: {ticker}...")

    date = datetime.now().date().isoformat()
    price = Ticker(ticker).history(period="5d")["Close"][-1]

    print(f"fetched ticker: {ticker} at {price}")

    return f"P {date} {ticker} {price}"


def main():
    args = parser.parse_args()

    tickers = (
        args.tickers
        if sys.stdin.isatty()
        else [t for t in sys.stdin.read().splitlines() if len(t)]
    )
    output = args.output

    print(f"fetching prices for {tickers}...")

    with open(output, "a") as f:
        f.write("\n")
        for ticker in tickers:
            try:
                f.write(f"{fetch(ticker)}\n")
            except Exception:
                print(f"not a valid ticker: {ticker}")


if __name__ == "__main__":
    main()
