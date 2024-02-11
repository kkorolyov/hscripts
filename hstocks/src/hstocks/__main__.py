import argparse
from datetime import datetime, timedelta
import sys

from yfinance import Ticker

parser = argparse.ArgumentParser(
    description="Fetches and appends stock closing prices to a ledger file",
    formatter_class=argparse.RawTextHelpFormatter,
)
parser.add_argument(
    "tickers",
    type=str,
    nargs="*",
    help="tickers to fetch",
)
parser.add_argument(
    "-o",
    "--output",
    type=str,
    required=True,
    help="ledger file to write output to",
)
parser.add_argument(
    "-s",
    "--start",
    type=str,
    help="start date in ISO (YYYY-MM-DD) format",
    default=datetime.today().date().isoformat(),
)
parser.add_argument(
    "-e",
    "--end",
    type=str,
    help="end date in ISO (YYYY-MM-DD) format",
    default=(datetime.today() + timedelta(days=1)).date().isoformat(),
)
parser.add_argument(
    "-i",
    "--interval",
    type=str,
    choices=["1d", "5d", "1wk", "1mo", "3mo"],
    help="interval between fetched prices",
    default="1d",
)


def fetch(ticker: str, start: str, end: str, interval: str):
    print(f"fetching ticker: {ticker}...")

    history = Ticker(ticker).history(start=start, end=end, interval=interval)

    results = [
        f"P {row.Index.date().isoformat()} {ticker} {row.Close}"
        for row in history.itertuples()
    ]

    print(f"fetched {len(results)} results for ticker: {ticker}")

    return "\n".join(results)


def main():
    args = parser.parse_args()

    tickers = (
        args.tickers
        if sys.stdin.isatty()
        else [t for t in sys.stdin.read().splitlines() if len(t)]
    )
    output = args.output
    start = args.start
    end = args.end
    interval = args.interval

    print(f"fetching prices for {tickers}...")

    with open(output, "a") as f:
        f.write("\n")
        for ticker in tickers:
            f.write(f"{fetch(ticker, start, end, interval)}\n")


if __name__ == "__main__":
    main()
