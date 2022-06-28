import argparse
from datetime import datetime

import yaml
from yfinance import Ticker

parser = argparse.ArgumentParser(
    description="Fetches and appends current stock closing prices",
    formatter_class=argparse.RawTextHelpFormatter,
)
parser.add_argument(
    "file",
    help="""configuration YAML file path containing:
file - path of file to append prices to
tickers - array of ticker symbols to fetch
""",
)


def fetch(ticker):
    date = datetime.now().date().isoformat()
    price = Ticker(ticker).history(period="5d")["Close"][0]
    return f"P {date} {ticker} {price}"


def main():
    args = parser.parse_args()

    with open(args.file) as fc:
        config = yaml.load(fc, yaml.Loader)

        with open(config["file"], "a") as f:
            for ticker in config["symbols"]:
                f.write(f"{fetch(ticker)}\n")
            f.write("\n")


if __name__ == "__main__":
    main()
