import argparse
from datetime import datetime, timedelta
from os import environ

from hcommon import commodity
from hcommon.ledger import Ledger

parser = argparse.ArgumentParser(
    description="Updates stock closing prices in ledger files",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-i",
    "--input",
    type=str,
    default=environ["LEDGER_FILE"],
    help="ledger file to read",
)
parser.add_argument(
    "-o",
    "--output",
    type=str,
    required=True,
    help="ledger file to write",
)


def main():
    args = parser.parse_args()

    ledger = Ledger(args.input)
    transactions = list(ledger.transactions())
    stocks = {t for t in ledger.commodities() if commodity.typeOf(t) == "stock"}
    start = min(transactions, key=lambda t: t.time).time
    end = max(
        max(transactions, key=lambda t: t.time).time, datetime.today().date()
    ) + timedelta(days=1)

    print(f"getting stock prices for {len(stocks)} stocks from {start} to {end}")
    newValues = sorted(set(commodity.values(stocks, start, end)) - set(ledger.prices()))

    print(f"writing {len(newValues)} new values")

    if len(newValues):
        with open(args.output, "a") as f:
            f.write(
                "\n".join(
                    ["", *(f"P {t.time} {t.name} {t.value}" for t in newValues), ""]
                )
            )


if __name__ == "__main__":
    main()
