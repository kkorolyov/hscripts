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
    stocks = {
        t.commodity for t in transactions if commodity.typeOf(t.commodity) == "stock"
    }
    start = min(
        (t for t in transactions if commodity.typeOf(t.commodity) == "stock"),
        key=lambda t: t.time,
    ).time
    end = max(
        max(transactions, key=lambda t: t.time).time, datetime.today().date()
    ) + timedelta(days=1)

    print(f"getting stock prices for {len(stocks)} stocks from {start} to {end}")
    # avoid overloading the journal - use sparser interval for historical prices
    commodityStarts = {t.commodity: t.time for t in sorted(transactions, reverse=True)}
    endHistorical = end - timedelta(days=90)
    newValues = sorted(
        t
        for t in (
            set(commodity.values(stocks, start, endHistorical + timedelta(days=1), 30))
            | set(commodity.values(stocks, endHistorical, end, 1))
            - set(ledger.prices())
        )
        if t.time >= commodityStarts[t.name]
    )

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
