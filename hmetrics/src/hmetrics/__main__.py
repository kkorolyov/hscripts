from argparse import ArgumentParser, RawTextHelpFormatter
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
from os import environ

from hmetrics import commodity
from hmetrics.commodity import CommodityValue
from hmetrics.ledger import Ledger, Transaction
from hmetrics.metrics import client
from hmetrics.util import cumulativeSum, dateRange, fill


def _parseArgs():
    parser = ArgumentParser(
        description="Emits current ledger file metrics",
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "-i", "--input", type=str, help="ledger file to read; defaults to $LEDGER_FILE"
    )
    parser.add_argument("-u", "--url", type=str, help="URL to write to", required=True)

    return parser.parse_args()


def main():
    args = _parseArgs()

    # fetch ledger data
    ledger = Ledger(args.input or environ["LEDGER_FILE"])

    accounts = ledger.accounts()
    print(f"found {len(accounts)} accounts")

    transactions = list(ledger.transactions())
    start = min(transactions, key=lambda t: t.time).time
    end = max(transactions, key=lambda t: t.time).time + timedelta(days=1)
    print(f"found {len(transactions)} transactions from {start} - {end}")

    # fetch commodity values
    commodities = set(t.commodity for t in transactions)
    print(f"found {len(commodities)} distinct commodities")

    commodityValues = list(commodity.values(commodities, start, end))
    # infer any remaining commodities from ledger
    commodityValues.extend(
        (
            price
            for c in (commodities - {t.name for t in commodityValues})
            for price in ledger.prices(c)
        )
    )
    print(f"found {len(commodityValues)} commodity values")

    transactions = list(
        cumulativeSum(
            sorted(
                fill(
                    transactions,
                    dateRange(start, end),
                    lambda t: t.time,
                    lambda t: (t.account, t.commodity),
                    lambda time, group, _: Transaction(time, *group, Decimal(0)),
                )
            ),
            lambda t: (t.account, t.commodity),
            lambda t: t.quantity,
            Decimal(0),
        )
    )
    print(f"filled to total {len(transactions)} transactions")

    commodityValues = {
        (t.time, t.name): t
        for t in fill(
            commodityValues,
            dateRange(start, end),
            lambda t: t.time,
            lambda t: t.name,
            lambda time, name, t: CommodityValue(time, name, t.value),
        )
    }
    print(f"filled to total {len(commodityValues)} commodity values")

    # split into timeseries samples
    accountSamples = defaultdict[tuple[str, str], list[tuple[Transaction, Decimal]]](
        list
    )
    for t in transactions:
        accountSamples[(t[0].account, t[0].commodity)].append(t)
    print(f"split transactions into {len(accountSamples)} time series")

    commodityValueSamples = defaultdict[tuple[str, str], list[CommodityValue]](list)
    for t in commodityValues.values():
        commodityValueSamples[(t.name, commodity.typeOf(t.name))].append(t)
    print(f"split commodity values into {len(accountSamples)} time series")

    # write metrics
    with client(args.url) as c:
        # delete current samples
        c.delete("finances.*")

        # write fresh samples
        for group, samples in accountSamples.items():
            labels = dict(zip(("name", "commodity"), group))

            c.push(
                "finances_account_total",
                labels,
                {t.time: total for t, total in samples},
            )
            c.push(
                "finances_account_value",
                labels,
                {
                    t.time: total * commodityValues[(t.time, t.commodity)].value
                    for t, total in samples
                },
            )

        for group, samples in commodityValueSamples.items():
            labels = dict(zip(("name", "type"), group))

            c.push(
                "finances_commodity_value",
                labels,
                {t.time: t.value for t in samples},
            )


if __name__ == "__main__":
    main()
