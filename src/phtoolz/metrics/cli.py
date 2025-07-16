from argparse import ArgumentParser, RawTextHelpFormatter
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

from phtoolz.common import commodity
from phtoolz.common.commodity import CommodityValue
from phtoolz.common.ledger import Ledger, Transaction
from phtoolz.common.util import cumulativeSum, dateRange, fill
from phtoolz.metrics.metrics import client


def _parseArgs():
    parser = ArgumentParser(
        description="Emits current ledger file metrics",
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="ledger file to read",
    )
    parser.add_argument("-u", "--url", type=str, required=True, help="URL to write to")

    return parser.parse_args()


def cli():
    args = _parseArgs()

    # fetch ledger data
    ledger = Ledger(args.input)

    accounts = ledger.accounts()
    print(f"found {len(accounts)} accounts")

    transactions = ledger.transactions()
    start = min(transactions, key=lambda t: t.time).time
    end = max(transactions, key=lambda t: t.time).time + timedelta(days=1)
    transactionStarts = {
        (t.account, t.commodity): t.time for t in sorted(transactions, reverse=True)
    }
    commodityStarts = {
        k[1]: v
        for k, v in sorted(
            transactionStarts.items(), key=lambda t: (t[0][1], t[1]), reverse=True
        )
    }
    print(f"found {len(transactions)} transactions from {start} - {end}")

    # fetch commodity values
    commodities = set(t.commodity for t in transactions)
    print(f"found {len(commodities)} distinct commodities")

    commodityValues = list(commodity.values(commodities, start, end))
    # infer any remaining commodities from ledger
    missingCommodities = commodities - {t.name for t in commodityValues}
    commodityValues.extend(
        t for t in ledger.prices(True) if t.name in missingCommodities
    )
    print(f"found {len(commodityValues)} commodity values")

    # fill transaction gaps
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
    # start transaction tracking only when its account-commodity combo first referenced in the ledger
    transactions = [
        (t, time)
        for t, time in transactions
        if t.time >= transactionStarts[(t.account, t.commodity)]
    ]

    # fill commodity gaps
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
    # start commodity tracking only when it's first referenced in the ledger
    commodityValues = {
        k: v for k, v in commodityValues.items() if k[0] >= commodityStarts[k[1]]
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
