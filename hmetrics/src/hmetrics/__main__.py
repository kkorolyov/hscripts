from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import timedelta
from os import environ

from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from hmetrics import commodity
from hmetrics.commodity import CommodityValue
from hmetrics.ledger import Ledger, Transaction
from hmetrics.util import cumulativeSum, datetimeRangeDay, fill


def _parseArgs():
    parser = ArgumentParser(
        description="Emits current ledger file metrics",
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "-i", "--input", type=str, help="ledger file to read; defaults to $LEDGER_FILE"
    )
    parser.add_argument(
        "-u", "--url", type=str, help="InfluxDB URL to write to", required=True
    )
    parser.add_argument(
        "-t", "--token", type=str, help="InfluxDB token to use", required=True
    )
    parser.add_argument(
        "-o", "--org", type=str, help="InfluxDB org to write to", required=True
    )
    parser.add_argument(
        "-b", "--bucket", type=str, help="InfluxDB bucket to write to", required=True
    )

    return parser.parse_args()


def main():
    args = _parseArgs()

    # fetch ledger data
    ledger = Ledger(args.input or environ["LEDGER_FILE"])

    accounts = ledger.accounts()
    print(f"found {len(accounts)} accounts")

    assets = list(ledger.transactions("assets"))

    # combine all transactions for further processing
    transactions = [*assets]
    start = min(transactions, key=lambda t: t.time).time
    end = max(transactions, key=lambda t: t.time).time + timedelta(days=1)
    print(f"found {len(transactions)} transactions from {start} - {end}")

    # fetch commodity values
    commodities = set(t.commodity for t in transactions)
    print(f"found {len(commodities)} distinct commodities")

    commodityValues = list(commodity.values(commodities, start, end))
    print(f"found {len(commodityValues)} commodity values")

    transactions = list(
        cumulativeSum(
            sorted(
                fill(
                    transactions,
                    datetimeRangeDay(start, end),
                    lambda t: t.time,
                    lambda t: t.account,
                    lambda time, account, t: Transaction(
                        time, account, t.commodity, t.quantity
                    ),
                )
            ),
            lambda t: (t.time, t.account, t.commodity),
            lambda t: t.quantity,
            0.0,
        )
    )
    print(f"filled to total {len(transactions)} transactions")

    commodityValues = {
        (t.time, t.commodity): t
        for t in fill(
            commodityValues,
            datetimeRangeDay(start, end),
            lambda t: t.time,
            lambda t: t.commodity,
            lambda time, commodity, t: CommodityValue(time, commodity, t.value),
        )
    }
    print(f"filled to total {len(commodityValues)} commodity values")

    # generate points to write
    points = [
        *(
            Point.measurement("transaction")
            .field("change", t.quantity)
            .field(
                "change_v", t.quantity * commodityValues[(t.time, t.commodity)].value
            )
            .field("total", total)
            .field("value", total * commodityValues[(t.time, t.commodity)].value)
            .tag("name", t.account)
            .tag("commodity", t.commodity)
            .time(t.time)
            for t, total in transactions
        ),
        *(
            Point.measurement("commodity")
            .field("value", t.value)
            .tag("name", t.commodity)
            .time(t.time)
            for t in commodityValues.values()
        ),
    ]

    # write to InfluxDB
    with InfluxDBClient(args.url, args.token, org=args.org, timeout=60000) as client:
        with client.write_api(write_options=SYNCHRONOUS) as api:
            chunkSize = 10000
            print(f"writing {len(points)} points in chunks of {chunkSize}...")
            for i in range(0, len(points), chunkSize):
                api.write(args.bucket, record=points[i : i + chunkSize])
                print(f"wrote chunk {i // chunkSize}")


if __name__ == "__main__":
    main()
