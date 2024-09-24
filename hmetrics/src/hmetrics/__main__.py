import datetime
from argparse import ArgumentParser, RawTextHelpFormatter
from os import environ

from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS

from hmetrics import commodity
from hmetrics.ledger import Ledger, Transaction

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


def _loadStocks(transactions: list[Transaction]):
    stockTransactions: dict[str, list[datetime.datetime]] = {}

    for transaction in transactions:
        if commodity.typeOf(transaction.commodity) == "stock":
            entry = stockTransactions.setdefault(
                transaction.commodity, [transaction.time, transaction.time]
            )
            entry[0] = min(entry[0], transaction.time)
            entry[1] = max(entry[1], transaction.time)

    for stock, (start, end) in stockTransactions.items():
        commodity.loadTicker(stock, start, end + datetime.timedelta(days=1))


def main():
    args = parser.parse_args()

    # fetch ledger data
    ledger = Ledger(args.input or environ["LEDGER_FILE"])

    accounts = ledger.accounts()
    print(f"found {len(accounts)} accounts")

    assets = ledger.transactions("assets")

    # combine all transactions for further processing
    transactions = [*assets]
    print(f"found {len(transactions)} transactions")

    # bulk preload all stocks
    _loadStocks(assets)

    # generate points to write
    points = [
        Point.measurement("transaction")
        .field("quantity", transaction.quantity)
        .field(
            "value",
            transaction.quantity
            * commodity.value(transaction.commodity, transaction.time),
        )
        .tag("account", transaction.account)
        .tag("commodity", transaction.commodity)
        .time(transaction.time)
        for transaction in transactions
    ]

    # write to InfluxDB
    with InfluxDBClient(args.url, args.token, org=args.org) as client:
        print(f"writing {len(points)} points...")
        with client.write_api(write_options=SYNCHRONOUS) as api:
            api.write(
                args.bucket,
                record=points,
            )


if __name__ == "__main__":
    main()
