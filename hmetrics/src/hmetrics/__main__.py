import subprocess
from argparse import ArgumentParser, RawTextHelpFormatter
from os import environ

from hmetrics import commodity
from hmetrics.register import Register


parser = ArgumentParser(
    description="Emits current ledger file metrics",
    formatter_class=RawTextHelpFormatter,
)
parser.add_argument("-i", "--input", type=str, help="ledger file to read")


def getAccounts(path: str):
    return (
        subprocess.check_output(["hledger", "accounts", "-f", path])
        .decode()
        .splitlines()
    )


def getRegister(path: str, account: str, delimiter: str):
    return Register(
        account,
        subprocess.check_output(
            ["hledger", "aregister", account, "-O", "tsv", "-f", path]
        )
        .decode()
        .splitlines(),
        delimiter,
    )


def toRows(registers: list[Register], delimiter: str):
    # bulk preload all stocks
    stockTransactions: dict[str, list[str]] = {}
    for register in registers:
        for row in register.transactions:
            for balance in row.balances:
                if commodity.typeOf(balance.commodity) == "stock":
                    entry = stockTransactions.setdefault(
                        balance.commodity, [row.date, row.date]
                    )
                    entry[0] = min(entry[0], row.date)
                    entry[1] = max(entry[1], row.date)
    for stock, (start, end) in stockTransactions.items():
        commodity.loadTicker(stock, start, end)

    # get all dates some transaction occurred
    dates = set()
    for register in registers:
        for date in register.dates():
            dates.add(date)
    dates = sorted(dates)

    # expand all registers to have transactions for all known dates
    for register in registers:
        register.fill(dates)

    return "\n".join(
        (
            delimiter.join(("date", "total", "account", "type")),
            *(
                delimiter.join(
                    (
                        transaction.date,
                        str(balance.amount),
                        register.account,
                        balance.commodity,
                    )
                )
                for register in registers
                for transaction in register.transactions
                for balance in transaction.balances
            ),
            *(
                delimiter.join(
                    (
                        transaction.date,
                        str(transaction.value()),
                        register.account,
                        "value",
                    )
                )
                for register in registers
                for transaction in register.transactions
            ),
        )
    )


def main():
    args = parser.parse_args()
    input = args.input or environ["LEDGER_FILE"]

    accounts = getAccounts(input)
    assets = [getRegister(input, t, "\t") for t in accounts if t.startswith("assets")]

    print(toRows(assets, "\t"))


if __name__ == "__main__":
    main()
