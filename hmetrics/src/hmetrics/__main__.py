import subprocess
from argparse import ArgumentParser, RawTextHelpFormatter
from os import environ

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


def combineRegisters(registers: list[Register], delimiter: str):
    if not len(registers):
        return ""

    dates = set()
    for register in registers:
        for date in register.dates():
            dates.add(date)
    dates = sorted(dates)

    for register in registers:
        register.fill(dates)

    return "\n".join(
        (
            delimiter.join(("date", "total", "account")),
            *(
                delimiter.join((row.date, str(row.value()), register.account))
                for register in registers
                for row in register.rows
            ),
        )
    )


def main():
    args = parser.parse_args()
    input = args.input or environ["LEDGER_FILE"]

    accounts = getAccounts(input)
    assets = [getRegister(input, t, "\t") for t in accounts if t.startswith("assets")]

    print(combineRegisters(assets, "\t"))
