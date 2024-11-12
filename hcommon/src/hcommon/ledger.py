"""Provides ledger data."""

import csv
import subprocess
from datetime import date
from decimal import Decimal
from typing import NamedTuple

from hcommon.commodity import CommodityValue


class Transaction(NamedTuple):
    """A change in quantity of a commodity in an account at some time."""

    time: date
    account: str
    commodity: str
    quantity: Decimal


class Ledger:
    """Returns ledger data from a given file."""

    path: str

    def __init__(self, path: str) -> None:
        """Returns a ledger reading from file at `path`."""

        self.path = path

    def accounts(self) -> list[str]:
        """Returns all the accounts in the ledger."""

        return (
            subprocess.check_output(["hledger", "accounts", "-f", self.path])
            .decode()
            .splitlines()
        )

    def prices(self, commodity: str) -> list[CommodityValue]:
        """Returns all commodity prices for `commodity` known or inferred in the ledger."""

        reader = csv.reader(
            subprocess.check_output(
                ["hledger", "prices", "--infer-market-prices", f"cur:{commodity}"]
            )
            .decode()
            .splitlines(),
            delimiter=" ",
        )

        values = [
            CommodityValue(date.fromisoformat(line[1]), commodity, Decimal(line[3]))
            for line in reader
        ]

        return list({(t.name, t.time): t for t in values}.values())

    def transactions(self) -> list[Transaction]:
        """Returns transactions from ledger."""

        # returns in format (txnidx date code description account amount total)
        reader = csv.reader(
            subprocess.check_output(
                ["hledger", "register", "-O", "tsv", "-f", self.path]
            )
            .decode()
            .splitlines(),
            delimiter="\t",
        )

        # skip headers
        next(reader)

        # combine transactions with same (account, date, commodity)
        transactions = dict[tuple[str, date, str], Transaction]()
        for line in reader:
            time = date.fromisoformat(line[1])
            account = line[4]

            quantityCommodity = line[5]
            if " " in quantityCommodity:
                splitI = quantityCommodity.index(" ")
                quantity = Decimal(quantityCommodity[:splitI])
                commodity = quantityCommodity[(splitI + 1) :].replace('"', "")
            else:
                quantity = Decimal(quantityCommodity)
                commodity = "USD"

            key = (account, time, commodity)
            if key in transactions:
                current = transactions[key]
                transactions[key] = Transaction(
                    time, account, commodity, current.quantity + quantity
                )
            else:
                transactions[key] = Transaction(time, account, commodity, quantity)

        return list(transactions.values())
