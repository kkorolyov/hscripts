import csv
from functools import total_ordering
from typing import Iterable, Union

from hmetrics import commodity


class Balance:
    """A commodity amount."""

    amount: float
    commodity: str

    def __init__(self, data: str) -> None:
        if " " in data:
            splitI = data.index(" ")
            self.amount = float(data[:splitI])
            self.commodity = data[(splitI + 1) :].replace('"', "")
        else:
            self.amount = float(data)
            self.commodity = "USD"

    def __repr__(self) -> str:
        return f"{self.amount} {self.commodity}"


@total_ordering
class Transaction:
    """Balances at a particular date."""

    date: str
    balances: list[Balance]

    def __init__(self, data: Union[list[str], "Transaction"]) -> None:
        if isinstance(data, Transaction):
            self.date = data.date
            self.balances = data.balances
        else:
            self.date = data[1]
            self.balances = [Balance(t) for t in data[6].split(", ")]

    def value(self) -> float:
        """Returns the total value of this row."""

        total = 0
        for balance in self.balances:
            total += balance.amount * commodity.value(balance.commodity, self.date)
        return total

    def __eq__(self, value: object) -> bool:
        return isinstance(value, Transaction) and self.date == value.date

    def __lt__(self, value: object) -> bool:
        return isinstance(value, Transaction) and self.date < value.date

    def __repr__(self) -> str:
        return f"{self.date} {self.balances}"


class Register:
    """Transactions for a particular account."""

    account: str
    transactions: list[Transaction]

    def __init__(self, account: str, data: Iterable[str], delimiter: str) -> None:
        self.account = account

        reader = csv.reader(data, delimiter=delimiter)
        # skip headers
        next(reader)

        self.transactions = sorted(Transaction(t) for t in reader)

    def dates(self) -> set[str]:
        """Returns all unique transaction dates in this register."""

        return set(t.date for t in self.transactions)

    def fill(self, dates: list[str]):
        """
        Generates filler rows for provided dates not matching those already in this register.
        If the filler row would have a value of `0`, it is omitted.
        """

        if not len(self.transactions) or not len(dates):
            return

        newTransactions = []

        datesI = 0
        transactionsI = 0

        while datesI < len(dates):
            date = dates[datesI]
            transaction = self.transactions[
                min(len(self.transactions) - 1, transactionsI)
            ]

            # backfill
            if date < transaction.date:
                # only backfill between entries
                if transactionsI > 0:
                    # if next entry non-0
                    if transaction.value() != 0:
                        # fill with the next nearest row
                        newRow = Transaction(transaction)
                        newRow.date = date
                        newTransactions.append(newRow)
                        # increment the fill cursor
                        datesI += 1
                    else:
                        # get to next matching date
                        while date < transaction.date:
                            datesI += 1
                            date = dates[datesI]
                else:
                    datesI += 1
            elif date > transaction.date:
                # infill
                if transactionsI < len(self.transactions) - 1:
                    # infill until matches date
                    while (
                        transactionsI < len(self.transactions)
                        and date > transaction.date
                    ):
                        newTransactions.append(transaction)
                        transactionsI += 1
                        if transactionsI < len(self.transactions):
                            transaction = self.transactions[transactionsI]
                # postfill
                else:
                    # fill with prev only if non-0
                    if transaction.value() != 0:
                        # fill with the prev nearest row for the remaining dates
                        while datesI < len(dates):
                            newRow = Transaction(transaction)
                            newRow.date = date
                            newTransactions.append(newRow)
                            # increment fill cursor
                            datesI += 1
                            if datesI < len(dates):
                                date = dates[datesI]
                    # no more fill needed - exit
                    else:
                        break
            # dates match - just copy current row and continue
            else:
                newTransactions.append(transaction)
                datesI += 1
                transactionsI += 1

        self.transactions = newTransactions

    def __repr__(self) -> str:
        return f"{self.account} {self.transactions}"
