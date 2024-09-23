import csv
from functools import total_ordering
from typing import Iterable, Union

from hmetrics import commodity


class Balance:
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

    def value(self, date: str) -> float:
        """Returns the total value of this balance at `date`."""

        return self.amount * commodity.value(self.commodity, date)

    def __repr__(self) -> str:
        return f"{self.amount} {self.commodity}"


@total_ordering
class Row:
    date: str
    balances: list[Balance]

    def __init__(self, data: Union[list[str], "Row"]) -> None:
        if isinstance(data, Row):
            self.date = data.date
            self.balances = data.balances
        else:
            self.date = data[1]
            self.balances = [Balance(t) for t in data[6].split(", ")]

    def value(self) -> float:
        """Returns the total value of this row."""

        total = 0
        for balance in self.balances:
            total += balance.value(self.date)
        return total

    def __eq__(self, value: object) -> bool:
        return isinstance(value, Row) and self.date == value.date

    def __lt__(self, value: object) -> bool:
        return isinstance(value, Row) and self.date < value.date

    def __repr__(self) -> str:
        return f"{self.date} {self.balances}"


class Register:
    account: str
    rows: list[Row]

    def __init__(self, account: str, data: Iterable[str], delimiter: str) -> None:
        self.account = account

        reader = csv.reader(data, delimiter=delimiter)
        # skip headers
        next(reader)

        self.rows = sorted(Row(t) for t in reader)

    def dates(self) -> set[str]:
        """Returns all unique transaction dates in this register."""

        return set(t.date for t in self.rows)

    def fill(self, dates: list[str]):
        """Generates filler rows for provided dates not matching those already in this register."""

        if not len(self.rows) or not len(dates):
            return

        newRows = []

        datesI = 0
        rowsI = 0

        while datesI < len(dates):
            date = dates[datesI]
            row = self.rows[min(len(self.rows) - 1, rowsI)]
            rowDate = row.date

            # backfill
            if date < rowDate:
                # only backfill between entries
                if rowsI > 0:
                    # if next entry non-0
                    if row.value() != 0:
                        # fill with the next nearest row
                        newRow = Row(row)
                        newRow.date = date
                        newRows.append(newRow)
                        # increment the fill cursor
                        datesI += 1
                    else:
                        # get to next matching date
                        while date < rowDate:
                            datesI += 1
                            date = dates[datesI]
                else:
                    datesI += 1
            elif date > rowDate:
                # infill
                if rowsI < len(self.rows) - 1:
                    # infill until matches date
                    while rowsI < len(self.rows) and date > rowDate:
                        newRows.append(row)
                        rowsI += 1
                        if rowsI < len(self.rows):
                            row = self.rows[rowsI]
                            rowDate = row.date
                # postfill
                else:
                    # fill with prev only if non-0
                    if row.value() != 0:
                        # fill with the prev nearest row for the remaining dates
                        while datesI < len(dates):
                            newRow = Row(row)
                            newRow.date = date
                            newRows.append(newRow)
                            # increment fill cursor
                            datesI += 1
                            if datesI < len(dates):
                                date = dates[datesI]
                    # no more fill needed - exit
                    else:
                        break
            # dates match - just copy current row and continue
            else:
                newRows.append(row)
                datesI += 1
                rowsI += 1

        self.rows = newRows

    def __repr__(self) -> str:
        return f"{self.account} {self.rows}"
