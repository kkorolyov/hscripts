"""Provides commodity data."""

import re
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Iterable, Iterator, Literal, NamedTuple

import yfinance
from pandas import isna

from phtoolz.common.util import dateRange

_tBillPattern = re.compile(r"^.*\((.*) - (.*)\).*$")
_stockPattern = re.compile(r"^[A-Z]+$")


class CommodityValue(NamedTuple):
    """Value of 1 unit of a commodity at a particular time."""

    time: date
    name: str
    value: Decimal


def values(
    commodities: Iterable[str], start: date, end: date
) -> Iterator[CommodityValue]:
    """Returns values of `commodities` from `start` (inclusive) to `end` (exclusive) on a 1-day interval."""

    byType = defaultdict[Literal["intrinsic", "tbill", "stock", "other"], set[str]](set)
    for commodity in commodities:
        byType[typeOf(commodity)].add(commodity)

    # intrinsics are always 1
    for intrinsic in byType["intrinsic"]:
        for time in dateRange(start, end):
            yield CommodityValue(time, intrinsic, Decimal(1))

    # use tbills' lifetime
    for tbill in byType["tbill"]:
        match = _tBillPattern.match(tbill)
        if match is None:
            raise RuntimeError(f"tbill does not match pattern: {tbill}")
        else:
            for time in dateRange(
                max(start, date.fromisoformat(match.group(1))),
                min(end, date.fromisoformat(match.group(2))),
            ):
                yield CommodityValue(time, tbill, Decimal(1))

    # fetch stocks in batch
    if len(byType["stock"]):
        for symbol, prices in yfinance.download(
            byType["stock"], start, end, interval="1d"
        ).Close.items():
            for timestamp, price in prices.items():
                if not isna(price):
                    yield CommodityValue(
                        timestamp.to_pydatetime().date(), symbol, Decimal(price)
                    )


def typeOf(commodity: str):
    """Returns the general classification of `commodity`."""

    if commodity == "USD" or "Bond" in commodity:
        return "intrinsic"
    elif "TBill" in commodity:
        return "tbill"
    elif _stockPattern.match(commodity):
        return "stock"
    else:
        return "other"
