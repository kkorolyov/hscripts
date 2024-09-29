"""Provides commodity data."""

import re
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable, Iterator, Literal, NamedTuple

import yfinance
from pandas import isna

from hmetrics.util import datetimeRangeDay

_tBillPattern = re.compile(".*\\((.*) - (.*)\\)")


class CommodityValue(NamedTuple):
    """Value of 1 unit of a commodity at a particular time."""

    time: datetime
    name: str
    value: Decimal


def values(
    commodities: Iterable[str], start: datetime, end: datetime
) -> Iterator[CommodityValue]:
    """Returns values of `commodities` from `start` (inclusive) to `end` (exclusive) on a 1-day interval."""

    byType = defaultdict[Literal["intrinsic", "tbill", "stock"], set[str]](set)
    for commodity in commodities:
        byType[typeOf(commodity)].add(commodity)

    # intrinsics are always 1
    for intrinsic in byType["intrinsic"]:
        for time in datetimeRangeDay(start, end):
            yield CommodityValue(time, intrinsic, Decimal(1))

    # use tbills' lifetime
    for tbill in byType["tbill"]:
        match = _tBillPattern.search(tbill)
        if match is None:
            raise RuntimeError(f"tbill does not match pattern: {tbill}")
        else:
            for time in datetimeRangeDay(
                max(
                    start,
                    datetime.combine(
                        date.fromisoformat(match.group(1)), datetime.min.time()
                    ),
                ),
                min(
                    end,
                    datetime.combine(
                        date.fromisoformat(match.group(2)), datetime.min.time()
                    ),
                ),
            ):
                yield CommodityValue(time, tbill, Decimal(1))

    # fetch stocks in batch
    for symbol, prices in yfinance.download(
        byType["stock"], start, end, interval="1d"
    ).Close.items():
        for timestamp, price in prices.items():
            if not isna(price):
                yield CommodityValue(timestamp.to_pydatetime(), symbol, Decimal(price))


def typeOf(commodity: str):
    """Returns the general classification of `commodity`."""

    if commodity == "USD" or "Bond" in commodity:
        return "intrinsic"
    elif "TBill" in commodity:
        return "tbill"
    else:
        return "stock"
