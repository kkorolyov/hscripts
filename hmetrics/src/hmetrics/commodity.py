"""Provides commodity data."""

from datetime import datetime
from typing import Iterable, Iterator, Literal, NamedTuple

from hmetrics.util import datetimeRangeDay
import yfinance


class CommodityValue(NamedTuple):
    """Value of 1 unit of a commodity at a particular time."""

    time: datetime
    name: str
    value: float


def values(
    commodities: Iterable[str], start: datetime, end: datetime
) -> Iterator[CommodityValue]:
    """Returns values of `commodities` from `start` (inclusive) to `end` (exclusive) on a 1-day interval."""

    byType: dict[Literal["intrinsic", "stock"], set[str]] = {}
    for commodity in commodities:
        byType.setdefault(typeOf(commodity), set()).add(commodity)

    # intrinsics are always 1
    for intrinsic in byType.get("intrinsic", set()):
        for time in datetimeRangeDay(start, end):
            yield CommodityValue(time, intrinsic, 1.0)

    # fetch stocks in batch
    for symbol, prices in yfinance.download(
        byType.get("stock", set()), start, end, interval="1d"
    ).Close.items():
        for timestamp, price in prices.items():
            yield CommodityValue(timestamp.to_pydatetime(), symbol, price)


def typeOf(commodity: str) -> Literal["intrinsic", "stock"]:
    """Returns the general classification of `commodity`."""

    if commodity == "USD" or "TBill" in commodity or "Bond" in commodity:
        return "intrinsic"
    else:
        return "stock"
