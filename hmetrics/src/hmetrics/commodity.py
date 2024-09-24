import datetime
import re
from functools import cache
from typing import cast

from pandas import DataFrame, to_datetime
from yfinance import Ticker

_tickers: dict[str, DataFrame] = {}
_tbillPattern = re.compile(".*\\((.*) - (.*)\\)")


def loadTicker(symbol: str, start: datetime.datetime, end: datetime.datetime):
    """Fetches and stores ticker data for `symbol` from `start` to `end` (exclusive)"""
    _tickers[symbol] = Ticker(symbol).history(
        start=start,
        end=end,
        interval="1d",
    )


def value(commodity: str, time: datetime.datetime) -> float:
    """Returns the value of 1 unit of `commodity` at `time`."""

    match typeOf(commodity):
        case "stock":
            return _stockValue(commodity, time)
        case "tbill":
            return _tbillValue(commodity, time)
        case "bond":
            return 1
        case _:
            return 1


def typeOf(commodity: str):
    """Returns the general classification of `commodity`."""

    if commodity == "USD":
        return "base"
    elif "TBill" in commodity:
        return "tbill"
    elif "Bond" in commodity:
        return "bond"
    else:
        return "stock"


@cache
def _stockValue(symbol: str, time: datetime.datetime) -> float:
    # get from preloaded ticker at date
    df = _tickers[symbol]["Close"].tz_localize(None)
    return (
        cast(float, df[df.index.get_indexer([to_datetime(time)], method="pad")[0]])
        if len(df)
        else 0
    )


@cache
def _tbillValue(commodity: str, time: datetime.datetime) -> float:
    return 1
    # parse maturity date from commodity
    match = _tbillPattern.search(commodity)
    if match is None:
        raise RuntimeError(
            f"tbill '{commodity}' does not match pattern /{_tbillPattern}/"
        )

    start = match.group(1)
    end = match.group(2)

    return 0 if start <= time.date().isoformat() < end else 1
