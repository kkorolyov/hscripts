import datetime
import re
from functools import cache
from typing import cast

from pandas import DataFrame, to_datetime
from yfinance import Ticker

_tickers: dict[str, DataFrame] = {}
_tbillPattern = re.compile(".*\\((.*) - (.*)\\)")


def loadTicker(symbol: str, start: str, end: str):
    _tickers[symbol] = Ticker(symbol).history(
        start=start,
        end=(datetime.datetime.strptime(end, "%Y-%m-%d") + datetime.timedelta(days=1))
        .date()
        .isoformat(),
        interval="1d",
    )


def value(commodity: str, date: str) -> float:
    """Returns the value of 1 unit of `commodity` at `date`."""

    match typeOf(commodity):
        case "stock":
            return _stockValue(commodity, date)
        case "tbill":
            return _tbillValue(commodity, date)
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
def _stockValue(symbol: str, date: str) -> float:
    # get from preloaded ticker at date
    df = _tickers[symbol]["Close"].tz_localize(None)
    return (
        cast(float, df[df.index.get_indexer([to_datetime(date)], method="pad")[0]])
        if len(df)
        else 0
    )


@cache
def _tbillValue(commodity: str, date: str) -> float:
    # parse maturity date from commodity
    match = _tbillPattern.search(commodity)
    if match is None:
        raise RuntimeError(
            f"tbill '{commodity}' does not match pattern /{_tbillPattern}/"
        )

    start = match.group(1)
    end = match.group(2)

    return 0 if start <= date < end else 1
