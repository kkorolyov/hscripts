from functools import cache
from typing import Literal
from yfinance import Ticker


def value(commodity: str, date: str) -> float:
    match type(commodity):
        case "stock":
            return stockValue(commodity, date)
        case "tbill":
            return tbillValue(commodity, date)
        case _:
            return 1


def type(commodity: str):
    if commodity == "USD":
        return "base"
    elif "TBill" in commodity:
        return "tbill"
    else:
        return "stock"


@cache
def stockValue(symbol: str, date: str) -> float:
    # TODO
    # get from preloaded ticker at date
    return 0


@cache
def tbillValue(commodity: str, date: str) -> float:
    # TODO
    # parse maturity date from commodity
    return 0
