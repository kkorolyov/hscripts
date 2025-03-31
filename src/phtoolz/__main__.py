from phtoolz.metrics.cli import cli as metricsCli
from phtoolz.stocks.cli import cli as stocksCli
from phtoolz.treas.cli import cli as treasCli
from phtoolz.vests.cli import cli as vestsCli


def metrics():
    metricsCli()


def stocks():
    stocksCli()


def treas():
    treasCli()


def vests():
    vestsCli()
