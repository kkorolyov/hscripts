import argparse
import re
from phtoolz.common.ledger import Ledger

TREASURY_PREFIX = "TBill"

parser = argparse.ArgumentParser(
    description="Updates treasuries in ledger files",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-i",
    "--input",
    type=str,
    help="ledger file to read",
)
parser.add_argument(
    "-o", "--output", type=str, required=True, help="ledger file to write"
)


def fetchCommodities(ledger: Ledger):
    return set(t for t in ledger.commodities() if t.startswith(TREASURY_PREFIX))


def fetchTreasuries(path: str):
    with open(path) as f:
        lines = f.read().splitlines()

    pattern = re.compile('.*"(.*)".*')

    return set(pattern.search(t).group(1) for t in lines if t)  # type: ignore


def formatTreasury(text: str):
    pattern = re.compile(r".*\((.*) - (.*)\)")

    match = pattern.search(text)

    if match is None:
        raise RuntimeError(f"treasury does not match pattern: {text}")
    else:
        start = match.group(1)
        end = match.group(2)

        return [f'P {start} "{text}" 0', f'P {end} "{text}" 1']


def cli():
    args = parser.parse_args()

    ledger = Ledger(args.input)
    commodities = fetchCommodities(ledger)
    treasuries = fetchTreasuries(args.output)

    newTreasuries = [formatTreasury(t) for t in commodities if t not in treasuries]
    newLines = [t for group in newTreasuries for t in group]

    print(f"writing {len(newLines)} lines: {newLines}")
    if len(newLines):
        with open(args.output, "a") as f:
            f.write("\n".join(["", *newLines, ""]))
