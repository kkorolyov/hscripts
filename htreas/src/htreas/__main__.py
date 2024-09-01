import argparse
import re
import subprocess


TREASURY_PREFIX = "TBill"

parser = argparse.ArgumentParser(
    description="Updates treasuries in ledger files",
    formatter_class=argparse.RawTextHelpFormatter,
)
parser.add_argument(
    "-i",
    "--input",
    type=str,
    required=True,
    help="ledger file to read commodities from",
)
parser.add_argument(
    "-o", "--output", type=str, required=True, help="ledger file to write output to"
)


def fetchCommodities(path: str):
    allCommodities = (
        subprocess.check_output(["hledger", "commodities", "-f", path])
        .decode()
        .splitlines()
    )
    return set(t for t in allCommodities if t.startswith(TREASURY_PREFIX))


def fetchTreasuries(path: str):
    with open(path) as f:
        lines = f.read().splitlines()

    pattern = re.compile('.*"(.*)".*')

    return set(pattern.search(t).group(1) for t in lines if t)  # type: ignore


def formatTreasury(text: str):
    pattern = re.compile(".*\\((.*) - (.*)\\)")

    match = pattern.search(text)

    if match is None:
        raise RuntimeError(f"treasury does not match pattern: {text}")
    else:
        start = match.group(1)
        end = match.group(2)

        return [f'P {start} "{text}" 0', f'P {end} "{text}" 1']


def main():
    args = parser.parse_args()
    input = args.input
    output = args.output

    commodities = fetchCommodities(input)
    treasuries = fetchTreasuries(output)

    newTreasuries = [formatTreasury(t) for t in commodities if t not in treasuries]
    newLines = [t for group in newTreasuries for t in group]

    print(f"writing {len(newLines)} lines: {newLines}")

    if len(newLines):
        with open(output, "a") as f:
            f.write("\n".join(["", *newLines, ""]))
