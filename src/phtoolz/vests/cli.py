import argparse


parser = argparse.ArgumentParser(
    description="Emits hledger forecast transactions for sell-to-cover vesting",
    formatter_class=argparse.RawTextHelpFormatter,
)
parser.add_argument("-p", "--period", type=str, required=True, help="period expression")
parser.add_argument(
    "-s", "--shares", type=int, required=True, help="number of shares vesting"
)
parser.add_argument("-u", "--unit", type=str, required=True, help="stock unit")
parser.add_argument("-c", "--company", type=str, required=True, help="income company")
parser.add_argument(
    "-a", "--account", type=str, required=True, help="investment account"
)


def formatVestForecast(period: str, shares: int, unit: str, company: str, account: str):
    return f"""\
~ {period}  Vest
  income:{company}                          -{shares} {unit}
  expenses:taxes:oasdi                  {round(shares * .062)} {unit}
  expenses:taxes:medicare               {round(shares * .0145)} {unit}
  expenses:taxes:federal                {round(shares * .22)} {unit}
  expenses:taxes:state                  {round(shares * .1023)} {unit}
  expenses:taxes:vdi                    {round(shares * .0103)} {unit}
  assets:investment:{account}"""


def cli():
    args = parser.parse_args()
    print(
        formatVestForecast(
            args.period, args.shares, args.unit, args.company, args.account
        )
    )
