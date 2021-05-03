from config import config, client
import logging
from currencies import CurrencyUnit
from portfolio import Portfolio
import argparse
import matplotlib
matplotlib.use("TkAgg")
logging.basicConfig(filename='run_log.log', level=logging.DEBUG, filemode='w')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Displays profits from portfolio')
    parser.add_argument("--currency", type=str, default="CHF")
    args = parser.parse_args()
    return args


def main(args):
    if client.api.key is None:
        client.api.key = input("Enter API key:")
    if client.api.secret is None:
        client.api.secret = input("Enter secret:")
    currency = config["Portfolio"].get("currency", args.currency)
    portfolio = Portfolio.from_kraken_ledger(currency)

    portfolio.display(
        currency_unit=CurrencyUnit.create_currency_unit(currency))
    portfolio.plot_old_values()
    portfolio.plot_old_values(func_name="get_total_return")

if __name__ == "__main__":
    main(parse_args())
