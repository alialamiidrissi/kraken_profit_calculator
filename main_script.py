import argparse
from portfolio import Portfolio
import logging
from config import config,client
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
    portfolio = Portfolio.from_kraken_ledger(config["Portfolio"].get("currency",args.currency))
    portfolio.display()


if __name__ == "__main__":
    main(parse_args())
