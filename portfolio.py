from currencies import CryptoCurrency, FiatCurrency, CurrencyUnit
import pandas as pd
import numbers
from utils import FIAT_CURRENCIES
import time
import krakenex
from pykrakenapi import KrakenAPI
from utils import get_cached, save_data, ts_format, read_data
import logging
from config import client, config
import copy


class Portfolio():
    def __init__(self, base_currency_unit):
        self.securities = {}
        self.base_currency_unit = base_currency_unit
        self._total_invested = 0
        self.update_time()
        self.creation_time = ts_format()
        self.first_transaction_time = None
        self.last_checkpoint = None
        self.realized_profit = 0
        self.total_invested_up_now = 0

    def update_time(self, ts=None):
        self.last_update_time = ts_format(ts)

    def get_total_return(self, currency_unit=None, from_currency=False):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
        if from_currency:
            return sum(map(lambda x: x.get_total_return(currency_unit), self.securities.values()))
        else:
            return self.get_current_value(currency_unit) - self._total_invested

    def get_return_rate(self, from_currency=False):
        result = self.get_total_return(from_currency=from_currency)
        denom = self.get_current_value()
        if denom == 0:
            return 0
        result /= denom

        return result

    def get_current_value(self, currency_unit=None):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
        return sum(map(lambda x: x.get_current_value(currency_unit), self.securities.values()))

    def get_total_invested(self, currency_unit=None, from_currency=False, update_from_currency=False):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
        if from_currency or update_from_currency:

            return_value = sum(map(lambda x: x.total_invested(
                currency_unit), self.securities.values()))
            if update_from_currency:
                self._total_invested = return_value
            return return_value
        else:
            return self._total_invested

    def top_up(self, value, currency_unit, fee=0, avg_base_price=None,
               avg_base_price_currency_unit=None, update_avg_price=True,
               timestamp=None):
        logging.debug(f"Top up of Portfolio started")
        logging.debug(f"\t top up Args: {locals()}")
        if currency_unit.name not in self.securities:
            self.securities[currency_unit.name] = currency_unit.create_currency()

        if avg_base_price is None:
            avg_base_price = currency_unit.convert(self.base_currency_unit, 1)
        elif avg_base_price_currency_unit is not None:
            avg_base_price = avg_base_price_currency_unit.convert(
                self.base_currency_unit, avg_base_price)

        self.securities[currency_unit.name].top_up(value, fee=fee, avg_base_price=avg_base_price,
                                                   avg_base_price_currency_unit=avg_base_price_currency_unit,
                                                   update_avg_price=update_avg_price,
                                                   date=timestamp.normalize())
        self._total_invested += avg_base_price*value
        self.total_invested_up_now += avg_base_price*value

        if self.last_checkpoint is None:
            self.first_transaction_time = timestamp
        self.last_checkpoint = copy.deepcopy(self)
        self.update_time(timestamp)

        logging.debug(f"Top up finished {self}")
        filtered_global_var = filter(lambda x: not (
            x.startswith("__") or callable(getattr(self, x))), dir(self))
        logging.debug(
            f"Portfolio State {list(map(lambda x: (x,getattr(self,x)),filtered_global_var))}")

    def trade(self, value_sold, sell_currency_unit,
              value_bought, buy_currency_unit, sell_fee=0, buy_fee=0,
              timestamp=None):
        logging.debug(f"Trade order started")
        logging.debug(f"\t Trade Args: {locals()}")
        logging.debug(f"\t Current securities: {self.securities}")
        if sell_currency_unit.name not in self.securities:
            raise ValueError(f"No {sell_currency_unit.name} to sell")
        sell_currency = self.securities[sell_currency_unit.name]

        if buy_currency_unit.name not in self.securities:
            new_currency = buy_currency_unit.create_currency(
                sell_currency.base_currency_unit)
            self.securities[buy_currency_unit.name] = new_currency
        buy_currency = self.securities[buy_currency_unit.name]

        realized_profit, profit_currency_unit = sell_currency.sell(buy_currency,
                                                                   value_sold,
                                                                   value_bought,
                                                                   sell_fee=sell_fee,
                                                                   buy_fee=buy_fee,
                                                                   date=timestamp.normalize()
                                                                   )
        logging.debug(f"PORTFOLIO PROFIT {realized_profit}")

        self.realized_profit += profit_currency_unit.convert(self.base_currency_unit,
                                                             realized_profit)
        logging.debug(f"TOTAL PORTFOLIO PROFIT {self.realized_profit}")

        if self.last_checkpoint is None:
            self.first_transaction_time = timestamp
        self.last_checkpoint = copy.deepcopy(self)
        self.update_time(timestamp)
        logging.debug(f"Trade order finished {self}")
        filtered_global_var = filter(lambda x: not (
            x.startswith("__") or callable(getattr(self, x))), dir(self))
        logging.debug(
            f"Portfolio State {list(map(lambda x: (x,getattr(self,x)),filtered_global_var))}")

    def get_total_invested_up_now(self, currency_unit=None, from_currency=False):
        if from_currency:
            raise NotImplementedError

        if currency_unit is None:
            currency_unit = self.base_currency_unit
        return self.base_currency_unit.convert(currency_unit, self.total_invested_up_now)

    def get_realized_return(self, currency_unit=None, from_currency=False):
        if from_currency:
            raise NotImplementedError
        ret = self.realized_profit
        if currency_unit is not None:
            ret = self.base_currency_unit.convert(currency_unit, ret)
        return ret

    def get_all_return_rate(self, from_currency=False):
        if from_currency:
            raise NotImplementedError
        if self.total_invested_up_now == 0:
            return 0
        return (self.realized_profit + self.get_total_return()) / self.total_invested_up_now

    def get_realized_return_rate(self, from_currency=False):
        if from_currency:
            raise NotImplementedError
        denom = (self.total_invested_up_now - self._total_invested)
        if denom == 0:
            return 0
        return self.realized_profit / denom

    def display(self, verbose=True, tabulation="", currency_unit=None, tabulation_char="\t"):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
        current_val = self.get_current_value(currency_unit=currency_unit)
        total_invested = self.get_total_invested(currency_unit)
        total_invested_up_now = self.get_total_invested_up_now(
            currency_unit)

        display_str = f"{tabulation}Current value: {current_val} {currency_unit.name}\n"
        display_str += f"{tabulation}Invested: {total_invested} {currency_unit.name}\n"
        display_str += f"{tabulation}Invested all to now: {total_invested_up_now} {currency_unit.name}\n"
        profit = self.get_total_return(
            currency_unit=currency_unit)

        # realized_profit = self.get_realized_return(currency_unit)
        display_str += f"{tabulation}Unrealized Return: {profit} {currency_unit.name}\n"
        # display_str += f"{tabulation}Realized Return: {realized_profit} {currency_unit.name}\n"
        # total_profit = realized_profit + profit
        # display_str += f"{tabulation}Total Return: {total_profit} {currency_unit.name}\n"
        return_rate = self.get_return_rate()
        # realized_return_rate = self.get_realized_return_rate()
        all_return_rate = self.get_all_return_rate()
        display_str += f"{tabulation}Return rate: {return_rate*100} %\n"
        # display_str += f"{tabulation}Realized Return rate: {realized_return_rate*100} %\n"
        display_str += f"{tabulation}All Return rate: {all_return_rate*100} %\n"

        new_tab = tabulation + tabulation_char
        if verbose:
            print(display_str)
        for security in self.securities.values():

            display_str += security.display(verbose=verbose, tabulation=new_tab,
                                            currency_unit=currency_unit, tabulation_char=tabulation_char)

        return display_str

    @classmethod
    def from_kraken_ledger(clf, base_currency_ticker):
        ttl = int(config["Global"].get("ttl", 3600))
        cached_portfolio_path = "cached_portfolio.pkl"
        cached_ledger_path = "cached_ledger.pkl"
        portfolio = read_data(cached_portfolio_path, add_path_prefix=True)
        trades_history = get_cached(cached_ledger_path, expiration=ttl)
        if portfolio is None:
            start = None
            portfolio = clf(
                CurrencyUnit.create_currency_unit(base_currency_ticker))
        else:
            portfolio = portfolio["value"]
            start = portfolio.last_update_time
            logging.debug(f"Loaded portfolio from cache with last update time = {start}")


        if trades_history is None:

            trades_history, _ = client.get_ledgers_info(
                ascending=True, start=start)
            save_data(trades_history, cached_ledger_path)
        else:
            trades_history = trades_history["value"]
            trades_history = trades_history[trades_history.index > portfolio.last_update_time]

        last_entry = None
        for ts, entry in trades_history.iterrows():
            logging.debug(str((ts, entry.asset, portfolio.securities)))
            if last_entry is not None and entry.type != "trade":
                raise ValueError("Corrupted ledger")
            if entry.type == "deposit":

                asset_name = entry.asset
                currencies_unit = CurrencyUnit.create_currency_unit(asset_name)
                portfolio.top_up(abs(entry.amount), currencies_unit, fee=abs(entry.fee),
                                 timestamp=ts
                                 )
            elif entry.type == "trade":
                if last_entry is None:
                    last_entry = entry
                else:
                    if last_entry.amount < 0:
                        value_sold = abs(last_entry.amount)
                        sell_currency_unit = CurrencyUnit.create_currency_unit(
                            last_entry.asset)
                        sell_fee = last_entry.fee
                        if entry.amount < 0:
                            raise ValueError("Corrupted ledger")
                        value_bought = entry.amount
                        buy_currency_unit = CurrencyUnit.create_currency_unit(
                            entry.asset)
                        buy_fee = entry.fee
                    else:
                        value_sold = abs(entry.amount)
                        sell_currency_unit = CurrencyUnit.create_currency_unit(
                            entry.asset)
                        sell_fee = entry.fee
                        if last_entry.amount < 0:
                            raise ValueError("Corrupted ledger")
                        value_bought = last_entry.amount
                        buy_currency_unit = CurrencyUnit.create_currency_unit(
                            last_entry.asset)
                        buy_fee = last_entry.fee

                    last_entry = None

                    timestamp = ts
                    portfolio.trade(value_sold, sell_currency_unit,
                                    value_bought, buy_currency_unit, sell_fee=sell_fee, buy_fee=buy_fee,
                                    timestamp=timestamp)

        save_data(portfolio, cached_portfolio_path)

        return portfolio

    def __str__(self):
        return f"<Portfolio with {self._total_invested} {self.base_currency_unit}: {list(self.securities.values())}>"

    def __repr__(self):
        return str(self)
