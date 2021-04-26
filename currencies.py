import requests
from forex_python.converter import CurrencyRates
from utils import URL_MARKET_PRICE, URL_MARKET_PRICE_FIAT, KRAKEN_PUBLIC_END_POINT, FIAT_CURRENCIES
from utils import CRYPTO_ALT_NAMES
from utils import url_join, save_data, get_pair_from_kraken, ts_format, read_data
from utils import get_cached as utils_get_cached
import json
import logging
from config import client, config
import pandas as pd
from numbers import Number


class CurrencyUnit():
    def __init__(self, name):
        self.name = name

    def convert(self, to, amount, date=None):
        raise NotImplementedError

    def create_currency(self, base_currency_unit=None):
        raise NotImplementedError

    def get_cached(self, to, date, attr="price"):
        expiration = float(config["Global"].get("ttl", 3600))
        price_data, requested_price = None, None
        if date is None:
            save_file = f"{self.name}_{to}_latest.pkl"
            price_data = utils_get_cached(save_file, expiration=expiration)
            if price_data is not None:
                requested_price = price_data["value"]
        else:
            date = ts_format(date)
            date = date.normalize()

            save_file = f"{self.name}_{to}.pkl"
            requested_price = None

            price_data = read_data(save_file, add_path_prefix=True)
            # No cached exchange rates
            if price_data is not None:
                # Check if the exchange rate for the requested date is available
                price_data = price_data["value"]
                requested_price = price_data.loc[date]
                if len(requested_price) > 0:
                    requested_price = float(
                        price_data[attr].iloc[0])

        return price_data, requested_price, date

    @classmethod
    def create_currency_unit(clf, name):
        if name in FIAT_CURRENCIES:
            return FiatUnit(name)
        else:
            return CryptoUnit(name)


class CryptoUnit(CurrencyUnit):

    def convert(self, to, amount, date=None):
        logging.debug(f"Convert {locals()}")
        to = to if isinstance(to, str) else to.name

        if to == self.name:
            return amount

        price_data, requested_price, date = self.get_cached(
            to, date, attr="close")

        if requested_price is None:
            requested_price_kraken = get_pair_from_kraken(
                self.name, to, client, date=date)
            if date == None:
                save_file = f"{self.name}_{to}_latest.pkl"
                requested_price = float(requested_price_kraken)
                save_data(requested_price, save_file)
            else:
                save_file = f"{self.name}_{to}.pkl"
                # Get the exchange rate for the closest date to the requested date
                requested_price = requested_price_kraken.index.get_loc(
                    date, method='nearest')
                requested_price = requested_price_kraken["close"].iloc[requested_price]
                if not isinstance(requested_price, Number):
                    requested_price = requested_price.iloc[0]

                requested_price_kraken = requested_price_kraken[["close"]]
                logging.debug(
                    f"Price data queried {price_data}: {type(price_data)}")

                save_data(requested_price_kraken, save_file)

        logging.debug(f"Price data {price_data}: {type(price_data)}")

        return requested_price*amount

    def create_currency(self, base_currency_unit=None):
        if base_currency_unit is None:
            base_currency_unit = self
        return CryptoCurrency(self.name, base_currency_unit)

    def __str__(self):
        return f"<CryptoUnit {self.name}>"

    def __repr__(self):
        return str(self)


class FiatUnit(CurrencyUnit):

    def convert(self, to, amount, date=None):
        logging.debug(f"Convert {locals()}")
        to = to if isinstance(to, str) else to.name

        if to == self.name:
            return amount

        price_data, requested_price, date = self.get_cached(to, date)

        if requested_price is None:
            if date == None:
                save_file = f"{self.name}_{to}_latest.pkl"

                price_url = url_join(URL_MARKET_PRICE_FIAT, "latest")
                requested_price = requests.get(price_url, params={
                    "base": self.name}).json()["rates"][to]
                requested_price = float(requested_price)
                save_data(requested_price, save_file, add_path_prefix=True)
            else:
                save_file = f"{self.name}_{to.name}.pkl"
                date_query = date.strftime(
                    "%Y-%m-%d")
                price_url = url_join(URL_MARKET_PRICE_FIAT, date_query)
                requested_price = requests.get(price_url, params={
                    "base": self.name})["rates"][to]
                requested_price = float(requested_price)
                new_value = pd.Series(
                    name=date, data={"price": requested_price}).to_frame()

                if price_data is None:
                    price_data = price_data.append(new_value)
                else:
                    price_data = new_value
                save_data(price_data, save_file, add_path_prefix=True)

        return requested_price*amount

    def create_currency(self, base_currency_unit=None):
        if base_currency_unit is None:
            base_currency_unit = self
        return FiatCurrency(self.name, base_currency_unit)

    def __str__(self):
        return f"<FiatUnit {self.name}>"

    def __repr__(self):
        return str(self)


class Currency():
    URL_MARKET_PRICE = url_join(KRAKEN_PUBLIC_END_POINT, "Ticker")

    def __init__(self, ticker, base_currency_unit):
        self.ticker = ticker
        self.base_currency_unit = base_currency_unit

        self.value = 0
        self.currency_unit = None
        self.total_invested = 0
        self.total_invested_up_now = 0
        self.realized_profit = 0
        self.compute_avg_base_price()

    def compute_avg_base_price(self):
        if self.value != 0:
            self.avg_base_price = self.total_invested/self.value
        else:
            self.avg_base_price = 0

    def get_total_invested(self, currency_unit=None):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
        return self.base_currency_unit.convert(currency_unit, self.total_invested)

    def get_all_return(self, currency_unit=None):
        ret = self.realized_profit + self.get_total_return()
        if currency_unit is not None:
            ret = self.base_currency_unit(currency_unit, ret)
        return ret

    def get_total_invested_up_now(self, currency_unit=None):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
        return self.base_currency_unit.convert(currency_unit, self.total_invested_up_now)

    def get_realized_return(self, currency_unit=None):
        ret = self.realized_profit
        if currency_unit is not None:
            ret = self.base_currency_unit.convert(currency_unit, ret)
        return ret

    def get_all_return_rate(self):
        if self.total_invested_up_now == 0:
            return 0
        return (self.realized_profit + self.get_total_return()) / self.total_invested_up_now

    def get_realized_return_rate(self):
        denom = (self.total_invested_up_now - self.total_invested)
        if denom == 0:
            return 0
        return self.realized_profit / denom

    def withdraw(self, value, fee=0, update_avg_price=True):
        logging.debug(f"Withdraw {self.ticker}")
        logging.debug(f"\t Withdraw Args: {locals()}")

        value_with_fee = value + fee
        if value_with_fee > self.value:
            raise ValueError(
                f"Not enough funds to withdraw {value} {self.ticker}")

        avg_value_with_fees = value_with_fee*self.avg_base_price

        self.value -= value_with_fee
        self.total_invested -= (value_with_fee*self.avg_base_price)
        if update_avg_price:
            self.compute_avg_base_price()
        logging.debug(f"Withdraw {self.ticker} finished")
        filtered_global_var = filter(lambda x: not (
            x.startswith("__") or callable(getattr(self, x))), dir(self))
        logging.debug(
            f"{self.ticker}_Portfolio State {list(map(lambda x: (x,getattr(self,x)),filtered_global_var))}")

        return (avg_value_with_fees, self.base_currency_unit)

    def sell(self, buy_currency, value_sold, value_bought, sell_fee=0,
             buy_fee=0, date=None):
        logging.debug(f"sell {self.ticker}")
        logging.debug(f"\t sell Args: {locals()}")
        avg_value_with_fees, _ = self.withdraw(
            value_sold, fee=sell_fee, update_avg_price=False)
        logging.debug(
            f"AVG VALUE WITH FEES {self.ticker + buy_currency.ticker}: {avg_value_with_fees}")
        realized_profit = buy_currency.currency_unit.convert(
            self.base_currency_unit, value_bought, date=date)
        logging.debug(
            f"Price at SELL {self.ticker + buy_currency.ticker}: {realized_profit}")
        realized_profit = realized_profit - avg_value_with_fees
        logging.debug(
            f"REALIZED PROFIT {self.ticker + buy_currency.ticker}: {realized_profit}")
        self.realized_profit += realized_profit
        buy_rate = value_sold/value_bought
        buy_currency.buy(self, value_bought, buy_rate, buy_fee=buy_fee,date=date)
        self.compute_avg_base_price()
        logging.debug(f"Sell {self.ticker} finished")
        filtered_global_var = filter(lambda x: not (
            x.startswith("__") or callable(getattr(self, x))), dir(self))
        logging.debug(
            f"{self.ticker}_Portfolio State {list(map(lambda x: (x,getattr(self,x)),filtered_global_var))}")

        return realized_profit, self.base_currency_unit

    def top_up(self, value, fee=0, avg_base_price=None,
               avg_base_price_currency_unit=None, update_avg_price=True,
               date=None):
        logging.debug(f"Top up with {self.ticker}")
        logging.debug(f"\t top up Args: {locals()}")
        if avg_base_price is None:
            avg_base_price = self.get_current_unit_value()
        elif avg_base_price_currency_unit is not None:
            avg_base_price = avg_base_price_currency_unit.convert(
                self.base_currency_unit, avg_base_price, date=date)

        value_with_fees = value - fee
        self.value += value_with_fees
        top_up_base = (value*avg_base_price)
        logging.debug(f"{self.ticker} Top up base value  {top_up_base}")
        self.total_invested += top_up_base
        self.total_invested_up_now += top_up_base
        if update_avg_price:
            self.compute_avg_base_price()

        logging.debug(f"{self.ticker} Top up finished {self}")
        filtered_global_var = filter(lambda x: not (
            x.startswith("__") or callable(getattr(self, x))), dir(self))
        logging.debug(
            f"{self.ticker}_Portfolio State {list(map(lambda x: (x,getattr(self,x)),filtered_global_var))}")

    def buy(self, from_currency, value, buy_rate, buy_fee=0, date=None):

        logging.debug(f"Buy {self.ticker}")
        logging.debug(f"\t buy Args: {locals()}")

        self.top_up(value, fee=buy_fee, avg_base_price=from_currency.avg_base_price*buy_rate,
                    avg_base_price_currency_unit=from_currency.base_currency_unit,
                    update_avg_price=True,
                    date=date)

        logging.debug(f"{self.ticker} Buy finished")
        filtered_global_var = filter(lambda x: not (
            x.startswith("__") or callable(getattr(self, x))), dir(self))
        logging.debug(
            f"{self.ticker}_Portfolio State {list(map(lambda x: (x,getattr(self,x)),filtered_global_var))}")

    def get_total_return(self, currency_unit=None):
        total_return = (self.get_current_value() - self.total_invested)
        if currency_unit is not None:
            total_return = self.base_currency_unit.convert(
                currency_unit, total_return)
        return total_return

    def get_return_rate(self):
        if self.total_invested == 0:
            return 0
        return self.get_total_return()/self.total_invested

    def get_current_value(self, currency_unit=None):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
            logging.debug(f"CU: {currency_unit}")
        return self.currency_unit.convert(currency_unit, self.value)

    def get_current_unit_value(self, currency_unit=None):
        if currency_unit is None:
            currency_unit = self.base_currency_unit
        return self.currency_unit.convert(currency_unit, 1)

    def display(self, verbose=True, tabulation="", currency_unit=None, tabulation_char="\t"):

        display_str = f"{tabulation}{self.ticker}:\n"
        current_val = self.get_current_value(currency_unit=currency_unit)
        total_invested = self.get_total_invested(currency_unit)
        total_invested_up_now = self.get_total_invested_up_now(
            currency_unit)

        display_str += f"{tabulation}{tabulation_char}Current value: {current_val} {currency_unit.name}\n"
        display_str += f"{tabulation}{tabulation_char}Invested: {total_invested} {currency_unit.name}\n"
        display_str += f"{tabulation}{tabulation_char}Invested all to now: {total_invested_up_now} {currency_unit.name}\n"
        profit = self.get_total_return(
            currency_unit=currency_unit)
        # realized_profit = self.get_realized_return(currency_unit)
        display_str += f"{tabulation}{tabulation_char}Unrealized Return: {profit} {currency_unit.name}\n"
        # display_str += f"{tabulation}{tabulation_char}Realized Return: {realized_profit} {currency_unit.name}\n"
        # total_profit = realized_profit + profit
        # display_str += f"{tabulation}{tabulation_char}Total Return: {total_profit} {currency_unit.name}\n"
        return_rate = self.get_return_rate()
        # realized_return_rate = self.get_realized_return_rate()
        # all_return_rate = self.get_all_return_rate()
        display_str += f"{tabulation}{tabulation_char}Return rate: {return_rate*100} %\n"
        # display_str += f"{tabulation}{tabulation_char}Realized Return rate: {realized_return_rate*100} %\n"
        # display_str += f"{tabulation}{tabulation_char}All Return rate: {all_return_rate*100} %\n"
        if verbose:
            print(display_str)
        return display_str

    def __str__(self):
        return f"<Currency_Portfolio with {self.value:.4f} {self.ticker}: invested -> {self.total_invested:.4f} {self.base_currency_unit} >"

    def __repr__(self):
        return str(self)


class FiatCurrency(Currency):
    def __init__(self, ticker, base_currency_unit):
        super().__init__(ticker, base_currency_unit)
        self.currency_unit = FiatUnit(ticker)

    def __str__(self):
        return f"<Fiat{super().__str__()[1:]}"

    def __repr__(self):
        return str(self)


class CryptoCurrency(Currency):
    def __init__(self, ticker, base_currency_unit):
        super().__init__(ticker, base_currency_unit)
        self.currency_unit = CryptoUnit(ticker)

    def __str__(self):
        return f"<Crypto{super().__str__()[1:]}"

    def __repr__(self):
        return str(self)
