import json
import time
import os
import pickle
import requests
import logging
import pandas as pd
import numbers
from pykrakenapi.pykrakenapi import KrakenAPIError


def url_join(*urls):
    return '/'.join(url.strip('/') for url in urls)


def read_data(path, add_path_prefix=False):
    if add_path_prefix:
        path = os.path.join("./data", path)
    if not os.path.exists(path):
        return None
    return_val = None
    with open(path, "rb") as f:
        return_val = pickle.load(f)
    return return_val


def read_json(path):
    return_val = None
    with open(path, "r") as f:
        return_val = json.load(f)
    return return_val


def save_data(obj, path, add_path_prefix=True):
    obj = {"value": obj}
    if add_path_prefix:
        path = os.path.join("./data", path)
    obj["ts"] = time.time()
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def get_cached(file_name, expiration=3600):
    path = os.path.join("./data", file_name)
    if os.path.exists(path):
        json_dump = read_data(path)
        ts = json_dump["ts"]
        if (time.time() - ts) > expiration:
            return None
        else:
            return json_dump


def get_cached_df_row(file_name, key, expiration=3600, historical_data=False):
    df = get_cached(file_name, expiration=3600)
    if df is None:
        return None
    df = df["value"]
    if key not in df.index:
        return None
    row = df["value"][key]
    ts = row["ts"]
    if (not historical_data) and ((time.time() - ts) > expiration):
        return None
    return row


def save_row_to_cached_df(file_name, key, values):
    df_saved = get_cached(file_name, expiration=3600)
    values["ts"] = time.time()
    new_value = pd.Series(name=key, data=values).to_frame()
    if df_saved is None:
        df_saved = new_value
    else:
        df_saved = df_saved["value"]
        if key in df_saved.index:
            df_saved[key] = new_value
        else:
            df_saved.append(new_value)

    save_data(file_name, df_saved)


def ts_format(ts=None):
    if ts is None:
        return pd.to_datetime(time.time(), unit='s')
    elif not isinstance(ts, numbers.Number):
        return pd.to_datetime(ts, unit='s')
    else:
        return ts


KRAKEN_PUBLIC_END_POINT = "https://api.kraken.com/0/public/"
URL_MARKET_PRICE = url_join(KRAKEN_PUBLIC_END_POINT, "Ticker")
URL_MARKET_PRICE_FIAT = "https://api.ratesapi.io/api"
FIAT_CURRENCIES = read_json("./data/Common-Currency.json")
CRYPTO_ALT_NAMES = read_json("./data/crypto_ticker_converter.json")


def get_pair_from_kraken(from_curr, to_curr, client, date=None):
    new_name = CRYPTO_ALT_NAMES.get(from_curr, from_curr)
    logging.debug(f"Get pair {from_curr}/{to_curr} from Kraken")

    if (date is None) or (date == "latest"):
        return retry_kraken(get_latest_pair_from_kraken(client), new_name, to_curr, index_normalize=False)
    else:
        res = retry_kraken(get_ohlc_from_kraken(client),
                           new_name, to_curr)
        return res


def get_latest_pair_from_kraken(client):
    def inner_func(pair):
        price = float(client.get_ticker_information(pair)["c"].iloc[0][0])
        return price
    return inner_func


def get_ohlc_from_kraken(client):
    def inner_func(pair):
        price = client.get_ohlc_data(pair, interval=1440)[0]
        return price
    return inner_func


def retry_kraken(func, from_currency, to_currency, index_normalize=True, *args, **kwargs):
    """If a direct conversion is not available convert to proxy_currencies_first first"""
    pair = f"{from_currency}{to_currency}"

    try:
        price_data = func(pair=pair, *args, **kwargs)

    except KrakenAPIError:
        logging.debug(
            f"Failed to convert {from_currency} to {to_currency}: Trying to go through a proxy currency")

        price_data_1,price_data_2= retry_conversion(func, from_currency, to_currency, *args, **kwargs)

        if index_normalize:
            price_data_1.index = price_data_1.index.normalize()
            price_data_2.index = price_data_2.index.normalize()
            if len(price_data_1) < len(price_data_2):
                price_data_2 = price_data_2.loc[price_data_1.index]
            else:
                price_data_1 = price_data_1.loc[price_data_2.index]
        price_data = price_data_1*price_data_2

    return price_data


def retry_conversion(func,from_currency, to_currency, *args, **kwargs):
    proxy_currencies = ["USD", "EUR", "GBP"]
    pair_1,pair_2 = None,None
    for currency in proxy_currencies:
        pair_1 = f"{from_currency}{currency}"
        pair_2 = f"{currency}{to_currency}"
        try:
            price_data_1 = func(pair=pair_1, *args, **kwargs)

            logging.debug(f"{pair_1} conversion Successfull")
            price_data_2 = func(pair=pair_2, *args, **kwargs)
            logging.debug(f"{pair_2} conversion Successfull")
        except:
            continue
    if (price_data_1 is None) or (price_data_2 is None):
        raise ValueError(
                f"Cannot convert {from_currency} to {to_currency}")
    return price_data_1, price_data_2
