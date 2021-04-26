import krakenex
from pykrakenapi import KrakenAPI
import configparser


def create_client_from_config(config):
    retry = config["API"].get("retry", 0)
    crl_sleep = config["API"].get("crl_sleep", 5)
    tier = config["API"].get("tier", "Intermediate")
    api_key = config["API"].get("key", None)
    secret = config["API"].get("secret", None)
    client = krakenex.API(key=api_key, secret=secret)
    client = KrakenAPI(client, retry=float(retry), crl_sleep=float(crl_sleep), tier=tier)
    return client


# Read local file `config.ini`.
config = configparser.ConfigParser()
config.read('./config.ini')
client = create_client_from_config(config)
