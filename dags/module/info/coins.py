from enum import Enum


class Coins(Enum):
    BTC = "BTC"
    BCH = "BCH"
    ETH = "ETH"
    XRP = "XRP"
    LTC = "LTC"
    ADA = "ADA"
    EOS = "EOS"
    XLM = "XLM"
    TRX = "TRX"
    BNB = "BNB"

    @classmethod
    def list(cls):
        return [coin.value for coin in cls]
