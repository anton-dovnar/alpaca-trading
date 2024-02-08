from datetime import datetime, timedelta, time

import pandas as pd
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus
from alpaca.trading.enums import AssetClass
from alpaca.trading.enums import AssetExchange
from alpaca.trading.requests import GetCalendarRequest
from alpaca.data.enums import Adjustment, DataFeed


class Scanner:
    _instance = None
    _is_initialized = False

    def __new__(
        cls,
        trading_client: TradingClient,
        data_client: StockHistoricalDataClient
    ):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self, trading_client: TradingClient, data_client: StockHistoricalDataClient
    ) -> None:
        if not self._is_initialized:
            self._is_initialized = True
            self.trading_client = trading_client
            self.data_client = data_client

    def get_all_symbols(self) -> list[str]:
        request = GetAssetsRequest(
            status=AssetStatus.ACTIVE,
            asset_class=AssetClass.US_EQUITY,
            exchange=AssetExchange.NASDAQ
        )
        assets = self.trading_client.get_all_assets(request)
        symbols = []

        for asset in assets:
            if asset.tradable:
                symbols.append(asset.symbol)

        return symbols

    def clean_data(self, data) -> pd.DataFrame:
        """ Clean the data """

        data = data.df
        data.columns = data.columns.str.title()
        data = data.reset_index()
        data = data.pivot(index="timestamp", columns="symbol")
        data.index = pd.to_datetime(data.index)
        data.index = data.index.tz_convert("US/Eastern")

        # Exclude postmarket hours data
        data = data[data.index.time < time(16, 0)]
        conditions = (data["Close"] > 0.8) & (data["Close"] < 20)
        data = data[conditions[conditions]].dropna(axis=1, how="all")
        return data

    def get_year_data(self) -> pd.DataFrame:
        """ Retrieve the data for all stocks for the past year  """

        symbols = self.get_all_symbols()
        end = datetime.utcnow() - timedelta(minutes=15)
        start = end - timedelta(days=365)

        payload = StockBarsRequest(
            symbol_or_symbols=symbols,
            start=start,
            end=end,
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            limit=None,
            feed=DataFeed.SIP,
            adjustment=Adjustment.RAW,
        )

        data = self.data_client.get_stock_bars(payload)
        data = self.clean_data(data)
        return data

    def get_two_days_data(self) -> pd.DataFrame:
        """ Retrieve the data for all stocks for the past 2 days """

        symbols = self.get_all_symbols()
        request = GetCalendarRequest(end=datetime.utcnow().date())
        calendar = self.trading_client.get_calendar(request)[-4:-1]

        start = calendar[0].open
        end = calendar[-1].close

        payload = StockBarsRequest(
            symbol_or_symbols=symbols,
            start=start,
            end=end,
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            limit=None,
            feed=DataFeed.SIP,
            adjustment=Adjustment.RAW,
        )

        data = self.data_client.get_stock_bars(payload)
        data = self.clean_data(data)
        return data