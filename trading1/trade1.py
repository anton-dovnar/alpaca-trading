import asyncio
import logging
from datetime import date, datetime, time, timedelta
from os import environ as env

import arrow
import pandas as pd
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.live.stock import StockDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    GetCalendarRequest,
)
from alpaca.trading.stream import TradingStream

from .scanner import Scanner

logging.basicConfig(
   level=logging.INFO,
   filename="premarket-perk.txt",
   filemode="w",
   format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


API_KEY = env["API_KEY"]
SECRET_KEY = env["SECRET_KEY"]
TZ = "US/Eastern"


class TradingBot:
    _instance = None
    _is_initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self._is_initialized:
            self._is_initialized = True
            self.should_exit = False
            self.minimum_premarket_percent_change = 30
            self.consolidations = {}

            # Clients
            self.trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
            self.data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
            self.wss_client = StockDataStream(API_KEY, SECRET_KEY)
            self.scanner = Scanner(self.trading_client, self.data_client)
            self.trading_stream = TradingStream(API_KEY, SECRET_KEY, paper=True)

            # Market state
            self.market_is_open = False
            self.open_time = four_am + timedelta(days=1) if arrow.utcnow().to(TZ) > (four_am := arrow.get(datetime.combine(date.today(), time(hour=4, minute=00)), tzinfo=TZ)) else four_am
            self.close_time = arrow.get(
                datetime.combine(date.today(), time(hour=16, minute=00)), tzinfo=TZ
            )
            self.time_to_open = 0

            self.trading_stream.subscribe_trade_updates(self.trade_update_handler)
            logger.info("Subscribed for trading updates.")
            logger.info("Trading Client is initialized.")

    def subscribe_to_trades(self, symbols: list):
        self.wss_client.subscribe_trades(self.socket_data_handler, *symbols)

    def unsubscribe_from_trades(self, symbols: list):
        self.wss_client.unsubscribe_trades(*symbols)

    def reset_market_state(self):
        self.market_is_open = False
        self.open_time = four_am + timedelta(days=1) if arrow.utcnow().to(TZ) > (four_am := arrow.get(datetime.combine(date.today(), time(hour=4, minute=00)), tzinfo=TZ)) else four_am
        self.close_time = 0
        self.time_to_open = 0

    def get_market_state(self):
        payload = GetCalendarRequest(
            start=datetime.utcnow().date(), end=datetime.utcnow().date()
        )
        calendar_list: list = self.trading_client.get_calendar(payload)

        if calendar_list:
            calendar = calendar_list[0]
            self.open_time = arrow.get(calendar.open, tzinfo=TZ)
            self.close_time = arrow.get(calendar.close, tzinfo=TZ)
            current_time = arrow.utcnow().to(TZ)

            if current_time > self.open_time and current_time < self.close_time:
                self.market_is_open = True
            elif current_time < self.open_time:
                self.time_to_open = (self.open_time - current_time).seconds
            else:
                self.reset_market_state()
        else:
            self.reset_market_state()

    async def trade_update_handler(self, data):
        order = data.order
        logger.info(
            "Trade update handler called: %s %s %s %s %s",
            data.event,
            order.symbol,
            order.side,
            order.qty,
            order.filled_qty,
        )

    async def socket_data_handler(self, data):
        logger.info("Data: %s", str(data))

    async def main(self):
        while not self.should_exit:
            # if arrow.utcnow().to(TZ) > self.open_time:
            #     self.open_time = self.open_time + timedelta(days=1)

            # time_to_open = self.open_time - arrow.utcnow().to(TZ)
            # logger.info("Waiting Time to Open %s", time_to_open)
            # await asyncio.sleep(time_to_open.seconds)
            # logger.info("Premarket is Open")

            # logger.info("Main loop called.")
            # logger.info("Getting year data")
            # await asyncio.to_thread(self.get_market_state)

            self.year_data = await asyncio.to_thread(self.scanner.get_year_data)
            logger.info("Getting last two days")
            data = await asyncio.to_thread(self.scanner.get_two_days_data)
            symbols = data.columns.get_level_values(1).unique().to_list()

            # Set initial prices
            logger.info("Setting initial prices")
            for symbol in symbols:
                data["Initial Price", symbol] = data["Close", symbol][-1]
                data["Threshold", symbol] = data["Close", symbol][-1] * (1 + self.minimum_premarket_percent_change/100)
                self.consolidations[symbol] = pd.DataFrame(columns=["Open", "High", "Low", "Close"])

            self.data = data
            logger.info("Subscribing to trades")
            logger.info("Number of symbols %s", str(len(symbols)))
            await asyncio.to_thread(self.subscribe_to_trades, symbols)
            logger.info("Subscribed to trades.")

            if self.close_time:
                current_time = arrow.utcnow().to(TZ)
                thirty_mins_to_close = (
                    self.close_time - current_time
                ).seconds - 1800

                logger.info("Waiting Time to Close %s", thirty_mins_to_close)
                await asyncio.sleep(thirty_mins_to_close)

                logger.info("Close All Positions")
                await asyncio.to_thread(
                    self.trading_client.close_all_positions, True
                )

                logger.info("Unsubscribe all stocks data stream")
                await asyncio.to_thread(
                    self.unsubscribe_from_trades, symbols
                )