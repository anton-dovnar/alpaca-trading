import asyncio
import warnings

from trading1.trade1 import TradingBot

warnings.filterwarnings("ignore")


def main():
    trade = TradingBot()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    socket_task = loop.create_task(trade.wss_client._run_forever())
    main_task = loop.create_task(trade.main())
    loop.run_until_complete(asyncio.gather(socket_task, main_task))
    loop.close()


if __name__ == "__main__":
    main()
