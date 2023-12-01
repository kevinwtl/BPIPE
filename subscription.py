import blpapi
from blpapi import AuthOptions, TlsOptions
from pymongo import MongoClient
from datetime import datetime, timedelta
import traceback


class BPIPE:
    def __init__(self, tickers: list[str], mongo_connection_string: str = "mongodb://192.168.10.153:27017/"):
        # Setup MongoDB connection
        client = MongoClient(mongo_connection_string)
        self.db = client["BPIPE"][datetime.now().strftime("%Y%m%d")]

        # Define tickers
        self.tickers = tickers

        # Initialize session options
        self.options = blpapi.SessionOptions()
        self.options.setServerAddress("cloudpoint1.bloomberg.com", 8194, 0)
        self.options.setServerAddress("cloudpoint2.bloomberg.com", 8194, 1)
        self.options.setSessionIdentityOptions(AuthOptions.createWithApp("CHINASILVER:Trading"))

        # TLS key files for connecting to ZFP endpoint
        self.options.setTlsOptions(TlsOptions.createFromFiles(r"H:\GitHub\BPIPE\certificates\65C7B614CC7BF90D81CDE1363A617D85.pk12", "123456", r"H:\GitHub\BPIPE\certificates\rootCertificate.pk7"))

        self.options.setAutoRestartOnDisconnection(True)
        print(self.options)

        self.session = blpapi.Session(self.options, self.onEvent)
        self.session.start()
        self.session.openService("//blp/mktdepthdata")

    def subscribe(self):
        subList = blpapi.SubscriptionList()
        for ticker in self.tickers:
            print(f"Subscribing to {ticker}...")
            subList.add("//blp/mktdepthdata/ticker/" + str(ticker), options="type=TOP", correlationId=blpapi.CorrelationId(ticker + ".type=TOP"))
            subList.add("//blp/mktdepthdata/ticker/" + str(ticker), options="type=MBO", correlationId=blpapi.CorrelationId(ticker + ".type=MBO"))  # Need full book
            subList.add("//blp/mktdepthdata/ticker/" + str(ticker), options="type=MBL", correlationId=blpapi.CorrelationId(ticker + ".type=MBL"))
            subList.add(ticker, fields=["EVT_TRADE_TIME_RT", "MKTDATA_EVENT_TYPE", "EVT_TRADE_PRICE_RT", "EVT_TRADE_SIZE_RT", "EVT_TRADE_CONDITION_CODE_RT"], correlationId=blpapi.CorrelationId(ticker + ".type=QR"))
        self.session.subscribe(subList)

    def onEvent(self, event, session):
        for msg in event:
            if msg.messageType() == "MarketDepthUpdates":
                cid = msg.correlationId().value()
                msg = msg.toPy()
                msg["cid"] = cid
                self.db.insert_one(msg)

            elif msg.messageType() == "MarketDataEvents":
                try:
                    cid = msg.correlationId().value()
                    msg = msg.toPy()
                    if "EVT_TRADE_TIME_RT" in msg and msg["MKTDATA_EVENT_TYPE"] == "TRADE":
                        msg["cid"] = cid
                        msg["EVT_TRADE_TIME_RT"] = datetime.combine(datetime.today(), msg["EVT_TRADE_TIME_RT"]) + timedelta(hours=8)  # Convert datetime.time to datetime.datetime
                        self.db.insert_one(msg)
                except Exception as e:
                    print(e)
                    print(msg)

            else:
                print(msg)


if __name__ == "__main__":
    try:
        BPIPE(["2060 HK Equity", "3633 HK Equity", "3683 HK Equity", "3738 HK Equity", "959 HK Equity", "970 HK Equity", "6698 HK Equity", "2415 HK Equity"]).subscribe()
    except:
        traceback.print_exc()
    input()  # to suspend the program
