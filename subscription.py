import blpapi
from blpapi import AuthOptions, TlsOptions
from pymongo import MongoClient
from datetime import datetime

class BPIPE:
    def __init__(self, tickers: list[str], mongo_connection_string: str = "mongodb://localhost:27017/"):
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
        self.options.setTlsOptions(TlsOptions.createFromFiles(r"T:\Intern Folder\External Research\2023 Interns\BPIPE\certificates\65C7B614CC7BF90D81CDE1363A617D85.pk12", "123456", r"T:\Intern Folder\External Research\2023 Interns\BPIPE\certificates\rootCertificate.pk7"))

        self.options.setAutoRestartOnDisconnection(True)
        print(self.options)

        #self.msgList = []
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
            # subList.add(f'/ticker/{ticker}', fields=['MKTDATA_EVENT_SUBTYPE,EVT_TRADE_TIME_RT,EVT_TRADE_CONDITION_CODE_RT,EVT_TRADE_PRICE_RT,EVT_TRADE_SIZE_RT'], correlationId=blpapi.CorrelationId(ticker+'.type=QR'))
        self.session.subscribe(subList)


    def onEvent(self, event, session):
        for msg in event:
            if msg.messageType() in ["MarketDepthUpdates", "MarketDataEvents"]:

                cid = msg.correlationId().value()
                msg = msg.toPy()  # dictionary
                # if msg["MKTDEPTH_EVENT_TYPE"] == "MARKET_BY_ORDER" and msg["EID"] == 14112:
                #     msg["SUBSCRIPTION_TYPE"] = "TOP"
                # elif msg["MKTDEPTH_EVENT_TYPE"] == "MARKET_BY_ORDER" and msg["EID"] == 53714:
                #     msg["SUBSCRIPTION_TYPE"] = "MBO"
                # elif msg["MKTDEPTH_EVENT_TYPE"] == "MARKET_BY_LEVEL" and msg["EID"] == 14112:
                #     msg["SUBSCRIPTION_TYPE"] = "MBL"

                msg["cid"] =  cid # identifier of the subscription
                #self.msgList.append(msg)
                self.db.insert_one(msg) # insert to database

            else:
                print(msg)


if __name__ == "__main__":
    BPIPE(['2060 HK Equity','3633 HK Equity','3683 HK Equity','3738 HK Equity', '959 HK Equity', '970 HK Equity']).subscribe()
    input() # to suspend the program
