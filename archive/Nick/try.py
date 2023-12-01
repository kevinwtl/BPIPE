
from __future__ import print_function
from __future__ import absolute_import

from optparse import OptionParser
import blpapi
from datetime import datetime
import pandas as pd
import pyperclip as py
fname = r'/data/'
text = ""
data = pd.DataFrame(columns=['text'])



def parseCmdLine():
    parser = OptionParser(description="Retrieve realtime data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    parser.add_option("--me",
                      dest="maxEvents",
                      type="int",
                      help="stop after this many events (default: %default)",
                      metavar="maxEvents",
                      default=1000000)

    options,_ = parser.parse_args()

    return options


def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print("Connecting to %s:%d" % (options.host, options.port))

    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/mktdata"):
        print("Failed to open //blp/mktdata")
        return

    #security1 = '//blp/mktref/srcref/eid/<eid>'
    #security1 = '//blp/mktdata/US/IBM'
    # security1 = "//blp/mktdepthdata/bsym/HK/767?type=TOP"
    #security2 = "/cusip/912828GM6@BGN"
    security1 = "767 HK Equity"

    # security2 = "1810 HK Equity"
    fields = "LAST_PRICE"
    subscriptions = blpapi.SubscriptionList()

    subscriptions.add(security1,fields,
                      "",
                      correlationId=blpapi.CorrelationId(security1))
    # subscriptions.add(security1,
    #                   correlationId=blpapi.CorrelationId(security1))
    # #"LAST_PRICE,BID,ASK,BID2,ASK2,BID3,ASK3,BID4,ASK4,BID5,ASK5",
    security2 = '2660 HK Equity'
    subscriptions.add(security2,
                      fields,"",
                      blpapi.CorrelationId(security2))
    # # (subscriptions.destroy())
    session.subscribe(subscriptions)
    try:
        # Process received events
        eventCount = 0
        while(True):
            now = datetime.now()
            tdy = now.strftime("%m-%d")
            # We provide timeout to give the chance to Ctrl+C handling:
            event = session.nextEvent(500)
            for msg in event:
                print(f'##################\nEvent type: {event.eventType()}\n################')
                print(f'This message type is: {msg.messageType()}')
                if event.eventType() == 8:
                    outF = open(f'{msg.correlationIds()[0].value().replace(" HK Equity","HK")}_subtxt_{tdy}.txt','a')
                    now = datetime.now()
                    outF.write(f'Time now is : {now.strftime("%m/%d/%Y %I:%M:%S")}\n')
                    outF.write(f'Message type: {msg.messageType()}\n')
                    outF.write(f'Message is :\n#######################################\n{msg}\n#######################################\n')
                    outF.close()
                    if msg.getElementAsString('MKTDATA_EVENT_TYPE') == 'TRADE':
                        try:
                            # last_size = msg.getElementAsInteger('SIZE_LAST_TRADE')
                            # last_price = msg.getElementAsFloat('LAST_TRADE')
                            last_size_tdy = msg.getElementAsFloat('SIZE_LAST_TRADE_TDY')
                            last_price_tdy = msg.getElementAsFloat('LAST_TRADE_PRICE_TODAY_RT')
                            last_time = msg.getElementAsString('LAST_TRADE_PRICE_TIME_TODAY_RT')
                            time_stamp = msg.getElementAsString('TRADE_UPDATE_STAMP_RT')
                            time_stamp = f'"{time_stamp}"'
                            num_of_trades = msg.getElementAsInteger('NUM_TRADES_RT')
                        except blpapi.exception.NotFoundException:
                            print('Not ok')
                            pass
                        else:
                            # data = pd.read_csv('data.csv',index_col=0)
                            try:
                                data = pd.read_csv(f'{msg.correlationIds()[0].value().replace(" HK Equity","HK")}_data_{tdy}.csv',index_col=0)
                            except FileNotFoundError:
                                data = pd.DataFrame(columns=['NUM_TRADES_RT','TRADE_UPDATE_TIME_STAMP','LAST_TRADE_PRICE_TIME_TODAY_RT',
                                                             'LAST_TRADE_PRICE_TODAY_RT','SIZE_LAST_TRADE_TDY'])
                                data.to_csv(f'{msg.correlationIds()[0].value().replace(" HK Equity","HK")}_data_{tdy}.csv')
                                # data=pd.read_csv(f'{msg.correlationIds()[0].value().replace(" HK Equity","HK")}_data_11-8.csv')
                            else:
                                print("Message:")
                                meg = msg.getElementAsString('MKTDATA_EVENT_TYPE')
                                # current_time = now.strftime("%m/%d/%Y %I:%M:%S")
                                data.loc[num_of_trades] = [num_of_trades,time_stamp,last_time,last_price_tdy,last_size_tdy]
                                print(f'****************\nData is : {data}\n****************')
                                data.to_csv(f'{msg.correlationIds()[0].value().replace(" HK Equity","HK")}_data_{tdy}.csv')

                #print(msg)

                #print(f'Message:\n{msg}\n#########')
                # # if event.eventType() == blpapi.Event.SUBSCRIPTION_STATUS or \
                # #        event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                # # #if event.eventType() == blpapi.Event.RESPONSE:
                # #     print(f'Event count = {eventCount}')
                # #     print(f'Correlation ID = {blpapi.CorrelationId(security1)}')
                # #     print("%s - %s" % (msg.correlationIds()[0].value(), msg))
                # #     #print(f'**{(msg.messageType())}\n**')
                #     #print(msg)
                #     #print(f'\n####\n{msg}')
                #     #df = str(msg)
                #     #py.copy(df)
                #     #print(f'The type of df: {type(df)}')
                #     #print(f"\n\n\n\n\n\n##############\n{df}\n\n\n\n\n\n\n\n########")
                #
                # else:
                #     pass
            if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                eventCount += 1
                if eventCount >= options.maxEvents:
                    break
    finally:
        # Stop the session
        session.stop()

if __name__ == "__main__":
    print("SimpleSubscriptionExample")
    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")





