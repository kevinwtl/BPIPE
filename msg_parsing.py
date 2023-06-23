# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from functools import cached_property
from blp import blp
import pandas as pd
import numpy as np
from pymongo import MongoClient
from tqdm import tqdm
import requests


class OrderBook:
    """
    This class contains functions to update the orderbook
    """

    def __init__(self, ticker: str, date: str = datetime.now().strftime("%Y%m%d")):
        # Create empty frames
        self.df_bid = pd.DataFrame(columns=["MBO_BID_POSITION_RT", "MBO_BID_RT", "MBO_BID_COND_CODE_RT", "MBO_ORDER_ID_RT", "MBO_BID_SIZE_RT", "MBO_TIME_RT", "MD_TABLE_CMD_RT", "MBO_BID_BROKER_RT"])
        self.df_ask = pd.DataFrame(columns=["MBO_ASK_POSITION_RT", "MBO_ASK_RT", "MBO_ASK_COND_CODE_RT", "MBO_ORDER_ID_RT", "MBO_ASK_SIZE_RT", "MBO_TIME_RT", "MD_TABLE_CMD_RT", "MBO_ASK_BROKER_RT"])
        self.mbo_update = pd.DataFrame()
        self.top_update = pd.DataFrame()
        self.journal: list[dict] = []
        self.journal_df = pd.DataFrame()

        # Bloomberg Connection
        self.bquery = blp.BlpQuery().start()

        self.ticker = ticker
        self.date = date

        # MongoDB connection
        CONNECTION_STRING = "mongodb://localhost:27017/"
        client = MongoClient(CONNECTION_STRING)
        self.db = client["BPIPE"][date]

        # Retrieve messages
        cursor = self.db.find({"cid": {"$regex": self.ticker}})
        self.all_msg_df = pd.DataFrame(list(cursor)).drop(["_id"], axis=1).dropna(subset=["MKTDEPTH_EVENT_TYPE"])
        # self.all_msg_df = pd.DataFrame(msgList).dropna(subset=["MKTDEPTH_EVENT_TYPE"], inplace=True)

        # Debugging
        self.bid_order_books = []

    @staticmethod
    def get_time_to_second(dt) -> datetime:
        """
        This function aligns timestamp format from miliseconds to seconds.
        ------
        dt: timestamp in milisecond
        """
        if dt.microsecond > 990000:
            dt_new = dt.round(freq="S")
        else:
            dt_new = dt.floor(freq="S")

        return dt_new

    @cached_property
    def trade_prints(self) -> pd.DataFrame:
        """Download all trades taken place on the day. Same as QR <GO>."""
        df = self.bquery.bdit(
            security=self.ticker,
            event_types=["TRADE"],
            start_datetime=datetime.strptime(self.date, "%Y%m%d").strftime("%Y-%m-%dT01:00:00"),
            end_datetime=datetime.strptime(self.date, "%Y%m%d").strftime("%Y-%m-%dT09:00:00"),
            options=[("includeConditionCodes", True), ("includeNonPlottableEvents", True)],
        )
        df["time"] += timedelta(hours=8)
        if "conditionCodes" in df:
            df[(df["conditionCodes"] != "IE") & (df["conditionCodes"] != "OC")].reset_index(drop=True)
        elif "conditionCodes" not in df:
            df["conditionCodes"] = np.nan
        return df

    @property
    def broker_ID_dict(self) -> dict:
        # requests to download .csv from HKEX
        csv_url = "https://www.hkex.com.hk/ENG/PLW/csv/List_of_Current_SEHK_EP.CSV"
        with requests.Session() as s:
            download = s.get(csv_url)
        decoded_content = download.content.decode("utf-16").splitlines()
        decoded_content = decoded_content[: len(decoded_content) - 17]
        broker_ID_dict = {}
        for content in decoded_content:
            broker_num = content.split("\t")[1].replace('"', "").split(", ")
            for num in broker_num:
                if len(num) == 2:
                    num = "  " + num
                if len(num) == 3:
                    num = " " + num
                broker_ID_dict[num] = content.split("\t")[2].replace('"', "")
        return broker_ID_dict

    def get_initpaint(self, side: str):
        """
        This function returns initial paints for bid side and ask side.
        If no initial paint is provided in the MBO or TOP, empty DataFrame is returned with columns names.
        ------
        side: "BID" or "ASK"
        """
        initpaint_mbo = self.all_msg_df[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE == "TABLE_INITPAINT") & (self.all_msg_df.EID == 53714)]
        initpaint_top = self.all_msg_df[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE == "TABLE_INITPAINT") & (self.all_msg_df.EID == 14112)]

        if side == "BID":
            if "MBO_TABLE_BID" in initpaint_mbo.columns and "MBO_TABLE_BID" in initpaint_top.columns and pd.notna(initpaint_mbo["MBO_TABLE_BID"].values[0]) and pd.notna(initpaint_top["MBO_TABLE_BID"].values[0]):
                mbo_bid = pd.DataFrame(initpaint_mbo["MBO_TABLE_BID"].to_list()[0])
                top_bid = pd.DataFrame(initpaint_top["MBO_TABLE_BID"].to_list()[0])
                self.df_bid = mbo_bid.join(top_bid[["MBO_BID_BROKER_RT"]], how="outer")

        if side == "ASK":
            if "MBO_TABLE_ASK" in initpaint_mbo.columns and "MBO_TABLE_ASK" in initpaint_top.columns and pd.notna(initpaint_mbo["MBO_TABLE_ASK"].values[0]) and pd.notna(initpaint_top["MBO_TABLE_ASK"].values[0]):
                mbo_ask = pd.DataFrame(initpaint_mbo["MBO_TABLE_ASK"].to_list()[0])
                top_ask = pd.DataFrame(initpaint_top["MBO_TABLE_ASK"].to_list()[0])
                self.df_ask = mbo_ask.join(top_ask[["MBO_ASK_BROKER_RT"]], how="outer")

    def add_del_mod(self, mbo, cmd, side):
        """
        This function perform the command of ADD, DEL, or MOD to the current order book.
        ------
        mbo: Series, the change of orderbook
        cmd: str, one of "ADD", "DEL" and "MOD"
        side: str, one of "BID", "ASK"
        """

        if side == "BID":
            df = self.df_bid.copy()
            if cmd == "DEL":
                df = df.drop(index=df[df.MBO_BID_POSITION_RT == mbo.MBO_BID_POSITION_RT].index).reset_index(drop=True)
            elif cmd == "ADD":
                if mbo.MBO_BID_POSITION_RT not in df["MBO_BID_POSITION_RT"].to_list():
                    df.loc[df.shape[0]] = [mbo.MBO_BID_POSITION_RT, mbo.MBO_BID_RT, "", "", mbo.MBO_BID_SIZE_RT, mbo.MBO_TIME_RT, mbo.MD_TABLE_CMD_RT, "TODO"]  # type: ignore
                else:
                    df["MBO_BID_POSITION_RT"] = df["MBO_BID_POSITION_RT"].apply(lambda x: x + 1 if x >= mbo.MBO_BID_POSITION_RT else x)
                    df.loc[df.shape[0]] = [mbo.MBO_BID_POSITION_RT, mbo.MBO_BID_RT, "", "", mbo.MBO_BID_SIZE_RT, mbo.MBO_TIME_RT, mbo.MD_TABLE_CMD_RT, "TODO"]  # type: ignore
                    df = df.sort_values(by=["MBO_BID_POSITION_RT"])
            else:
                df.loc[df[df.MBO_BID_POSITION_RT == mbo.MBO_BID_POSITION_RT].index, "MBO_BID_RT"] = mbo.MBO_BID_RT
                df.loc[df[df.MBO_BID_POSITION_RT == mbo.MBO_BID_POSITION_RT].index, "MBO_BID_SIZE_RT"] = mbo.MBO_BID_SIZE_RT
            self.df_bid = df.reset_index(drop=True)
            self.bid_order_books.append(self.df_bid)  # FIXME: debug

        if side == "ASK":
            df = self.df_ask.copy()
            if cmd == "DEL":
                df = df.drop(index=df[df.MBO_ASK_POSITION_RT == mbo.MBO_ASK_POSITION_RT].index).reset_index(drop=True)
            elif cmd == "ADD":
                if mbo.MBO_ASK_POSITION_RT not in df["MBO_ASK_POSITION_RT"].to_list():
                    df.loc[df.shape[0]] = [mbo.MBO_ASK_POSITION_RT, mbo.MBO_ASK_RT, "", "", mbo.MBO_ASK_SIZE_RT, mbo.MBO_TIME_RT, mbo.MD_TABLE_CMD_RT, "TODO"]  # type: ignore
                else:
                    df["MBO_ASK_POSITION_RT"] = df["MBO_ASK_POSITION_RT"].apply(lambda x: x + 1 if x >= mbo.MBO_ASK_POSITION_RT else x)
                    df.loc[df.shape[0]] = [mbo.MBO_ASK_POSITION_RT, mbo.MBO_ASK_RT, "", "", mbo.MBO_ASK_SIZE_RT, mbo.MBO_TIME_RT, mbo.MD_TABLE_CMD_RT, "TODO"]
                df = df.sort_values(by=["MBO_ASK_POSITION_RT"])
            else:
                df.loc[df[df.MBO_ASK_POSITION_RT == mbo.MBO_ASK_POSITION_RT].index, "MBO_ASK_RT"] = mbo.MBO_ASK_RT
                df.loc[df[df.MBO_ASK_POSITION_RT == mbo.MBO_ASK_POSITION_RT].index, "MBO_ASK_SIZE_RT"] = mbo.MBO_ASK_SIZE_RT
            self.df_ask = df.reset_index(drop=True)

    def update_orderbook(self, side, cmd, mbo, top):
        """
        This function is to update the orderbook
        ------
        side: str, one of "BID", "ASK"
        cmd: str, one of "ADD", "DEL" and "MOD"
        mbo: Series, the change of orderbook
        top: DataFrame, the source of broker ID
        msg: list, stack all the results
        """

        if side == "BID":
            df = self.df_bid.copy()
            if cmd == "DEL":
                self.journal.append([mbo.MBO_TIME_RT, df[df.MBO_BID_POSITION_RT == mbo.MBO_BID_POSITION_RT]["MBO_BID_BROKER_RT"].values[0], mbo.MBO_BID_RT, mbo.MKTDEPTH_EVENT_SUBTYPE, mbo.MBO_BID_SIZE_RT, mbo.MD_TABLE_CMD_RT, mbo.MBO_BID_POSITION_RT])
                self.add_del_mod(mbo, cmd, side)
            elif cmd == "MOD":
                self.journal.append(
                    [
                        mbo.MBO_TIME_RT,
                        df[df.MBO_BID_POSITION_RT == mbo.MBO_BID_POSITION_RT]["MBO_BID_BROKER_RT"].values[0],
                        mbo.MBO_BID_RT,
                        mbo.MKTDEPTH_EVENT_SUBTYPE,
                        df[df.MBO_BID_POSITION_RT == mbo.MBO_BID_POSITION_RT]["MBO_BID_SIZE_RT"].values[0] - mbo.MBO_BID_SIZE_RT,
                        mbo.MD_TABLE_CMD_RT,
                        mbo.MBO_BID_POSITION_RT,
                    ]
                )
                self.add_del_mod(mbo, cmd, side)
            else:
                self.add_del_mod(mbo, cmd, side)
                broker_posit = self.df_bid[self.df_bid.MBO_BID_BROKER_RT == "TODO"]["MBO_BID_POSITION_RT"].to_list()
                for k, posit in enumerate(broker_posit):
                    try:
                        self.df_bid.loc[self.df_bid[self.df_bid.MBO_BID_BROKER_RT == "TODO"].index[k], "MBO_BID_BROKER_RT"] = top[top.MBO_BID_POSITION_RT == posit]["MBO_BID_BROKER_RT"].to_list()[-1]
                    except IndexError:
                        continue
                self.journal.append([mbo.MBO_TIME_RT, self.df_bid[self.df_bid.MBO_BID_POSITION_RT == mbo.MBO_BID_POSITION_RT]["MBO_BID_BROKER_RT"].values[0], mbo.MBO_BID_RT, mbo.MKTDEPTH_EVENT_SUBTYPE, mbo.MBO_BID_SIZE_RT, mbo.MD_TABLE_CMD_RT, mbo.MBO_BID_POSITION_RT])

        if side == "ASK":
            df = self.df_ask.copy()
            if cmd == "DEL":
                self.journal.append([mbo.MBO_TIME_RT, df[df.MBO_ASK_POSITION_RT == mbo.MBO_ASK_POSITION_RT]["MBO_ASK_BROKER_RT"].values[0], mbo.MBO_ASK_RT, mbo.MKTDEPTH_EVENT_SUBTYPE, mbo.MBO_ASK_SIZE_RT, mbo.MD_TABLE_CMD_RT, mbo.MBO_ASK_POSITION_RT])
                self.add_del_mod(mbo, cmd, side)
            elif cmd == "MOD":
                self.journal.append(
                    [
                        mbo.MBO_TIME_RT,
                        df[df.MBO_ASK_POSITION_RT == mbo.MBO_ASK_POSITION_RT]["MBO_ASK_BROKER_RT"].values[0],
                        mbo.MBO_ASK_RT,
                        mbo.MKTDEPTH_EVENT_SUBTYPE,
                        df[df.MBO_ASK_POSITION_RT == mbo.MBO_ASK_POSITION_RT]["MBO_ASK_SIZE_RT"].values[0] - mbo.MBO_ASK_SIZE_RT,
                        mbo.MD_TABLE_CMD_RT,
                        mbo.MBO_ASK_POSITION_RT,
                    ]
                )
                self.add_del_mod(mbo, cmd, side)
            else:
                self.add_del_mod(mbo, cmd, side)
                broker_posit = self.df_ask[self.df_ask.MBO_ASK_BROKER_RT == "TODO"]["MBO_ASK_POSITION_RT"].to_list()
                for k, posit in enumerate(broker_posit):
                    try:
                        self.df_ask.loc[self.df_ask[self.df_ask.MBO_ASK_BROKER_RT == "TODO"].index[k], "MBO_ASK_BROKER_RT"] = top[top.MBO_ASK_POSITION_RT == posit]["MBO_ASK_BROKER_RT"].to_list()[-1]
                    except IndexError:
                        continue
                self.journal.append([mbo.MBO_TIME_RT, self.df_ask[self.df_ask.MBO_ASK_POSITION_RT == mbo.MBO_ASK_POSITION_RT]["MBO_ASK_BROKER_RT"].values[0], mbo.MBO_ASK_RT, mbo.MKTDEPTH_EVENT_SUBTYPE, mbo.MBO_ASK_SIZE_RT, mbo.MD_TABLE_CMD_RT, mbo.MBO_ASK_POSITION_RT])

    def match_trades(self):
        """ """
        for time in self.journal_df["Timestamp"].unique():
            if time not in self.trade_prints["time"].unique():
                pass
            else:
                journal_unique = self.journal_df[self.journal_df["Timestamp"] == time]
                trades_unique = self.trade_prints[self.trade_prints.time == time]
                for j, row in journal_unique.iterrows():
                    if row.Action in ["DEL", "MOD"]:
                        for k, entry in trades_unique.iterrows():
                            if row.Price == entry.value and row.Size == entry["size"] and row.Position == 1:
                                self.journal_df.loc[j, "Action"] = "Executed"
                                self.journal_df.loc[j, "Condition Codes"] = entry["conditionCodes"]
                                # trades_unique = trades_unique.drop(index=k)
                                # break

    def record_unmatched_trades(self):
        """Add a new row to self.journal_df if the trade was not located there. (Debug purpose)"""
        for j, entry in self.trade_prints.iterrows():
            if entry['time'] not in self.journal_df['Timestamp'].unique():
                na_row = pd.DataFrame(
                    {
                        "Timestamp": entry["time"],
                        "BrokerID": "(Debug) Not Recognized",
                        "Price": entry["value"],
                        "Side": "(Debug) Not Recognized",
                        "Size": entry["size"],
                        "Action": "Trade",
                        "Position": np.nan,
                        "Condition Codes": entry["conditionCodes"],
                        "Broker Name": "(Debug) Not Recognized",
                    },
                    index=[0],
                )

                self.journal_df = pd.concat([self.journal_df, na_row], ignore_index=True).sort_values('Timestamp')  # insert row

    def run(self):
        """
        This function is the main program to update the orderbook and generate output
        """

        self.mbo_update = self.all_msg_df.loc[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE != "TABLE_INITPAINT") & (self.all_msg_df.EID == 53714)].dropna(how="all", axis=1)
        self.top_update = self.all_msg_df.loc[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE != "TABLE_INITPAINT") & (self.all_msg_df.EID == 14112)].dropna(how="all", axis=1)
        self.get_initpaint("BID")
        self.get_initpaint("ASK")

        for time in tqdm(self.mbo_update["MBO_TIME_RT"].unique()):
            mbo = self.mbo_update[self.mbo_update.MBO_TIME_RT == time].reset_index(drop=True)
            top = self.top_update[self.top_update.MBO_TIME_RT == self.get_time_to_second(time)].reset_index(drop=True)
            cmd_list = mbo["MD_TABLE_CMD_RT"].to_list()
            type_list = mbo["MKTDEPTH_EVENT_SUBTYPE"].to_list()
            # reorder_sign = Technique().reorder_indicator(cmd_list, type_list)

            for j, row in mbo.iterrows():
                if type_list[j] == "BID":
                    if row.MBO_BID_POSITION_RT not in self.df_bid.MBO_BID_POSITION_RT or not pd.isna(self.df_bid.loc[self.df_bid.MBO_BID_POSITION_RT == row.MBO_BID_POSITION_RT, "MBO_BID_BROKER_RT"].values[0]):  # type: ignore
                        self.update_orderbook("BID", cmd_list[j], row, top)

                if type_list[j] == "ASK":
                    if row.MBO_ASK_POSITION_RT not in self.df_ask.MBO_ASK_POSITION_RT or not pd.isna(self.df_ask.loc[self.df_ask.MBO_ASK_POSITION_RT == row.MBO_ASK_POSITION_RT, "MBO_ASK_BROKER_RT"].values[0]):  # type: ignore
                        self.update_orderbook("ASK", cmd_list[j], row, top)

                # if reorder_sign[j] is True:
                self.df_bid = self.df_bid.sort_values(by=["MBO_BID_POSITION_RT"]).reset_index(drop=True)
                self.df_bid.MBO_BID_POSITION_RT = range(1, len(self.df_bid) + 1)
                self.df_ask = self.df_ask.sort_values(by=["MBO_ASK_POSITION_RT"]).reset_index(drop=True)
                self.df_ask.MBO_ASK_POSITION_RT = range(1, len(self.df_ask) + 1)

        self.journal_df = pd.DataFrame(self.journal, columns=["Timestamp", "BrokerID", "Price", "Side", "Size", "Action", "Position"])
        self.journal_df = self.journal_df[(self.journal_df.BrokerID != "") & (self.journal_df.BrokerID != "TODO")].reset_index(drop=True)
        self.journal_df["Timestamp"] = self.journal_df["Timestamp"] + timedelta(hours=8)  # type: ignore
        self.journal_df["Timestamp"] = self.journal_df["Timestamp"].apply(lambda x: x.replace(tzinfo=None))
        self.journal_df["Price"] = self.journal_df["Price"].apply(lambda x: round(x, 2))

        self.match_trades()
        self.journal_df["Broker Name"] = self.journal_df["BrokerID"].map(self.broker_ID_dict)
        #self.record_unmatched_trades()

        # Formatting
        d = {"ADD": "New Order", "DEL": "Removed Order", "MOD": "Order Modified", "Executed": "Trade", "Trade":"Trade"}
        self.journal_df["Action"] = self.journal_df["Action"].map(d)
        self.journal_df["Timestamp"] = pd.to_datetime(self.journal_df["Timestamp"])
        self.journal_df["Timestamp"] = self.journal_df["Timestamp"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f"))


if __name__ == "__main__":

    x = OrderBook("3738 HK Equity")
    print(x.all_msg_df)
    x.run()
    print(x.journal_df)
    # x.run()
    # x.journal_df
