# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from functools import cached_property
from typing import Literal
import pandas as pd
import numpy as np
from pymongo import MongoClient
from tqdm import tqdm
import requests


class OrderBook:
    """
    This class contains functions to update the orderbook
    """

    def __init__(self, ticker: str, date: str = datetime.now().strftime("%Y%m%d"), mongo_connection_string: str = "mongodb://localhost:27017/"):
        # Create empty frames
        self.df_bid = pd.DataFrame(columns=["MBO_BID_POSITION_RT", "MBO_BID_RT", "MBO_BID_COND_CODE_RT", "MBO_ORDER_ID_RT", "MBO_BID_SIZE_RT", "MBO_TIME_RT", "MD_TABLE_CMD_RT", "MBO_BID_BROKER_RT"])
        self.df_ask = pd.DataFrame(columns=["MBO_ASK_POSITION_RT", "MBO_ASK_RT", "MBO_ASK_COND_CODE_RT", "MBO_ORDER_ID_RT", "MBO_ASK_SIZE_RT", "MBO_TIME_RT", "MD_TABLE_CMD_RT", "MBO_ASK_BROKER_RT"])
        self.all_MBO_df = pd.DataFrame()
        self.all_TOP_df = pd.DataFrame()
        self.journal: list[dict] = []
        self.journal_df = pd.DataFrame()

        # Bloomberg Connection (not used)
        #self.bquery = blp.BlpQuery().start()

        self.ticker = ticker
        self.date = date

        # MongoDB connection
        self.client = MongoClient(mongo_connection_string)
        self.db = self.client["BPIPE"][date]

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
    def all_msg_df(self) -> pd.DataFrame:
        cursor = self.db.find({"cid": {"$regex": f"(?=.*{self.ticker})(?!.*QR)"}})  # Match "Not QR"
        df = pd.DataFrame(list(cursor))
        if len(df) == 0:
            raise IndexError("No BPIPE messages Found.")
        return df.drop(["_id"], axis=1).dropna(subset=["MKTDEPTH_EVENT_TYPE"])

    @cached_property
    def trade_prints(self) -> pd.DataFrame:
        """Download all trades taken place on the day. Same as QR <GO>."""
        # df = self.bquery.bdit(
        #     security=self.ticker,
        #     event_types=["TRADE"],
        #     start_datetime=datetime.strptime(self.date, "%Y%m%d").strftime("%Y-%m-%dT01:00:00"),
        #     end_datetime=datetime.strptime(self.date, "%Y%m%d").strftime("%Y-%m-%dT09:00:00"),
        #     options=[("includeConditionCodes", True), ("includeNonPlottableEvents", True)],
        # )
        # df["time"] += timedelta(hours=8)
        # if "conditionCodes" in df:
        #     df[(df["conditionCodes"] != "IE") & (df["conditionCodes"] != "OC")].reset_index(drop=True)
        # elif "conditionCodes" not in df:
        #     df["conditionCodes"] = np.nan
        # return df

        cursor = self.db.find({"cid": {"$regex": f"(?=.*{self.ticker})(?=.*QR)"}})  # Match QR
        df = pd.DataFrame(list(cursor))
        if len(df) == 0:
            raise IndexError("No QR Trades Found.")
        df = df.rename(columns={"MKTDATA_EVENT_TYPE": "type", "EVT_TRADE_PRICE_RT": "value", "EVT_TRADE_SIZE_RT": "size", "EVT_TRADE_CONDITION_CODE_RT": "conditionCodes", "EVT_TRADE_TIME_RT": "time"})
        return df.drop(["_id"], axis=1)

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

    def get_initpaint(self, side: Literal["BID", "ASK"]):
        """Update `self.df_bid` or `self.df_ask` with the initial paints, if any."""

        initpaint_mbo = self.all_msg_df[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE == "TABLE_INITPAINT") & (self.all_msg_df.EID == 53714)]
        initpaint_top = self.all_msg_df[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE == "TABLE_INITPAINT") & (self.all_msg_df.EID == 14112)]

        if f"MBO_TABLE_{side}" in initpaint_mbo and f"MBO_TABLE_{side}" in initpaint_top and pd.notna(initpaint_mbo[f"MBO_TABLE_{side}"].values[0]) and pd.notna(initpaint_top[f"MBO_TABLE_{side}"].values[0]):
            mbo = pd.DataFrame(initpaint_mbo[f"MBO_TABLE_{side}"].to_list()[0])
            top = pd.DataFrame(initpaint_top[f"MBO_TABLE_{side}"].to_list()[0])
            if side == "BID":
                self.df_bid = mbo.join(top[[f"MBO_{side}_BROKER_RT"]], how="outer")
            elif side == "ASK":
                self.df_ask = mbo.join(top[[f"MBO_{side}_BROKER_RT"]], how="outer")

    def add_del_mod(self, mbo: pd.Series, cmd: Literal["ADD", "DEL", "MOD"], side: Literal["BID", "ASK"]):
        """
        This function perform the command of ADD, DEL, or MOD to the current order book.
        ------
        mbo: Series, the change of orderbook
        cmd: str, one of "ADD", "DEL" and "MOD"
        side: str, one of "BID", "ASK"
        """

        df = self.df_bid.copy() if side == "BID" else self.df_ask.copy()
        if cmd == "DEL":
            df.drop(index=df[df[f"MBO_{side}_POSITION_RT"] == mbo[f"MBO_{side}_POSITION_RT"]].index, inplace=True)
        elif cmd == "ADD":
            df[f"MBO_{side}_POSITION_RT"] = df[f"MBO_{side}_POSITION_RT"].apply(lambda x: x + 1 if x >= mbo[f"MBO_{side}_POSITION_RT"] else x)  # shift all positions by 1
            df.loc[len(df)] = [mbo[f"MBO_{side}_POSITION_RT"], mbo[f"MBO_{side}_RT"], "", "", mbo[f"MBO_{side}_SIZE_RT"], mbo["MBO_TIME_RT"], mbo["MD_TABLE_CMD_RT"], "0000"]  # type: ignore
            df.sort_values(by=[f"MBO_{side}_POSITION_RT"], inplace=True)
        elif cmd == "MOD":
            df.loc[df[df[f"MBO_{side}_POSITION_RT"] == mbo[f"MBO_{side}_POSITION_RT"]].index, f"MBO_{side}_SIZE_RT"] = mbo[f"MBO_{side}_SIZE_RT"]

        if side == "BID":
            self.df_bid = df.reset_index(drop=True)
        elif side == "ASK":
            self.df_ask = df.reset_index(drop=True)

    def update_journal(self, side: Literal["BID", "ASK"], cmd: Literal["ADD", "DEL", "MOD"], mbo, top):
        """
        ...
        ------
        side: str, one of "BID", "ASK"
        cmd: str, one of "ADD", "DEL" and "MOD"
        mbo: Series, the change of orderbook
        top: DataFrame, the source of broker ID
        msg: list, stack all the results
        """

        if cmd == "DEL":
            df = self.df_bid.copy() if side == "BID" else self.df_ask.copy()
            self.journal.append(
                [
                    mbo["MBO_TIME_RT"],
                    df[df[f"MBO_{side}_POSITION_RT"] == mbo[f"MBO_{side}_POSITION_RT"]][f"MBO_{side}_BROKER_RT"].values[0],
                    mbo[f"MBO_{side}_RT"],
                    mbo["MKTDEPTH_EVENT_SUBTYPE"],
                    mbo[f"MBO_{side}_SIZE_RT"],  # qty = size of the whole order
                    mbo["MD_TABLE_CMD_RT"],
                    mbo[f"MBO_{side}_POSITION_RT"],
                ]
            )
            self.add_del_mod(mbo, cmd, side)

        elif cmd == "MOD":
            df = self.df_bid.copy() if side == "BID" else self.df_ask.copy()
            self.journal.append(
                [
                    mbo["MBO_TIME_RT"],
                    df[df[f"MBO_{side}_POSITION_RT"] == mbo[f"MBO_{side}_POSITION_RT"]][f"MBO_{side}_BROKER_RT"].values[0],
                    mbo[f"MBO_{side}_RT"],
                    mbo["MKTDEPTH_EVENT_SUBTYPE"],
                    df[df[f"MBO_{side}_POSITION_RT"] == mbo[f"MBO_{side}_POSITION_RT"]][f"MBO_{side}_SIZE_RT"].values[0] - mbo[f"MBO_{side}_SIZE_RT"],  # qty = change in size of the order
                    mbo["MD_TABLE_CMD_RT"],
                    mbo[f"MBO_{side}_POSITION_RT"],
                ]
            )
            self.add_del_mod(mbo, cmd, side)

        elif cmd == "ADD":
            self.add_del_mod(mbo, cmd, side)
            df = self.df_bid.copy() if side == "BID" else self.df_ask.copy()
            unmatched_orders = df[df[f"MBO_{side}_BROKER_RT"] == "0000"] # orders from umatched broker "0000"
            for ix, row in unmatched_orders.iterrows():
                unmatched_position = row[f"MBO_{side}_POSITION_RT"]
                unmatched_price = row[f"MBO_{side}_RT"]
                query = f'(MBO_{side}_POSITION_RT == @unmatched_position) & (MBO_{side}_RT == @unmatched_price)' # Match position number and price
                broker_list = top.query(query)[f"MBO_{side}_BROKER_RT"].to_list() # Find broker ID
                if len(broker_list) > 0: # if we can match the broker
                    df.loc[ix, f"MBO_{side}_BROKER_RT"] = broker_list[-1]
            self.journal.append(
                [
                    mbo["MBO_TIME_RT"],
                    df[df[f"MBO_{side}_POSITION_RT"] == mbo[f"MBO_{side}_POSITION_RT"]][f"MBO_{side}_BROKER_RT"].values[0],
                    mbo[f"MBO_{side}_RT"],
                    mbo["MKTDEPTH_EVENT_SUBTYPE"],
                    mbo[f"MBO_{side}_SIZE_RT"],
                    mbo["MD_TABLE_CMD_RT"],
                    mbo[f"MBO_{side}_POSITION_RT"],
                ]
            )
            if side == "BID":
                self.df_bid = df
            elif side == "ASK":
                self.df_ask = df


    def match_trades(self):
        """ """

        df = self.journal_df[self.journal_df['Timestamp'].isin(self.trade_prints['time'])] # Timestamp filter
        df = df[df['Action'].isin(['DEL', 'MOD'])] # Action filter

        for ix, row in df.iterrows():
            trade = self.trade_prints[self.trade_prints['time'] == row['Timestamp']]
            for _, entry in trade.iterrows():
                if entry['value'] == row['Price'] and entry['size'] == row['Size'] and row['Position'] == 1:
                    self.journal_df.loc[ix, 'Action'] = 'Executed'
                    self.journal_df.loc[ix, "Condition Codes"] = entry["conditionCodes"]


    def identify_active_passive_trades(self):
        """Identify whether the trade is executed passively (i.e. waited to be lifted up / hitted down) or actively (i.e. sending new order and execute trades instantly)."""
        self.journal_df["Trade Type"] = self.journal_df["Condition Codes"].apply(lambda x: "Automatch" if x == "Y" else np.nan)  # Match Automatch (Y) Trade

        for ts, group in self.journal_df.groupby("Timestamp"):
            number_of_sides_in_ts = len(group[group["Action"] == "Trade"]["Side"].unique())

            if number_of_sides_in_ts == 1:  # Active broker not shown
                self.journal_df.loc[(self.journal_df["Action"] == "Trade") & (self.journal_df["Timestamp"] == ts) & (self.journal_df["Condition Codes"] != "Y"), "Trade Type"] = "Passive"

            elif number_of_sides_in_ts > 1:  # Both Bid & Ask brokers are shown
                # Because "New Order" is always shown before the "Trades" get printed, we know that the broker who sent the "New Order" was the active buyer / seller.
                for ix, row in group.iterrows():
                    if row["Action"] == "New Order" and row["Position"] == 1:
                        active_side = row["Side"]  # Identify whether the active side is 'BID' or 'ASK'
                    elif row["Action"] == "Trade" and pd.isnull(row["Trade Type"]):
                        self.journal_df.loc[ix, "Trade Type"] = "Active" if row["Side"] == active_side else "Passive"

    @property
    def unrecorded_trades(self) -> pd.DataFrame:
        """Return the trades that have been executed, but not shown in `self.journal_df`"""
        trades_tuples: list[tuple] = list(zip(self.trade_prints["time"], self.trade_prints["value"], self.trade_prints["size"]))
        recorded_trades_tuples: list[tuple] = list(zip(pd.to_datetime(self.journal_df["Timestamp"]), self.journal_df["Price"], self.journal_df["Size"]))
        unrecorded_trades: list[tuple] = list(set(trades_tuples) - set(recorded_trades_tuples))

        df = pd.DataFrame()
        for ts, px, qty in unrecorded_trades:
            df = pd.concat([df, self.trade_prints.query("time == @ts and value == @px and size == @qty")])
        return df.sort_index()

    def concat_top_auction(self):
        """Create an initial paint of TOP at 9:20 (right after the auction) and concat to the `self.all_TOP_df`"""
        auc_time = self.all_MBO_df["MBO_TIME_RT"].iloc[0]  # Time when auction session ends and MBO starts to update
        TOP_df_during_auction = self.all_TOP_df[self.all_TOP_df.MBO_TIME_RT < auc_time]  # TOP in the auction session
        top_bid = pd.DataFrame(columns=["MBO_TIME_RT", "MBO_BID_POSITION_RT", "MBO_BID_RT", "MBO_BID_BROKER_RT"])
        top_ask = pd.DataFrame(columns=["MBO_TIME_RT", "MBO_ASK_POSITION_RT", "MBO_ASK_RT", "MBO_ASK_BROKER_RT"])

        for ix, event in TOP_df_during_auction.iterrows():
            side = event["MKTDEPTH_EVENT_SUBTYPE"]
            df = top_bid if side == 'BID' else top_ask
            if event["MD_TABLE_CMD_RT"] == "REPLACE":
                df.loc[event[f"MBO_{side}_POSITION_RT"]] = [event["MBO_TIME_RT"], event[f"MBO_{side}_POSITION_RT"], event[f"MBO_{side}_RT"], event[f"MBO_{side}_BROKER_RT"]]
            elif event["MD_TABLE_CMD_RT"] == "REPLACE_CLEAR":
                df.drop(event[f"MBO_{side}_POSITION_RT"], inplace=True)
            if side == 'BID':
                top_bid = df
            elif side == "ASK":
                top_ask = df

        top_bid["MBO_TIME_RT"], top_ask["MBO_TIME_RT"] = self.get_time_to_second(auc_time), self.get_time_to_second(auc_time)
        self.all_TOP_df = pd.concat([self.all_TOP_df, top_bid, top_ask])

    def run(self):
        """
        This function is the main program to update the orderbook and generate output
        """

        self.all_MBO_df = self.all_msg_df.loc[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE != "TABLE_INITPAINT") & (self.all_msg_df.EID == 53714)].dropna(how="all", axis=1)
        self.all_TOP_df = self.all_msg_df.loc[(self.all_msg_df.MKTDEPTH_EVENT_TYPE == "MARKET_BY_ORDER") & (self.all_msg_df.MKTDEPTH_EVENT_SUBTYPE != "TABLE_INITPAINT") & (self.all_msg_df.EID == 14112)].dropna(how="all", axis=1)
        self.concat_top_auction()  # insert top broker book after auction into all_TOP_df
        self.get_initpaint("BID")
        self.get_initpaint("ASK")

        for time in tqdm(self.all_MBO_df["MBO_TIME_RT"].unique()):
            mbo = self.all_MBO_df[self.all_MBO_df.MBO_TIME_RT == time].reset_index(drop=True)
            top = self.all_TOP_df[self.all_TOP_df.MBO_TIME_RT == self.get_time_to_second(time)].reset_index(drop=True)
            cmd_list = mbo["MD_TABLE_CMD_RT"].to_list()
            type_list = mbo["MKTDEPTH_EVENT_SUBTYPE"].to_list()
            # reorder_sign = Technique().reorder_indicator(cmd_list, type_list)

            for j, row in mbo.iterrows():
                if type_list[j] == "BID":
                    if row.MBO_BID_POSITION_RT not in self.df_bid.MBO_BID_POSITION_RT or not pd.isna(self.df_bid.loc[self.df_bid.MBO_BID_POSITION_RT == row.MBO_BID_POSITION_RT, "MBO_BID_BROKER_RT"].values[0]):  # type: ignore
                        self.update_journal("BID", cmd_list[j], row, top)

                if type_list[j] == "ASK":
                    if row.MBO_ASK_POSITION_RT not in self.df_ask.MBO_ASK_POSITION_RT or not pd.isna(self.df_ask.loc[self.df_ask.MBO_ASK_POSITION_RT == row.MBO_ASK_POSITION_RT, "MBO_ASK_BROKER_RT"].values[0]):  # type: ignore
                        self.update_journal("ASK", cmd_list[j], row, top)

                # if reorder_sign[j] is True:
                self.df_bid = self.df_bid.sort_values(by=["MBO_BID_POSITION_RT"]).reset_index(drop=True)
                self.df_bid.MBO_BID_POSITION_RT = range(1, len(self.df_bid) + 1)
                self.df_ask = self.df_ask.sort_values(by=["MBO_ASK_POSITION_RT"]).reset_index(drop=True)
                self.df_ask.MBO_ASK_POSITION_RT = range(1, len(self.df_ask) + 1)

        self.journal_df = pd.DataFrame(self.journal, columns=["Timestamp", "BrokerID", "Price", "Side", "Size", "Action", "Position"])
        # self.journal_df = self.journal_df[(self.journal_df.BrokerID != "") & (self.journal_df.BrokerID != "0000")].reset_index(drop=True)
        self.journal_df = self.journal_df[self.journal_df.BrokerID != ""].reset_index(drop=True)
        self.journal_df["Timestamp"] = self.journal_df["Timestamp"] + timedelta(hours=8)  # type: ignore
        self.journal_df["Timestamp"] = self.journal_df["Timestamp"].apply(lambda x: x.replace(tzinfo=None))
        self.journal_df["Price"] = self.journal_df["Price"].apply(lambda x: round(x, 3))  # FIXME: rounding digits

        self.match_trades()

        self.journal_df["Broker Name"] = self.journal_df["BrokerID"].map(self.broker_ID_dict)
        # self.record_unmatched_trades()

        # Formatting
        d = {"ADD": "New Order", "DEL": "Removed Order", "MOD": "Order Modified", "Executed": "Trade", "Trade": "Trade"}
        self.journal_df["Action"] = self.journal_df["Action"].map(d)
        self.journal_df["Timestamp"] = pd.to_datetime(self.journal_df["Timestamp"])
        self.journal_df["Timestamp"] = self.journal_df["Timestamp"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f"))

        self.identify_active_passive_trades()
