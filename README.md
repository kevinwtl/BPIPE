# BPIPE Project Specification

## Data Types (for HKEx)

### 1. TOP (Top Brokers)

`'MKTDEPTH_EVENT_TYPE' == 'MARKET_BY_ORDER' and 'EID' == 14112`

* Broker ID
* Queue position at that price level
* Bid / Offer price
* **Does not show size**
* Timestamp (rounded to seconds, due to Bloomberg Backend Issues) when the order was *submited*

### 2. MBO (Market By Order)

`'MKTDEPTH_EVENT_TYPE' == 'MARKET_BY_ORDER' and 'EID' == 53714`

* **Does not show Broker ID**
* Queue position at that price level
* Bid / Offer price
* Size
* Timestamp (accruate to the miliseconds) when the order was *updated* (for example partially filled and outstanding size changes)

### 3. MBL (Market By Level)

`'MKTDEPTH_EVENT_TYPE' == 'MARKET_BY_LEVEL' and 'EID' == 14112`

* Number of orders at each price level
* Total Size of all orders at each price level
* Price Level
* Timestamp (accruate to the miliseconds)

### 4. TRADE

`'MKTDATA_EVENT_TYPE' == 'TRADE'`. A free data stream that does not require BPIPE subscription.

* Traded Price
* Traded Size
* Condition Code of Trade, [more information on HKEx](https://www.hkex.com.hk/Services/Trading/Securities/Overview/Trading-Mechanism?sc_lang=en)
* Time of execution (accruate to the miliseconds)

## Data Parsing

`concat_top_auction()` keeps record of TOP in the premarket auction session to generate the inital paint of the TOP. When market opens at 9:30:00, MBO and TOP starts to update information on order book changes. However, the initial paint of MBO has updated at some time after 9:20:00 when the premarket auction session is closed. 

Combining the initial paints of TOP and MBO, the initial paint of an order book with brokers' ID is created and `add_del_mod()` will be updated continuously when MBO pushes updates. 

`update_journal()` records an entry in the journal each time when there is an update of the current order book, and the corresponding broker's ID will be searched from TOP based on the unique timestamp, order position, and price level. If no broker information is provided in the TOP, a broker ID '0000' and a broker name 'TODO' is recorded.

`match_trades()` mateches the journal of all broker activities according to the TRADE dataset in terms of its timestamp, price, size and order position.

Then the broker name is mapped from the broker list downloaded from [HKEx website](https://www.hkex.com.hk/eng/plw/search.aspx?selecttype=se) for each request.

`identify_active_passive_trades()` indentifies which side is the active broker and which is the passive one. Because "New Order" is always shown before the "Trades" get printed, then the broker who sent the "New Order" was the active buyer / seller.

## Target output

### 1. Journal of all broker activities
| Timestamp  | Broker ID | Price | Side | Size  | Action         | Broker Name                                    | Trade Type |
| ---------- | --------- | ----- | ---- | ----- | -------------- | ---------------------------------------------- | ---------- |
| 9:30:14:35 | 6996      | 2.38  | Ask  | 88000 | New Order      | China Investment Information Services Limited  |            |
| 9:31:24:15 | 6996      | 2.38  | Ask  | 2000  | Trade Executed | China Investment Information Services Limited  |            |
| 9:35:21:05 | 6996      | 2.38  | Ask  | 86000 | Removed Order  | China Investment Information Services Limited  |            |



### 2. Visalization of Broker Activities
![boker_visalization](res/boker_visalization.png)

## Note

* The trades in premarket auction session are not matched yet.
* Due to the limitation of data, it is possible that not all trades are matched. For example, the broker who puts the order and whose order gets executed immediately will not shown in the TOP, and only the other side of the broker can be identified.
