import datetime
import numpy as np
import pandas as pd
DIVISOR_PRICE  = 1000000
DIVISOR_QTY = 1000000
DIVISOR_INTEREST = 1000000
DIVISOR_DELTA = 1000000
DIVISOR_DECIMAL = 1000000
BID_OFFER_RECORD  = 140
TRADE_RECORD = 49
STOCK_INFO = 269
AUCTION_RECODE = 62
MAX_BID = 9223372036854.775807
MIN_OFFER = 0.000001
securities = dict()
import copy
class Security:
    def __init__(self, order_book_id,
                 instrunement_type,
                 short_name,
                 market_id,
                 market_list_id,
                 closing_price,
                 round_lot,
                 subscription_group,
                 security_type,
                 is_odd_lot,
                 upper_price_limit,
                 lower_price_limit,
                 allow_shot_sell,
                 allow_shor_sell_on_ndvr,
                 allow_ndvr,
                 closing_date):
        self.order_book_id = order_book_id #numberic code used to represent the security
        self.instrument_type = instrunement_type # Symbol
        self.short_name = short_name # Symbol e.g. AAV
        self.market_id = market_id #MARKET e.g. SET
        self.market_list_id  = market_list_id # type of security e.g. common stock
        self.closing_price = closing_price # last closing price
        self.round_lot = round_lot # minimum number of securities to be bid or offered.
        self.subscription_group = subscription_group #group of id of security
        self.security_type = security_type # e.g., "F" = Forieng common stock, "I" = Index Warrant...
        self.is_odd_lot = is_odd_lot # no idea what odd lot is
        self.ceil_price = upper_price_limit # ceiling price
        self.floor_price = lower_price_limit # floor price
        self.allow_short_sell = allow_shot_sell
        self.allow_short_sell_on_nvdr = allow_shor_sell_on_ndvr
        self.allow_nvdr = allow_ndvr #e.g. SELL_ONLY
        self.closing_date = closing_date # last closing date


def read296(message):
    actions = ["ADD", "UPDATE", None, "REMOVE","REMOVE","REMOVE","REMOVE","REMOVE"]
    truncated_message = message[1:-1]
    separated_parts = truncated_message.split("|")
    closing_price = 0
    upper_price_limit = 100000000000000
    lower_price_limit = 0
    closing_date = None
    for part in separated_parts:
        key, data = part.split("=")
        key = int(key)
        if key == 8:
            name = data
        elif key == 3:
            action = actions[int(data)-1]
        elif key == 4:
            pass #sateSeqN which is the same to all
        elif key ==6:
            time_stamp = datetime.datetime.strptime(data, '%Y-%m-%dT%H:%M:%S.%f')
        elif key == 7:
            sym_id = data
        elif key == 9:
            internal_segmentID = data
        elif key == 10:
            if data == "T":
                is_enable = True
            else:
                is_enable = False
        elif key == 11:
            pass # document suggests not to mess with this one
        elif key == 12:
            disabled_count = int(data)
        elif key == 13:
            instrument_code = data
        elif key == 14:
            instrument_type = data #all SYMB
        elif key == 15:
            currency = data
        elif key == 16:
            pass # same as instrument_code
        elif key == 17:
            market = data
        elif key == 18:
            market_list = data
        elif key == 19:
            segmentID = data
        elif key == 20:
            closing_price = int(data)/ DIVISOR_PRICE
        elif key == 22:
            order_book_id = int(data)
        elif key == 23:
            pass # have not idea what TRANPARENT_ONLY mean
        elif key == 24:
            round_lot = int(int(data)/DIVISOR_QTY)
        elif key == 25:
            if data == "T":
                is_routing_order = True
            else:
                is_routing_order = False
        elif key == 27:
            subscription_group_id = int(data)
        elif key == 28:
            pass # no idea
        elif key == 31:
            pass # no need for first valid date
        elif key == 32:
            pass # no need fo last valid date
        elif key == 501:
            secuity_type = data
        elif key == 502:
            if data == "T":
                is_odd_lot = True
            else:
                is_odd_lot = False
        elif key == 503:
            upper_price_limit = int(data) / DIVISOR_PRICE
        elif key == 504:
            lower_price_limit = int(data) / DIVISOR_PRICE
        elif key == 507:
            if data == "T":
                allow_short_sell = True
            else:
                allow_short_sell = False
        elif key == 508:
            if data == "T":
                allow_short_sell_nvdr = True
            else:
                allow_short_sell_nvdr = False
        elif key == 509:
            try:
                allow_nvdr = ["ALL", "SELL_ONLY", "NONE"][int(data)-1]
            except:
                print(data)
        elif key == 509:
            allow_ttf = ["ALL", "SELL_ONLY", "NONE"][int(data)-1]
        elif key == 518:
            closing_date = datetime.datetime.strptime(data, '%Y-%m-%d')
        elif key == 525:
            last_trade_price = int(data) / DIVISOR_PRICE
        elif key ==528:
            beneficial_sign = data
    security = Security(order_book_id, instrument_type, instrument_code, market, market_list, closing_price, round_lot, subscription_group_id,
                        secuity_type, is_odd_lot, upper_price_limit, lower_price_limit, allow_short_sell,
                        allow_short_sell_nvdr, allow_nvdr, closing_date)
    return security

def read140(message):
    truncated_message = message[1:-1]
    separated_parts = truncated_message.split("|")

    corresponding_price = None # Only valid for bound
    for part in separated_parts:
        key, data = part.split("=")
        key = int(key)
        if key == 1:
            subscription_group = int(data)
        elif key == 2:
            sequence_number = int(data)
        elif key == 3:
            time_of_event = datetime.datetime.strptime(data, '%Y-%m-%dT%H:%M:%S.%f')
        elif key == 4:
            pass # no idea what it is
        elif key == 5:
            order_book_id = int(data)
        elif key == 6:
            if data == "T":
                order_type = "BID"
            else:
                order_type = "OFFER"
        elif key == 7:
            price = int(data) / DIVISOR_PRICE
        elif key  == 8:
            volume = int(data) / DIVISOR_QTY
        elif key == 9:
            ## print(data)
            event_type = ["INSERT", "CANCEL", "UPDATE"][int(data) - 1]
        elif key == 501:
            corresponding_price = int(data)
    return subscription_group, sequence_number, time_of_event, order_book_id, order_type, price, volume, event_type, corresponding_price

def read62(message):
    truncated_message = message[1:-1]
    separated_parts = truncated_message.split("|")
    resume_time = None  # Only valid for bound
    corresponding_price= None
    for part in separated_parts:
        key, data = part.split("=")
        key = int(key)
        key = int(key)
        if key == 1:
            subscription_group = int(data)
        elif key == 2:
            sequence_number = int(data)
        elif key == 3:
            time_of_event = datetime.datetime.strptime(data, '%Y-%m-%dT%H:%M:%S.%f')
        elif key == 4:
            pass  # no idea what it is
        elif key == 5:
            order_book_id = int(data)
        elif key == 6:
            imbalance = int(data) / DIVISOR_QTY
        elif key == 7:
            calculated_action_price = int(data) / DIVISOR_PRICE
        elif key == 8:
            resume_time = datetime.datetime.strptime(data, '%H:%M:%S')
        elif key == 9:
            matched_quantity = int(data) / DIVISOR_QTY
        elif key == 501:
            if data == "T":
                is_final = True
            else:
                is_final = False
        elif key == 502:
            corresponding_price = int(data) / DIVISOR_PRICE
    return  subscription_group, sequence_number, time_of_event, order_book_id, imbalance, calculated_action_price, resume_time, matched_quantity, is_final, corresponding_price

def read49(message):
    truncated_message = message[1:-1]
    separated_parts = truncated_message.split("|")

    corresponding_price = None
    reference_trade_id = None
    is_tbl = None
    bid_side_is_aggressor = None
    ask_side_is_aggressor = None
    high_price = None
    low_price = None
    last_trade_price = None
    last_auction_price = None
    last_ref_price = None
    avg_price = None
    opening_price = None
    percentage_change = None
    for part in separated_parts:
        key, data = part.split("=")
        key = int(key)
        key = int(key)
        if key == 1:
            subscription_group = int(data)
        elif key == 2:
            sequence_number = int(data)
        elif key == 3:
            time_of_event = datetime.datetime.strptime(data, '%Y-%m-%dT%H:%M:%S.%f')
        elif key == 4:
            pass  # no idea what it is
        elif key == 5:
            order_book_id = int(data)
        elif key == 6:
            trade_type = ["NEW", "BUSTED"][int(data) - 1]
        elif key == 7:
            sub_type_trade = int(data)
        elif key == 9:
            time_of_trade = datetime.datetime.strptime(data, '%Y-%m-%dT%H:%M:%S.%f')
        elif key == 10:
            order_qty = int(data) / DIVISOR_QTY
        elif key == 11:
            price = int(data) / DIVISOR_PRICE
        elif key == 12:
            corresponding_price = int(data) / DIVISOR_PRICE
        elif key == 13:
            trade_id = data
        elif key == 14:
            deal_id = data
        elif key == 16:
            if data == "T":
                is_tbl = True
            else:
                is_tbl = False
        elif key == 19:
            last_auction_price = int(data) / DIVISOR_PRICE
        elif key == 20:
            high_price = int(data) / DIVISOR_PRICE
        elif key == 21:
            low_price = int(data) / DIVISOR_PRICE
        elif key == 22:
            total_vol = int(data) / DIVISOR_QTY
        elif key == 23:
            total_turn_over = int(data) / DIVISOR_PRICE
        elif key == 24:
            last_trade_price = int(data) / DIVISOR_PRICE
        elif key == 25:
            reference_trade_id = data
        elif key == 26:
            if data == "T":
                update_high_low = True
            else:
                update_high_low = False
        elif key == 27:
            if data == "T":
                update_volume_turn_over = True
            else:
                update_volume_turn_over = False
        elif key == 28:
            last_ref_price = int(data) / DIVISOR_PRICE
        elif key == 29:
            avg_price = int(data) / DIVISOR_PRICE
        elif key == 30:
            if data == "T":
                update_avg_price = True
            else:
                update_avg_price =  False
        elif key == 31:
            if data == "T":
                update_last_paid_qualified = True
            else:
                update_last_paid_qualified = False
        elif key == 501:
            if data == "T":
                bid_side_is_aggressor = True
            else:
                bid_side_is_aggressor = False
        elif key == 502:
            if data == "T":
                ask_side_is_aggressor = True
            else:
                ask_side_is_aggressor = False
        elif key == 503:
            opening_price = int(data) / DIVISOR_PRICE
        elif key == 504:
            pass
        elif key == 505:
            total_vol_trade_report = int(data) / DIVISOR_QTY
        elif key == 506:
            total_turn_over_report = int(data) / DIVISOR_PRICE
        elif key == 513:
            pass #corresponding_low_price = int(data) /DIVISOR_PRICE
        elif key == 514:
            pass
        elif key == 515:
            total_num_trades = int(data)
        elif key == 516:
            pass
        elif key == 517:
            percentage_change = int(data) / DIVISOR_INTEREST
    return subscription_group, sequence_number, time_of_event, order_book_id, trade_type, sub_type_trade, time_of_trade,\
           order_qty, price, corresponding_price, trade_id, deal_id, is_tbl, last_auction_price, high_price, low_price, \
           total_vol, total_turn_over, last_trade_price, reference_trade_id, update_high_low, update_avg_price, \
           update_volume_turn_over, last_ref_price, avg_price, update_last_paid_qualified, bid_side_is_aggressor, \
           ask_side_is_aggressor, opening_price, total_vol_trade_report, total_vol_trade_report, total_num_trades, \
           percentage_change

def printBids(bids):
    prices = np.sort(list(bids.keys()))[::-1]
    text = "BIDS: "
    for price in prices:
        text += f"{price}: {bids[price]} "
    # print(text)

def printOffers(offers):
    prices = np.sort(list(offers.keys()))
    text = "OFFER: "
    for price in prices:
        text += f"{price}: {offers[price]} "
    # print(text)

def printBidsOffers(bids, offers):
    max_v = 9223372036854.775807
    min_v = 0.000001
    def check(value):
        if value == min_v:
            return "MIN_OFFER"
        elif value == max_v:
            return "MAX_BID"
        else:
            return f"{value:4.2f}"
    def text_vol(value):
        if value != " ":
            return f"{int(value):d}"
        else:
            return value


    b_prices = list(bids.keys())
    o_prices = list(offers.keys())
    prices = np.sort(list(set(b_prices + o_prices)))[::-1]
    buys = []
    sells = []
    for price in prices:
        if price in b_prices:
            buys.append(bids[price])
        else:
            buys.append(" ")
        if price in o_prices:
            sells.append(offers[price])
        else:
            sells.append(" ")
    data = dict()

    data['prices'] = prices
    data['bids'] = buys
    data['offers'] = sells
    df = pd.DataFrame(data)
    df['prices']  = df['prices'].map(check)
    df['bids'] = df['bids'].map(text_vol)
    df['offers'] = df['offers'].map(text_vol)
    # print(df)

def dailyBidOfferRecords(instrument_book_id, file_name):
    bids = dict()
    offers = dict()
    prices = []
    records = []
    with open(file_name, "r") as fp:
        for lin in fp:
            idx1 = lin.index("=")
            head = lin[:idx1]
            idx = head.index("]")
            timestamp = datetime.datetime.strptime(head[1:idx], '%d/%m/%y %H:%M:%S.%f')
            code = int(head[idx + 1:])
            if code == BID_OFFER_RECORD:
                message = lin[idx1 + 1:-1]
                subscription_group, sequence_number, time_of_event, order_book_id, order_type, price, volume, \
                event_type, corresponding_price = read140(message)
                if order_book_id == instrument_book_id:
                    if price not in prices:
                        prices.append(price)
                    if order_type == "BID":
                        if price in list(bids.keys()):
                            if event_type == "UPDATE":
                                bids[price] = volume
                            elif event_type == "CANCEL":
                                bids.pop(price)
                        else:
                            bids[price] = volume
                    elif order_type == "OFFER":
                        if price in list(offers.keys()):
                            if event_type == "UPDATE":
                                offers[price] = volume
                            elif event_type == "CANCEL":
                                offers.pop(price)
                        else:
                            offers[price] = volume
                    records.append([time_of_event, copy.deepcopy(bids), copy.deepcopy(offers)])
                    # print("============= ", time_of_event, " ==================")
                    # printBidsOffers(bids, offers)
                    # print("===========================================")
            elif code == AUCTION_RECODE:
                message = lin[idx1 + 1:-1]
                subscription_group, sequence_number, time_of_event, order_book_id, imbalance, calculated_action_price, \
                resume_time, matched_quantity, is_final, corresponding_price = read62(message)
                if order_book_id == instrument_book_id:  # ADVANC
                    if is_final:
                        bid_prices = np.sort(list(bids.keys()))[::-1]
                        offer_prices = np.sort(list(offers.keys()))
                        total_bid = matched_quantity
                        bids_succeses = dict()
                        for b_price in bid_prices:
                            if b_price >= calculated_action_price:
                                if bids[b_price] <= total_bid:
                                    bids_succeses[b_price] = bids[b_price]
                                    total_bid -= bids[b_price]
                                    bids.pop(b_price)
                                else:
                                    bids_succeses[b_price] = total_bid
                                    bids[b_price] -= total_bid
                                    total_bid = 0
                                    break
                        total_offer = matched_quantity
                        offer_successes = dict()
                        for o_price in offer_prices:
                            if o_price <= calculated_action_price:
                                if offers[o_price] <= total_offer:
                                    offer_successes[o_price] = offers[o_price]
                                    total_offer -= offers[o_price]
                                    offers.pop(o_price)
                                else:
                                    offer_successes[o_price] = total_offer
                                    offers[o_price] -= total_offer
                                    total_offer = 0
                                    break

                        records.append([time_of_event, copy.deepcopy(bids), copy.deepcopy(offers)])
                        # print("============= ", time_of_event, " ==================")
                        # print("AUCTION************************************")
                        # printBidsOffers(bids, offers)
                        # print("===========================================")

    if len(records) > 0:
        # replays
        times = []
        for time, bid_t, offer_t in records:
            times.append(time)
        bids = {"Time": times}
        offers = {"Time": times}
        #num_columns = len(prices)
        prices = np.sort(prices)
        times = []
        for price in prices:
            bids[price] = []
            offers[price] = []
        for time, bid_t, offer_t in records:
            times.append(time)
            for price in prices:
                if price in list(bid_t.keys()):
                    bids[price].append(bid_t[price])
                else:
                    bids[price].append(0)
                if price in list(offer_t.keys()):
                    offers[price].append(offer_t[price])
                else:
                    offers[price].append(0)



        df_bid = pd.DataFrame(data=bids)
        df_bid = df_bid.rename(columns={MIN_OFFER: "MARKET_OFFER", MAX_BID: "MARKET_BID"})
        df_offer = pd.DataFrame(data=offers)
        df_offer = df_offer.rename(columns={MIN_OFFER: "MARKET_OFFER", MAX_BID: "MARKET_BID"})
        return  df_bid, df_offer

def dailyTradeRecords(instrument_book_id, file_name):

    prices = []
    times = []
    vols = []
    records = []
    with open(file_name, "r") as fp:
        for lin in fp:
            idx1 = lin.index("=")
            head = lin[:idx1]
            idx = head.index("]")
            timestamp = datetime.datetime.strptime(head[1:idx], '%d/%m/%y %H:%M:%S.%f')
            code = int(head[idx + 1:])
            if code == TRADE_RECORD:
                if code == 49:
                    message = lin[idx1 + 1:-1]
                    subscription_group, sequence_number, time_of_event, order_book_id, trade_type, sub_type_trade, time_of_trade, \
                    order_qty, price, corresponding_price, trade_id, deal_id, is_tbl, last_auction_price, high_price, low_price, \
                    total_vol, total_turn_over, last_trade_price, reference_trade_id, update_high_low, update_avg_price, \
                    update_volume_turn_over, last_ref_price, avg_price, update_last_paid_qualified, bid_side_is_aggressor, \
                    ask_side_is_aggressor, opening_price, total_vol_trade_report, total_vol_trade_report, total_num_trades, \
                    percentage_change = read49(message)
                    if  order_book_id == instrument_book_id:
                        times.append(time_of_trade)
                        prices.append(price)
                        vols.append(order_qty)
                        # print("============= ", time_of_event, " ==================")
                        # print(f"Settle Price {price} with volumne {order_qty}!")
                        # print("===========================================")
            data = dict()
            data['Time'] = times
            data["Prices"] = prices
            data['Volumes'] = vols
            df = pd.DataFrame(data)
            df = df.set_index("Time")
        return df







# ###
# df_bid, df_offer = dailyBidOfferRecords(1069,r"E:\CMDF_Data\feed-mbl-20171201.txt") # Record bids and offers prices vs volumes at event times
# df_bid = df_bid.set_index("Time")
# df_offer = df_offer.set_index("Time")
# df_bid.to_csv(r"E:\CMDF_Data\ADVANC1069BID.csv")
# df_offer.to_csv(r"E:\CMDF_Data\ADVANC1069OFFER.csv")






# df_trade = dailyTradeRecords(1069, r"E:\CMDF_Data\feed-trade-20171201.txt") # Record of all sellted order with prices and volumes
# df_trade.to_csv(r"E:\CMDF_Data\ADVANC1069Trade.csv")





