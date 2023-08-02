import datetime
import os
import numpy as np
import pandas as pd
import copy
import time
from tqdm import tqdm

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

def mbl_processor(root_path, ord_name, mbl_name):
    # root_path = '/mnt/nfs/set_mkt-micro/feed-mbl-201712'
    # ord_name = 'feed-mbl-20171204-orderbook.csv'
    # mbl_name = 'feed-mbl-20171204.txt'

    print('Start Process MBL BID OFFER -->', mbl_name)

    file_name = os.path.join(root_path, mbl_name)

    ord_path = os.path.join(root_path, ord_name)
    ord_df = pd.read_csv(ord_path, index_col=0)

    ord_list = ord_df['id'].to_list()
    data_dict = dict()

    for id in ord_list:
        temp_dict = {
            'bids' : dict(),
            'offers' : dict(),
            'prices' : [],
            'records' : []
        }
        data_dict.update({str(id) : temp_dict})

    # t1 = time.time()
    with open(file_name, "r") as fp:
        for lin in tqdm(fp):
            
            idx1 = lin.index("=")
            head = lin[:idx1]
            idx = head.index("]")
            timestamp = datetime.datetime.strptime(head[1:idx], '%d/%m/%y %H:%M:%S.%f')
            code = int(head[idx + 1:])

            # print(code)

            if code == BID_OFFER_RECORD:
                message = lin[idx1 + 1:-1]
                subscription_group, sequence_number, time_of_event, order_book_id, order_type, price, volume, \
                event_type, corresponding_price = read140(message)

                temp_dict = data_dict[str(order_book_id)]
                prices = temp_dict['prices']
                bids = temp_dict['bids']
                offers = temp_dict['offers']
                records = temp_dict['records'] 

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

            elif code == AUCTION_RECODE:
                message = lin[idx1 + 1:-1]
                subscription_group, sequence_number, time_of_event, order_book_id, imbalance, calculated_action_price, \
                resume_time, matched_quantity, is_final, corresponding_price = read62(message)
                if is_final:
                    temp_dict = data_dict[str(order_book_id)]
                    prices = temp_dict['prices']
                    bids = temp_dict['bids']
                    offers = temp_dict['offers']
                    records = temp_dict['records'] 

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

    # t2 = time.time()
    # print('Process time :', t2 - t1)

    # t3 = time.time()
    # for order_book_id in ord_list:
    for i in tqdm(range(len(ord_df))):

        temp_df = ord_df.iloc[i]

        order_book_id = temp_df['id']
        asset_name = temp_df['Name']
        asset_unique = temp_df['Unique_name']

        replace_path = root_path + '-bid_offer'
        save_path = os.path.join(replace_path, mbl_name.split('.')[0])

        if not os.path.exists(save_path):
            os.makedirs(save_path) 

        file_bid_name = asset_unique + 'BID.csv'
        save_bid_path = os.path.join(save_path, file_bid_name)
        file_offer_name = asset_unique + 'OFFER.csv'
        save_offer_path = os.path.join(save_path, file_offer_name)

        if os.path.isfile(save_bid_path):
            os.remove(save_bid_path)
            print('Delete old BID file')
        if os.path.isfile(save_offer_path):
            os.remove(save_offer_path) 
            print('Delete old OFFER file')

        temp_dict = data_dict[str(order_book_id)]
        prices = temp_dict['prices']
        bids = temp_dict['bids']
        offers = temp_dict['offers']
        records = temp_dict['records'] 

        if len(records) > 0:
            times = []
            for time, bid_t, offer_t in records:
                times.append(time)
            bids = {"Time": times}
            offers = {"Time": times}

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

            df_bid = df_bid.set_index("Time")
            df_offer = df_offer.set_index("Time")

            df_bid.to_csv(save_bid_path)
            df_offer.to_csv(save_offer_path)

    # t4 = time.time()
    # print('Record time :', t2 - t1)

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

def feed_trade_processor(root_path, ord_name, feed_name):

    print('Start Process Feed-Trade BID OFFER -->', feed_name)

    file_name = os.path.join(root_path, feed_name)

    ord_path = os.path.join(root_path, ord_name)
    ord_df = pd.read_csv(ord_path, index_col=0)

    ord_list = ord_df['id'].to_list()

    data_dict = dict()

    for id in ord_list:
        temp_dict = {
            'prices' : [],
            'times' : [],
            'vols' : [],
            'records' : []
        }
        data_dict.update({str(id) : temp_dict})

    with open(file_name, "r") as fp:
        for lin in tqdm(fp):
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

                    temp_dict = data_dict[str(order_book_id)]
                    prices = temp_dict['prices']
                    times = temp_dict['times']
                    vols = temp_dict['vols']
                    records = temp_dict['records'] 

                    times.append(time_of_trade)
                    prices.append(price)
                    vols.append(order_qty)

    for i in tqdm(range(len(ord_df))):

        temp_df = ord_df.iloc[i]

        order_book_id = temp_df['id']
        asset_name = temp_df['Name']
        asset_unique = temp_df['Unique_name']

        replace_path = root_path + '-Trade'
        save_path = os.path.join(replace_path, feed_name.split('.')[0])

        if not os.path.exists(save_path):
            os.makedirs(save_path) 

        file_trade_name = asset_unique + 'Trade.csv'
        save_trade_path = os.path.join(save_path, file_trade_name)


        if os.path.isfile(save_trade_path):
            os.remove(save_bid_path)
            print('Delete old TRADE file')

        temp_dict = data_dict[str(order_book_id)]
        prices = temp_dict['prices']
        times = temp_dict['times']
        vols = temp_dict['vols']
        records = temp_dict['records'] 

        data = dict()
        
        if len(times) > 0 and len(vols) > 0 and len(prices) > 0:
            if len(times) == len(vols) and len(times) == len(prices):
                data['Time'] = times
                data["Prices"] = prices
                data['Volumes'] = vols
                df = pd.DataFrame(data)
                df = df.set_index("Time")

                df.to_csv(save_trade_path)
