import pandas as pd
import pathlib
import numpy as np

def read_txt( path ):
    f = open(path, "r")
    text = f.read()
    f.close()
    return text

def text_to_line( text ):
    lines = text.split('\n')
    i = 0
    while i != len(lines):
        if lines[i] == '':
            del lines[i]
        else:
            i+=1
    return lines

def read_text_line( path ):
    return text_to_line(read_txt(path))

def get_all_date():
    dates = read_text_line('Predictor/dates.txt')
    return dates

def get_df(QUOTE,date):
    base = pathlib.Path('/mnt/sda/SetData')

    data_path = base / f"feed-mbl-{date[:6]}-bid_offer" / f"feed-mbl-{date}"
    order_path = base / f"feed-mbl-{date[:6]}" / f"feed-mbl-{date}-orderbook.csv"

    df = pd.read_csv(order_path)

    orderbook_id = df[df['Name']==QUOTE].iloc[0]['id']
    bid_path = data_path / f"{QUOTE}{orderbook_id}BID.csv"
    offer_path = data_path / f"{QUOTE}{orderbook_id}OFFER.csv"

    bid_df = pd.read_csv(bid_path)
    offer_df = pd.read_csv(offer_path)

    bid_df['Time'] = pd.to_datetime(bid_df['Time'])
    offer_df['Time'] = pd.to_datetime(offer_df['Time'])

    bid_df.set_index('Time',inplace = True)
    offer_df.set_index('Time',inplace = True)



    bid_arr = bid_df.to_numpy()
    bid_price_arr = bid_arr[:,1:-1]
    bid_price_count = bid_df.columns[1:-1].to_numpy()
    bid_price_arr_reverse = bid_price_arr[:,::-1]
    bid_best_index = np.argmax((bid_price_arr_reverse>=1e-15)*1, axis=1)
    bid_best_index = len(bid_price_count) - bid_best_index - 1
    bid_best_price = bid_price_count[bid_best_index]
    bid_best_volume = bid_price_arr[np.arange(0,len(bid_best_index)),bid_best_index]
    bid_best_price = bid_best_price.astype(np.float64) * ( bid_best_volume > 1e-15 )

    offer_arr = offer_df.to_numpy()
    offer_price_arr = offer_arr[:,1:-1]
    offer_price_count = offer_df.columns[1:-1].to_numpy()
    offer_best_index = np.argmin((offer_price_arr<=1e-15)*1, axis=1)
    offer_best_price = offer_price_count[offer_best_index]
    offer_best_volume = offer_price_arr[np.arange(0,len(offer_best_index)),offer_best_index]
    offer_best_price = offer_best_price.astype(np.float64) * ( offer_best_volume > 1e-15 )

    bid_df['BestBid'] = bid_best_price
    offer_df['BestOffer'] = offer_best_price
    merged = pd.DataFrame(index=bid_df.index)
    merged['BestBid'] = bid_best_price
    merged['BestOffer'] = offer_best_price
    merged['BestBidVolume'] = bid_best_volume
    merged['BestOfferVolume'] = offer_best_volume

    return merged

def get_orderbook_id(QUOTE,date):
    base = pathlib.Path('/mnt/sda/SetData')

    order_path = base / f"feed-mbl-{date[:6]}" / f"feed-mbl-{date}-orderbook.csv"

    df = pd.read_csv(order_path)

    orderbook_id = df[df['Name']==QUOTE].iloc[0]['id']
    
    return orderbook_id