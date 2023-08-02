import os
import shutil
import numpy as np
import csv
import pandas as pd
import time

from bid_offer_processor import feed_trade_processor

folder_name = 'feed-trade-201712'

feed_list = []
ord_list = []

for root, dirs, files in os.walk(os.path.join('/mnt/nfs/set_mkt-micro', folder_name)):
    for f in files:
        if f.endswith('.txt'):
            feed_list.append(f)
        elif f.endswith('.csv'):
            ord_list.append(f)

if len(feed_list) == len(ord_list):

    for i in range(len(feed_list)):
        root_path = os.path.join('/mnt/nfs/set_mkt-micro', folder_name)
        feed_name = feed_list[i]
        ord_name = feed_name.split('.')[0] + '-orderbook.csv'

        ### Check feed-trade is exist ###
        if os.path.isfile(os.path.join(root_path, feed_name)):
            ### Check orderbook is already generate ###
            if os.path.isfile(os.path.join(root_path, ord_name)):
                save_bit_offer_path = os.path.join(root_path + '-Trade', feed_name.split('.')[0])
                if not os.path.exists(save_bit_offer_path):
                    feed_trade_processor(root_path, ord_name, feed_name)
                else:
                    print('Already create bit_offer')
            else:
                print('Orderbook doesnt exist')
        else:
            print('feed-trade doesnt exist')
            
else:       
    print('Number of feed-mbl and orderbook not matched')
    print('MBL :', len(mbl_list))
    print('Orderbook', len(ord_list))