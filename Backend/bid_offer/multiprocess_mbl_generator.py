import os
import shutil
import numpy as np
import csv
import pandas as pd
import time

from bid_offer_processor import mbl_processor
from asynchronous import run_function_multiple_times_multiple_processes

def autogen(folder_name):
    mbl_list = []
    ord_list = []
    for root, dirs, files in os.walk(os.path.join('/mnt/sda/SetData', folder_name)):
        for f in files:
            if f.endswith('.txt'):
                mbl_list.append(f)
            elif f.endswith('.csv'):
                ord_list.append(f)

    if len(mbl_list) == len(ord_list):

        for i in range(len(mbl_list)):
            root_path = os.path.join('/mnt/sda/SetData', folder_name)
            mbl_name = mbl_list[i]
            ord_name = mbl_name.split('.')[0] + '-orderbook.csv'

            ### Check feed-mbl is exist ###
            if os.path.isfile(os.path.join(root_path, mbl_name)):
                ### Check orderbook is already generate ###
                if os.path.isfile(os.path.join(root_path, ord_name)):
                    save_bit_offer_path = os.path.join(root_path + '-bid_offer', mbl_name.split('.')[0])
                    if not os.path.exists(save_bit_offer_path):
                        mbl_processor(root_path, ord_name, mbl_name)
                    else:
                        print('Already create bit_offer')
                else:
                    print('Orderbook doesnt exist')
            else:
                print('feed-mbl doesnt exist')
                
    else:       
        print('Number of feed-mbl and orderbook not matched')
        print('MBL :', len(mbl_list))
        print('Orderbook', len(ord_list))

if __name__ == '__main__':

    # for i in range(1)

    folder_list = []
    for root, dirs, files in os.walk(os.path.join('/mnt/sda/SetData')):
        for dir in dirs:
            folder_list.append(dir)

    data = run_function_multiple_times_multiple_processes(
                autogen,
                [
                    [file_name] for file_name in folder_list
                ],
                max_workers=16
            )