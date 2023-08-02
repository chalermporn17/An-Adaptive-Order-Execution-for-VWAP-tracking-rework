import os
import shutil
import numpy as np
import csv

import ftputil
from ftplib import FTP

from mds_process.orderbook import Create_OrderBook
from asynchronous import run_function_multiple_times_multiple_processes

# def 

# def gen_orderbook(root_path, filename):
def gen_orderbook(file_info):
    root_path = file_info[0]
    filename = file_info[1]

    print('Generate OrderBook :', filename)

    ftp = FTP('192.168.1.10')
    ftp.login(user='mook', passwd = 'M_0896144992')
    ftp.cwd(root_path)

    save_path = os.path.join('./feed-mbl-output', filename.split('.')[0])

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    localfile.close()

    ord_book = Create_OrderBook(filename)
    ord_book.search_data(res_keys=['13','22'], data=ord_book.split_tables['296'])
    save_ord_name = os.path.join(filename.split('.')[0] + '-orderbook.csv')
    ord_book.save_orderbook(save_ord_name)

    shutil.move(filename, os.path.join(save_path, filename))
    shutil.move(save_ord_name, os.path.join(save_path, save_ord_name))

    ftp.quit()

def generate_orderbook_nfs(file_info):
    root_path = file_info[0]
    filename = file_info[1]

    save_ord_name = filename.split('.')[0] + '-orderbook.csv'
    save_ord_path = os.path.join(root_path, save_ord_name)
    try:
        if os.path.isfile(save_ord_path):
            print('Orderbook already created.')
        else:
            print('Generate OrderBook :', filename)
            ord_book = Create_OrderBook(os.path.join(root_path, filename))
            ord_book.search_data(res_keys=['13','22'], data=ord_book.split_tables['296'])
            ord_book.save_orderbook(save_ord_path)
    except:
        print('Cannot Create Orderbook')

if __name__ == "__main__":

    if False:
        a_host = ftputil.FTPHost('192.168.1.10', 'mook','M_0896144992')

        dir_list = []
        for root, dirs, files in a_host.walk('/set_mkt_micro'):
            for dir in dirs:
                if 'feed-mbl' in dir:
                    dir_list.append(dir)

        file_list = []
        for root, dirs, files in a_host.walk('/set_mkt_micro/' + dir_list[1]):
            for f in files:
                if f.endswith('.txt'):
                    file_list.append([root, f])
                    # gen_orderbook(root_path=root, 
                    #                 filename=f)
                    

        data = run_function_multiple_times_multiple_processes(
                    gen_orderbook,
                    [
                        [file_name] for file_name in file_list
                    ],
                )

    if False:
        ord_list = []
        data_list = []
        for root, dirs, files in os.walk('./feed-mbl-output'):
            for f in files:
                if f.endswith('.csv'):
                    ord_list.append(os.path.join(root, f))
                if f.endswith('.txt'):
                    data_list.append(os.path.join(root, f))

        i = 0
        print(ord_list[i])
        print(data_list[i])

        key_value = np.loadtxt("filename.csv", delimiter=",")
        mydict = { k:v for k,v in key_value }
        print(mydict)


    dir_mbl_list = []
    dir_trade_list = []
    for root, dirs, files in os.walk('/mnt/nfs/set_mkt-micro'):
        for dir in dirs:
            if 'feed-mbl' in dir:
                dir_mbl_list.append(dir)
            
            elif 'feed-trade' in dir:
                dir_trade_list.append(dir)
    

    # for i in range(len(dir_mbl_list)):
    #     file_list = []
    #     for root, dirs, files in os.walk(os.path.join('/mnt/nfs/set_mkt-micro', dir_mbl_list[i])):
    #         for f in files:
    #             if f.endswith('.txt'):
    #                 file_list.append([root, f])

    #     data = run_function_multiple_times_multiple_processes(
    #                 generate_orderbook_nfs,
    #                 [
    #                     [file_name] for file_name in file_list
    #                 ],
    #             )
    for i in range(len(dir_trade_list)):
        file_list = []
        for root, dirs, files in os.walk(os.path.join('/mnt/nfs/set_mkt-micro', dir_trade_list[i])):
            for f in files:
                if f.endswith('.txt'):
                    file_list.append([root, f])

        data = run_function_multiple_times_multiple_processes(
                    generate_orderbook_nfs,
                    [
                        [file_name] for file_name in file_list
                    ],
                )

        # generate_orderbook_nfs(file_list[0])

