import pandas as pd 
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import traceback
try : 
    from modules.VaR_Page.VaR_Page import VaR_Page
    from modules.VaR_Page.read_MTM import ReadMarkToMarketData
    from utils.Utils import change_datetime_format
    from asynchronous import run_function_multiple_times_multiple_processes

    from modules.VaR_Page.create_VaR import VaRHypothentical
    from utils.MongoConnector import Mongo
except : 
    import sys 
    sys.path.append('../../')
    from modules.VaR_Page.VaR_Page import VaR_Page
    from modules.VaR_Page.read_MTM import ReadMarkToMarketData
    from utils.Utils import change_datetime_format
    from asynchronous import run_function_multiple_times_multiple_processes

    from modules.VaR_Page.create_VaR import VaRHypothentical
    from utils.MongoConnector import Mongo

def run_backtest(date, path, asset_type, num_month):
    print("\n\n", date) 
    print(f"Create new VaR BACKTEST data doc {date}")
    ts = time.time() 
    
    # if port_name == 'directional_trading' :
    #     mtm_data = read_mtm.read_to_market_data(port_name, date, 1, directional_trader=True, directional_asset_type=asset_type)
    #     all_subport_data = mtm_data[date]
    #     for subport_name in all_subport_data:
    #         print("--"*10)
    #         print("subport: ", subport_name)
    #         print("--"*10)
    #         data = var_page.create_hypo_var(date, path, subport_id=subport_name, directional_asset_type=asset_type, period=num_month)
    #     data = var_page.create_hypo_var(date, path, subport_id='All', directional_asset_type=asset_type, period=num_month)
    # else:
    data = var_page.create_hypo_var(date, backtest_range=num_month, num_path=path, asset_type='Equity')


if __name__ == "__main__":
    import time 
    var_page = VaRHypothentical()
    mongo_db = Mongo()
    print('-----')
    # read_data = var_page.read_var_mongo_data('block_trade', '2019-10-18')
    '''
    Create Data
    '''

    port_list = [
                    'block_trade', #later debug
                    # 'directional_trading', # running on screen 24
                    # 'directional_trading', # running on screen 24
                    # 'firm_underwriting', #checked
                    # 'general_investment', # running on screen 134 between 2020-12-25 to 2020-12-16 ONLY!
                    # 'treasury', 
                    # 'warehousing' #checked,
                    # 'trading_error'# running on screen 134 between 2020-12-25 to 2020-06-30 ONLY!!
                ] 
    date = None
    num_month = 3*12 # for backtest
    asset_type = 'Equity'
    path = 500000
    direct_count = 0 # 0: 'Equity', 1: 'TFEX'
    multi_process = False

    for port_name in port_list:
        var_page.port_name = port_name
        read_mtm = ReadMarkToMarketData()
        # want only date
        if port_name == 'directional_trading' :
            if direct_count == 0:
                query_data = mongo_db.client[port_name]['mark_to_market_equity'].find().sort('_id',-1)
                asset_type = 'Equity'
                direct_count += 1
            else:
                query_data = mongo_db.client[port_name]['mark_to_market_tfex'].find().sort('_id',-1)
                asset_type = 'TFEX'
        else:
            query_data = mongo_db.client[port_name]['mark_to_market'].find().sort('_id',-1)
            asset_type = 'Equity'
        
        if multi_process:
            data = run_function_multiple_times_multiple_processes(
                        run_backtest,
                        [
                            [doc['_id'], path, asset_type, num_month] for doc in query_data
                        ],
                        initialize_function=lambda: mongo_db.set_client(create_new=True),
                    )
        else:
            if date is None:
                for doc in query_data:
                    run_backtest(doc['_id'], path, asset_type, num_month)
            else:
                print('Running on specified date')
                run_backtest(date, path, asset_type, num_month)
