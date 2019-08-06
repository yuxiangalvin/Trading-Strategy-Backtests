# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 16:11:30 2019

@author: achen
"""


import datetime
def consecutive_5_backtest(Tele2, trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold, stop_threshold, bounce_stop_threshold, trade_cost):

    import numpy as np
    import pandas as pd
    import telescope as ts
    import telescope_metrics as tm
    import datetime
    from scipy.stats.stats import pearsonr  
    from tqdm import tqdm
    
    #--------------Input Parameters---------------
#    data_mpk_file_path, start_date, end_date
#    df_2019 = data_mpk_file_path
#    START = start_date
#    END = end_date
#    STIME = base_time_start
#    ETIME = test_time_end
#    TIME_FRAME_LENGTH = time_frame_length
#    ROLLING_STEP = rolling_step #(has to be a factor of TIME_FRAME_LENGTH)
    BASE_TIME_START = base_time_start
    BASE_TIME_END = base_time_end
    TEST_TIME_START = test_time_start
    TEST_TIME_END = test_time_end
    SMA_THRESHOLD = sma_threshold
    STOP_THRESHOLD = stop_threshold
    TRADE_COST = trade_cost
    #---------------------------------------------
    
    #--------------Input Parameters---------------
    #df_2019 = r"Z:\KaiData\Theo\2019\YM.ESTheo.mpk"
    #START = datetime.datetime(2019, 7, 8, 0, 0)
    #END = datetime.datetime(2019, 7, 12, 23, 59)
    #STIME = datetime.time(8, 0, 0)
    #ETIME = datetime.time(15, 15, 0)
    #TIME_FRAME_LENGTH = 5
    #ROLLING_STEP = '1T' #(has to be a factor of TIME_FRAME_LENGTH)
    #---------------------------------------------
    
    #print(Tele2.df)
    #print(Tele2.grouped)
    #print(Tele2.num_of_groups)
    #print(Tele2.group_names)
#    time_list = Tele2.df['time'].unique()
    
    daily_profit_list = []
    limit_stop_count = 0
    bounce_stop_count = 0
    trade_count = 0
    
#    check_list = []
    
    

    for trade_day in trade_day_list:
        
        base_timeframe_df = ts.select_date_timeframe(Tele2.df, trade_day, BASE_TIME_START, BASE_TIME_END)
        test_timeframe_df = ts.select_date_timeframe(Tele2.df, trade_day, TEST_TIME_START, TEST_TIME_END)
        
        profit = 0
        
        
        
#        base_bool_series = (base_timeframe_df['TheoPrice'] > base_timeframe_df['SMA'])
#        above_ratio = base_bool_series.sum()/base_bool_series.size
#        below_ratio = 1 - above_ratio
#        if above_ratio >= SMA_THRESHOLD:
#            try:
#                buy_price = test_timeframe_df[test_timeframe_df['time'] == test_time_start]['TheoPrice'][0]
#            except:
#                print(test_timeframe_df['time'])  
#                print(test_time_start)
#            max_price = buy_price
#            for time, price in test_timeframe_df['TheoPrice'].items():
#                if price <= buy_price - STOP_THRESHOLD:
#                    sell_price = price
#                    limit_stop_count += 1
#                    break
#                if price > max_price:
#                    max_price = price
#                if price < max_price - bounce_stop_threshold:
#                    sell_price = price
#                    bounce_stop_count += 1
#                    break
#                sell_price = price
##                close_time = time
#            
##            check_list.append([sell_price, buy_price, close_time])
#            trade_count += 1
#            profit = sell_price - buy_price - TRADE_COST
#        
#        if below_ratio > SMA_THRESHOLD:
#            sell_price = test_timeframe_df[test_timeframe_df['time'] == test_time_start]['TheoPrice'][0]
#            min_price = sell_price
#            for time, price in test_timeframe_df['TheoPrice'].items():
#                if price >= sell_price + STOP_THRESHOLD:
#                    buy_price = price
#                    limit_stop_count +=1
#                    break
#                if price < min_price:
#                    min_price = price
#                if price > min_price + bounce_stop_threshold:
#                    buy_price = price
#                    bounce_stop_count += 1
#                    break
#                buy_price = price
##                close_time = time
#            trade_count += 1
#            profit = sell_price - buy_price - TRADE_COST
            
#            check_list.append([sell_price, buy_price, close_time])
        
        daily_profit_list.append(profit)
    time_stop_count = trade_count - limit_stop_count - bounce_stop_count
    return np.array(daily_profit_list), trade_count, limit_stop_count, bounce_stop_count, time_stop_count
            
            

 
    
#    result = pd.DataFrame(column_list[1:], columns=column_list[0])
#    result['time'] = time_list[:-ignore_col_num]
#    result.set_index('time', inplace=True)

#    result.to_excel(output_excel_path)
#    return result