# -*- coding: utf-8 -*-
"""
Created on Thu Jul 25 11:08:12 2019

@author: achen
"""



import datetime
def bounce_match_strategy(Tele2, trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold, stop_threshold, bounce_stop_threshold, over_limit_length, theo_diff, loss_stop, trade_cost):

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
    trade_day_info = [['Date', 'Enter SMA Ratio', 'Entered Position', 'Exit Time', 'Exit Reason', 'Buy Price', 'Sell Price', 'Net Profit']]


    for trade_day in trade_day_list:
        
        base_timeframe_df = ts.select_date_timeframe(Tele2.df, trade_day, BASE_TIME_START, BASE_TIME_END)
        test_timeframe_df = ts.select_date_timeframe(Tele2.df, trade_day, TEST_TIME_START, TEST_TIME_END)
        
        profit = 0
        
        base_bool_series = (base_timeframe_df['TheoPrice'] > base_timeframe_df['SMA'])
        above_ratio = base_bool_series.sum()/base_bool_series.size
        below_ratio = 1 - above_ratio
        bounce_stop_reach_count = 0
        
        theo_change = base_timeframe_df['TheoPrice'][-1] - base_timeframe_df['TheoPrice'][0]
#        absolute_theo_change = (np.absolute(theo_change))
        max_min_spread = base_timeframe_df['TheoPrice'].max() - base_timeframe_df['TheoPrice'].min()
#        average = base_timeframe_df['TheoPrice'].mean()
#        std_dev = base_timeframe_df['TheoPrice'].std()
        bounce_stop_threshold = max_min_spread
        
        
#        if ((above_ratio >= SMA_THRESHOLD) & (theo_change >= theo_diff)):
        if ((above_ratio >= SMA_THRESHOLD)):
            enter_ratio = above_ratio
            enter_position = 'Long'
            exit_reason = 'Exit Time Reach'
            try:
                buy_price = test_timeframe_df[test_timeframe_df['time'] == test_time_start]['TheoPrice'][0]
            except:
                print(test_timeframe_df['time'])  
                print(test_time_start)
            max_price = buy_price
            for time, price in test_timeframe_df['TheoPrice'].items():
#                if price <= buy_price - STOP_THRESHOLD:
#                    sell_price = price
#                    close_time = time
#                    limit_stop_count += 1
#                    exit_reason = 'Reach Loss Up Stop'
#                    break
                if price > max_price:
                    max_price = price
                    bounce_stop_reach_count = 0
                if price < (buy_price - loss_stop):
                    exit_reason = 'Lose more than 500 dollars'
                    sell_price = price
                    close_time = time
                    break
                if price < max_price - bounce_stop_threshold:
#                    if price < test_timeframe_df['SMA'][time]:
#                            break
                    bounce_stop_reach_count += 1
                    if bounce_stop_reach_count >= over_limit_length:
                        sell_price = price
                        close_time = time
                        bounce_stop_count += 1
                        exit_reason = 'Reach Bounce Stop'
                        break
                sell_price = price
                close_time = time
            
#            check_list.append([sell_price, buy_price, close_time])
            trade_count += 1
            profit = sell_price - buy_price - TRADE_COST
        
#        if ((below_ratio > SMA_THRESHOLD) & (theo_change <= -theo_diff)):
        if ((below_ratio > SMA_THRESHOLD)):
            enter_ratio = below_ratio
            enter_position = 'Short'
            exit_reason = 'Exit Time Reach'
            
            sell_price = test_timeframe_df[test_timeframe_df['time'] == test_time_start]['TheoPrice'][0]
            min_price = sell_price
            for time, price in test_timeframe_df['TheoPrice'].items():
#                if price >= sell_price + STOP_THRESHOLD:
#                    buy_price = price
#                    close_time = time
#                    limit_stop_count +=1
#                    exit_reason = 'Reach Loss Up Stop'
#                    break
                if price < min_price:
                    min_price = price
                    bounce_stop_reach_count = 0
                if price > (sell_price + loss_stop):
                    exit_reason = 'Lose more than 500 dollars'
                    buy_price = price
                    close_time = time
                    break
                if price > min_price + bounce_stop_threshold:
#                    if price > test_timeframe_df['SMA'][time]:
#                        break
                    bounce_stop_reach_count += 1
                    if bounce_stop_reach_count >= over_limit_length:
                        buy_price = price
                        close_time = time
                        bounce_stop_count += 1
                        exit_reason = 'Reach Bounce Stop'
                        break
                buy_price = price
                close_time = time
            trade_count += 1
            profit = sell_price - buy_price - TRADE_COST
            
#            check_list.append([sell_price, buy_price, close_time])
        
        daily_profit_list.append(profit)
        
        if profit == 0:
            day_info = [trade_day,'No Trade','No Trade','No Trade','No Trade','No Trade','No Trade',profit]
        else:
            day_info = [trade_day, enter_ratio, enter_position, close_time.time(), exit_reason, buy_price, sell_price, profit]
        trade_day_info.append(day_info)
        
            
            
            
    time_stop_count = trade_count - limit_stop_count - bounce_stop_count
    return np.array(daily_profit_list), trade_count, limit_stop_count, bounce_stop_count, time_stop_count, trade_day_info, bounce_stop_threshold
            
            

 
    
#    result = pd.DataFrame(column_list[1:], columns=column_list[0])
#    result['time'] = time_list[:-ignore_col_num]
#    result.set_index('time', inplace=True)

#    result.to_excel(output_excel_path)
#    return result