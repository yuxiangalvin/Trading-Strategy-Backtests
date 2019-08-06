# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 13:28:34 2019

@author: achen
"""

import pandas as pd
import datetime
import numpy as np
from tqdm import tqdm

def rolling_backtest3(Tele2, end_datetime, trade_day_list):
    import telescope as ts
#    import telescope_metrics as tm
    import strategy_backtest3_loss as sbtest3l

    #dmb.dir_match_backtest(r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk", datetime.datetime(2019, 7, 16, 0, 0), datetime.datetime(2019, 7, 17, 23, 59))
    START = end_datetime - datetime.timedelta(days=13)
    END = end_datetime
    
    print(START)
    print(END)
    
    
    
#    Tele2 = ts.Telescope()
#    Tele2.load_dfs([r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk"])
#    Tele2.df = Tele2.df.resample('5S', fill_method = 'bfill')
#    tm.apply_all_metrics(Tele2.df, sma_period = 600)
#    Tele2.clip_time(datetime.time(8,30,0), datetime.time(12,0,0))
    
    chosen_Tele = ts.Telescope(df = Tele2.choose_date(START, END))
    
#    date_list = chosen_Tele.df['date'].unique()
#    
#    trade_day_list = []
#    not_trade_day_list = []
#    for date in date_list:
#        if ts.select_date_time(chosen_Tele.df, date, datetime.time(8,30,0))['TheoPrice'].empty:
#            not_trade_day_list.append(date)
#        else:
#            trade_day_list.append(date)
    test_day = trade_day_list[-1]
    trade_day_list = trade_day_list[:-1]
#    
#    Tele2.df = Tele2.df.resample('5S', fill_method = 'bfill')
#    
#    Tele2.parse_datetime()
    
#    tm.apply_all_metrics(Tele2.df, sma_period = 600)
#    
#    Tele2.clip_time(datetime.time(8,30,0), datetime.time(12,0,0))
    
    
    
    
    base_time_start = datetime.time(8,30,0)
    
    base_time_end_list = [datetime.time(8,31,0), datetime.time(8,33,0), datetime.time(8,35,0), datetime.time(8,40,0), datetime.time(8,45,0)]
    
    test_length_list = [360]
    
    sma_list = [0.5,0.6,0.7,0.8,0.9]
    
    stop_list = [0]
    
    bounce_stop_list = [80, 100, 120, 140, 160]
    
    over_limit_time_list = [1,30, 60, 90, 120, 150, 180]
    
    column_list = [['Profit', 'Trade Number', ' Time Stop Count', 'Limite Stop Count', 'Bounce Stop Count', 'Base Start', 'Base End', 'Test Start', 'Test End', 'SMA Side Ratio Threshold', 'Stop Threshold', 'Bounce Stop Threshold','Over Limit Time','Trading Cost']]
    for base_time_end in base_time_end_list:
        test_time_start = base_time_end
        for test_length in test_length_list:
            test_time_end = (ts.cal_datetime(test_time_start) + datetime.timedelta(minutes = test_length)).time()
            for sma in sma_list:
                for stop in stop_list:
                    for bounce_stop in bounce_stop_list:
                        for over_limit_time in over_limit_time_list:
                            absolute_stop = bounce_stop - stop 
                            [daily_profit_result, trade_count, limit_stop_count, bounce_stop_count, time_stop_count, loss_record] = sbtest3l.strategy_backtest3_loss(chosen_Tele, trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold = sma, stop_threshold = absolute_stop, bounce_stop_threshold = bounce_stop, over_limit_length = over_limit_time, trade_cost = 30)
                        
                        
                            total_profit = np.sum(daily_profit_result[~np.isnan(daily_profit_result)])
                        
                            single_column = [total_profit, trade_count, time_stop_count, limit_stop_count, bounce_stop_count, base_time_start, base_time_end, test_time_start, test_time_end, sma, stop, bounce_stop, over_limit_time, 30]
                            column_list.append(single_column)
    
    result_df = pd.DataFrame(column_list[1:], columns=column_list[0])
    
    parameter_series = result_df.loc[result_df['Profit'].idxmax(),:]
    print(result_df['Profit'].max())
    
    base_time_start = parameter_series['Base Start']
    base_time_end = parameter_series['Base End']
    test_time_start = parameter_series['Test Start']
    test_time_end = parameter_series['Test End']
    sma = parameter_series['SMA Side Ratio Threshold']
    stop = parameter_series['Stop Threshold']
    bounce_stop = parameter_series['Bounce Stop Threshold']
    trading_cost = parameter_series['Trading Cost']
    over_limit_time = parameter_series['Over Limit Time']
    
    [test_profit_result, trade_count, limit_stop_count, bounce_stop_count, time_stop_count, loss_record] = sbtest3l.strategy_backtest3_loss(chosen_Tele, [test_day], base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold = sma, stop_threshold = absolute_stop, bounce_stop_threshold = bounce_stop, over_limit_length = over_limit_time, trade_cost = trading_cost)
    return base_time_start, base_time_end, sma, bounce_stop, over_limit_time, result_df['Profit'].max(), test_profit_result[0]
#result_df.to_excel("C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\RTY Morning Trend PL\\RTY_20190401_20190717_PARAMETER_TEST3_PL.xlsx")
