# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 10:42:35 2019

@author: achen
"""

import pandas as pd
import telescope as ts
import telescope_metrics as tm
import sma_theodiff_strategy as sts
import datetime
import numpy as np
from tqdm import tqdm

#dmb.dir_match_backtest(r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk", datetime.datetime(2019, 7, 16, 0, 0), datetime.datetime(2019, 7, 17, 23, 59))
START = datetime.datetime(2019, 1, 1, 0, 0)
END = datetime.datetime(2019, 7, 22, 23, 59)

Tele2 = ts.Telescope()
Tele2.load_dfs([r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk"])
Tele2.choose_range(START, END)

Tele2.parse_datetime()

date_list = Tele2.df['date'].unique()

trade_day_list = []
not_trade_day_list = []
for date in date_list:
    if ts.select_date_time(Tele2.df, date, datetime.time(8,30,0))['TheoPrice'].empty:
        not_trade_day_list.append(date)
    else:
        trade_day_list.append(date)

Tele2.df = Tele2.df.resample('5S', fill_method = 'bfill')

Tele2.parse_datetime()

tm.apply_all_metrics(Tele2.df, sma_period = 600)

Tele2.clip_time(datetime.time(8,30,0), datetime.time(9,0,0))




base_time_start = datetime.time(8,30,0)

base_time_end_list = [datetime.time(8,40,0)]

test_length_list = [300]

sma_list = [0.6,0.7,0.8]

stop = 0

over_limit_time_list = [1,60, 120,180]

bounce_stop_list = [80, 120, 160]

column_list = [['Profit', 'Trade Number', ' Time Stop Count', 'Limite Stop Count', 'Bounce Stop Count', 'Base Start', 'Base End', 'Test Start', 'Test End', 'SMA Side Ratio Threshold', 'Bounce Stop Threshold', 'Over Limit Time', 'Trading Cost']]
for base_time_end in base_time_end_list:
    test_time_start = base_time_end
    for test_length in test_length_list:
        test_time_end = (ts.cal_datetime(test_time_start) + datetime.timedelta(minutes = test_length)).time()
        for sma in sma_list:
            for bounce_stop in bounce_stop_list:
                for over_limit_time in tqdm(over_limit_time_list):
           
                    [daily_profit_result, trade_count, limit_stop_count, bounce_stop_count, time_stop_count, loss_record] = sts.sma_theodiff_strategy(Tele2, trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold = sma, stop_threshold = bounce_stop, bounce_stop_threshold = bounce_stop, over_limit_length = over_limit_time, trade_cost = 30)
            
                    total_profit = np.sum(daily_profit_result[~np.isnan(daily_profit_result)])
                
                    single_column = [total_profit, trade_count, time_stop_count, limit_stop_count, bounce_stop_count, base_time_start, base_time_end, test_time_start, test_time_end, sma, bounce_stop, over_limit_time, 30]
                    column_list.append(single_column)

#loss_df = pd.DataFrame(loss_record[1:], columns=loss_record[0])"C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\RTY Morning Trend PL\\RTY_20190401_20190717_PARAMETER_TEST3_PL.xlsx"
result2_df = pd.DataFrame(column_list[1:], columns=column_list[0])
#result_df.to_excel()
