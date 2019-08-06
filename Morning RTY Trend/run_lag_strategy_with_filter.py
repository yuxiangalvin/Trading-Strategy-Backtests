# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 13:06:23 2019

@author: achen
"""

import pandas as pd
import telescope as ts
import telescope_metrics as tm
import strategy_backtest3_loss as sbtest3l
import datetime
import numpy as np
from tqdm import tqdm

START = datetime.datetime(2019, 1, 1, 0, 0)
END = datetime.datetime(2019, 6, 30, 23, 59)

Tele2 = ts.Telescope()
Tele2.load_dfs([r"Z:\KaiData\Theo\2018\RTY.ESTheo.mpk"])
Tele2.choose_range(START, END)

Tele2.parse_datetime()

#criteria_df = Tele2.df.copy()
#criteria_df = ts.tele_resample_new(criteria_df, '1T')
#criteria_df = tm.theochange(criteria_df, period=10)
#criteria_df['date'] = criteria_df.index.date
#criteria_df['time'] = criteria_df.index.time
#criteria_df=criteria_df[criteria_df['time'] == datetime.time(8,40,0)]
#
#criteria_df.index = criteria_df['date']
#criteria_df['Abs Diff'] = np.absolute(criteria_df['Theo Change'])
#criteria_df = criteria_df[criteria_df['Abs Diff'] >= 140]
#
#criteria_df['Log Volume RTY'] = np.log(criteria_df['Volume_x'])
#criteria_df['Log Volume ES'] = np.log(criteria_df['Volume_y'])
#criteria_df['Log Abs Diff'] = np.log(criteria_df['Abs Diff'])
#
#criteria_df['Log Vol ES RTY ratio'] = criteria_df['Log Volume ES'] / criteria_df['Log Volume RTY']
#criteria_df['Log Vol RTY Diff ratio'] = criteria_df['Log Volume RTY'] / np.log(criteria_df['Abs Diff'])
#
#criteria_df = criteria_df[(criteria_df['Log Vol RTY Diff ratio'] <= 1.3) & (criteria_df['Log Vol RTY Diff ratio'] >= 0.9)]
#
##
#date_list = criteria_df['date'].unique()
date_list = Tele2.df['date'].unique()
#
#
#
#
trade_day_list = []
not_trade_day_list = []

for date in date_list:
    if ts.select_date_time(Tele2.df, date, datetime.time(8,30,0))['TheoPrice'].empty:
        not_trade_day_list.append(date)
    else:
        trade_day_list.append(date)
        
Tele2.df = Tele2.df.resample('5S', fill_method = 'bfill')
#
Tele2.parse_datetime()
#
tm.apply_all_metrics(Tele2.df, sma_period = 600)

Tele2.clip_time(datetime.time(8,30,0), datetime.time(14,0,0))


base_time_start = datetime.time(8,30,0)

base_time_end_list = [datetime.time(8,40,0)]

test_length_list = [300]

sma_list = [0.6]

stop = 0

over_limit_time_list = [1,60,90,120,150,180]

bounce_stop_list = [80, 120, 160, 200]

column_list = [['Profit', 'Trade Number', ' Time Stop Count', 'Limite Stop Count', 'Bounce Stop Count', 'Base Start', 'Base End', 'Test Start', 'Test End', 'SMA Side Ratio Threshold', 'Bounce Stop Threshold', 'Over Limit Time', 'Trading Cost']]
for base_time_end in base_time_end_list:
    test_time_start = base_time_end
    for test_length in test_length_list:
        test_time_end = (ts.cal_datetime(test_time_start) + datetime.timedelta(minutes = test_length)).time()
        for sma in sma_list:
            for bounce_stop in bounce_stop_list:
                for over_limit_time in tqdm(over_limit_time_list):
           
                    [daily_profit_result, trade_count, limit_stop_count, bounce_stop_count, time_stop_count, loss_record] = sbtest3l.strategy_backtest3_loss(Tele2, trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold = sma, stop_threshold = bounce_stop, bounce_stop_threshold = bounce_stop, over_limit_length = over_limit_time, trade_cost = 30)
            
                    total_profit = np.sum(daily_profit_result[~np.isnan(daily_profit_result)])
                
                    single_column = [total_profit, trade_count, time_stop_count, limit_stop_count, bounce_stop_count, base_time_start, base_time_end, test_time_start, test_time_end, sma, bounce_stop, over_limit_time, 30]
                    column_list.append(single_column)

#loss_df = pd.DataFrame(loss_record[1:], columns=loss_record[0])
result_df = pd.DataFrame(column_list[1:], columns=column_list[0])
result_df.to_excel("C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\RTY Morning Trend PL\\RTY_20180101_20181231_VARIOUS_PARAM_with_FILTER.xlsx")
