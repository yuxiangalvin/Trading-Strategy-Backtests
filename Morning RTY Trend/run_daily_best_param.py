# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 14:09:41 2019

@author: achen
"""

import pandas as pd
import telescope as ts
import telescope_metrics as tm
import daily_best_feature_record_test as dbfrt

import datetime
import numpy as np
from tqdm import tqdm

#dmb.dir_match_backtest(r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk", datetime.datetime(2019, 7, 16, 0, 0), datetime.datetime(2019, 7, 17, 23, 59))
START = datetime.datetime(2019, 1, 1, 0, 0)
END = datetime.datetime(2019, 7, 25, 23, 59)

Tele2 = ts.Telescope()
Tele2.load_dfs([r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk"])
Tele2.choose_range(START, END)

Tele2.parse_datetime()

criteria_df = Tele2.df.copy()
criteria_df = ts.tele_resample_new(criteria_df, '1T')
criteria_df = tm.theochange(criteria_df, period=10)

criteria_df = criteria_df[criteria_df['Theo Change'] != 0]

criteria_df['date'] = criteria_df.index.date
criteria_df['time'] = criteria_df.index.time
criteria_df=criteria_df[criteria_df['time'] == datetime.time(8,39,0)]

criteria_df.index = criteria_df['date']
criteria_df['Abs Diff'] = np.absolute(criteria_df['Theo Change'])

criteria_df['Log Volume RTY'] = np.log(criteria_df['Volume_x'])
criteria_df['Log Volume ES'] = np.log(criteria_df['Volume_y'])
criteria_df['Log Abs Diff'] = np.log(criteria_df['Abs Diff'])

criteria_df['Log Vol ES RTY ratio'] = criteria_df['Log Volume ES'] / criteria_df['Log Volume RTY']
criteria_df['Log Vol RTY Diff ratio'] = criteria_df['Log Volume RTY'] / np.log(criteria_df['Abs Diff'])

criteria_df['(Log Vol RTY Diff ratio) over (Log Vol ES RTY ratio)'] = criteria_df['Log Vol RTY Diff ratio']  /  criteria_df['Log Vol ES RTY ratio'] 

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

Tele2.clip_time(datetime.time(8,30,0), datetime.time(15,10,0))




base_time_start = datetime.time(8,30,0)

base_time_end_list = [datetime.time(8,40,0)]

test_length_list = [350]

sma_list = [0.5]

stop = 0

over_limit_time_list = [100]

bounce_stop_list = [80]

loss_stop = 500

theo_diff = 100

column_list = [['Date','Max Profit', 'Above Ratio', 'Below Ratio','Real Time Theo Change', 'Abs Real Time Theo Change', 'Max min Spread','Average Theo', 'Std of Theo', 'Exit Reason', 'Base Start', 'Base End', 'Test Start', 'Test End', 'SMA Side Ratio Threshold', 'Bounce Stop Threshold', 'Over Limit Time', 'Trading Cost', 'Test time start price', 'Test time end price', 'Test Price Move', 'Abs Test Price Move']]
for test_trade_day in trade_day_list:
    test_trade_day_list = [test_trade_day]
    max_profit = -10000
    for base_time_end in base_time_end_list:
        test_time_start = base_time_end
        for test_length in test_length_list:
            test_time_end = (ts.cal_datetime(test_time_start) + datetime.timedelta(minutes = test_length)).time()
            for sma in sma_list:
                for bounce_stop in bounce_stop_list:
                    for over_limit_time in tqdm(over_limit_time_list):
               
                        [daily_profit_result, trade_count, limit_stop_count, bounce_stop_count, time_stop_count, loss_record, above_ratio, below_ratio, theo_change, absolute_theo_change, max_min_spread, average, std_dev, exit_reason, test_start_price, test_end_price, test_price_move, abs_test_price_move] = dbfrt.daily_best_feature_record_test(Tele2, test_trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold = sma, stop_threshold = bounce_stop, bounce_stop_threshold = bounce_stop, over_limit_length = over_limit_time, loss_stop = loss_stop, trade_cost = 30)
                
                        if daily_profit_result[0] > max_profit:
                            max_profit = daily_profit_result[0]
                            single_column = [test_trade_day, daily_profit_result[0], above_ratio, below_ratio, theo_change, absolute_theo_change, max_min_spread, average, std_dev, exit_reason, base_time_start, base_time_end, test_time_start, test_time_end, sma, bounce_stop, over_limit_time, 30, test_start_price, test_end_price, test_price_move, abs_test_price_move]         
    column_list.append(single_column)
#loss_df = pd.DataFrame(loss_record[1:], columns=loss_record[0])

result_df = pd.DataFrame(column_list[1:], columns=column_list[0])
result_df.index = result_df['Date']
del result_df['Date']
final_result_df = result_df.join(criteria_df, how = 'left')
del final_result_df['date']
del final_result_df['time']
final_result_df.to_excel("C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\RTY Delta Side\\RTY_20190101_20190725_ALL_FEATURES.xlsx")
