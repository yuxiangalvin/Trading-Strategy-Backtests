# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 13:44:35 2019

@author: achen
"""

import pandas as pd
import telescope as ts
import telescope_metrics as tm
import strategy_backtest3_loss as sbtest3l
import datetime
import numpy as np
from tqdm import tqdm

import run_past_4_day_param3 as rp4dp3

Tele2 = ts.Telescope()
Tele2.load_dfs([r"Z:\KaiData\Theo\2018\RTY.ESTheo.mpk"])

START = datetime.datetime(2018, 1, 1, 0, 0)
END = datetime.datetime(2018, 12, 31, 23, 59)


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

test_day_list = trade_day_list[9:]
print(trade_day_list)

Tele2.df = Tele2.df.resample('5S', fill_method = 'bfill')
tm.apply_all_metrics(Tele2.df, sma_period = 24)

Tele2.parse_datetime()

Tele2.clip_time(datetime.time(8,30,0), datetime.time(12,0,0))


date_list = []
profit_list = []
max_past_profit_list = []

column_list = [['Test Date', 'Test Day Profit','Max Profit in past 4 days',  'Base Time End', 'SMA Side Ratio Threshold', 'Bounce Stop', 'Over Time Limit']]

for i in tqdm(range(9,len(trade_day_list))):
    test_day = trade_day_list[i]
    [base_time_start, base_time_end, sma, bounce_stop, over_limit_time, max_past_profit, profit] = rp4dp3.rolling_backtest3(Tele2, test_day, trade_day_list[(i-9):(i+1)])
    single_column = [test_day, profit, max_past_profit, base_time_end, sma, bounce_stop, over_limit_time]
    column_list.append(single_column)
    
result_df = pd.DataFrame(column_list[1:], columns=column_list[0])
result_df.to_excel("C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\RTY Morning Trend PL\\RTY_20180101_20181231_ROLLING9_SMA2_PARAM.xlsx")



        
        

