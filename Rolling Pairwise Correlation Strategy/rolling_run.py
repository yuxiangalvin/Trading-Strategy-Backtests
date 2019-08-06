# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 18:20:09 2019

@author: achen
"""

import pandas as pd
import telescope as ts
import telescope_metrics as tm
import strategy_backtest2_loss as sbtest2l
import datetime
import numpy as np
from tqdm import tqdm

import run_past_4_day_param as rp4dp

Tele2 = ts.Telescope()
Tele2.load_dfs([r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk"])

START = datetime.datetime(2019, 4, 1, 0, 0)
END = datetime.datetime(2019, 7, 22, 23, 59)


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

test_day_list = trade_day_list[4:]
print(trade_day_list)

Tele2.df = Tele2.df.resample('5S', fill_method = 'bfill')
tm.apply_all_metrics(Tele2.df, sma_period = 600)

Tele2.parse_datetime()

Tele2.clip_time(datetime.time(8,30,0), datetime.time(12,0,0))


date_list = []
profit_list = []
max_past_profit_list = []

column_list = [['Test Date', 'Max Profit in past 4 days', 'Test Day Profit', 'Base Time End', 'SMA Side Ratio Threshold', 'Bounce Stop']]

for i in tqdm(range(4,len(trade_day_list))):
    test_day = trade_day_list[i]
    [base_time_start, base_time_end, sma, bounce_stop, max_past_profit, profit] = rp4dp.rolling_backtest(Tele2, test_day, trade_day_list[(i-4):(i+1)])
    single_column = [test_day, profit, max_past_profit, base_time_end, sma, bounce_stop]
    column_list.append(single_column)
    
result_df = pd.DataFrame(column_list[1:], columns=column_list[0])



        
        

