# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 15:31:07 2019

@author: achen
"""

import numpy as np
import pandas as pd
import telescope as ts
import telescope_metrics as tm
import datetime
from scipy.stats.stats import pearsonr  
from tqdm import tqdm

#--------------Input Parameters---------------
df_2019 = r"Z:\KaiData\Theo\2019\NQ.ESTheo.mpk"
START = datetime.datetime(2019, 6, 17, 0, 0)
END = datetime.datetime(2019, 7, 12, 23, 59)
STIME = datetime.time(8, 0, 0)
ETIME = datetime.time(15, 15, 0)
TIME_FRAME_LENGTH = 5
ROLLING_STEP = '1T' #(has to be a factor of TIME_FRAME_LENGTH)
METRIC = 'TheoPrice'
if (METRIC == 'SMA'):
    SMA_PERIOD = 5;
    
#---------------------------------------------



df_lst = [df_2019]

Tele2 = ts.Telescope()
Tele2.load_dfs(df_lst)
Tele2.choose_range(START, END)
Tele2.resample(ROLLING_STEP)

if (METRIC == 'SMA'):
    tm.apply_all_metrics(Tele2.df, sma_period = SMA_PERIOD)

Tele2.parse_datetime()

Tele2.specific_timeframe(STIME, ETIME)

#print(Tele2.df)
#print(Tele2.grouped)
#print(Tele2.num_of_groups)
#print(Tele2.group_names)
date_list = Tele2.df['date'].unique()
time_list = Tele2.df['time'].unique()

change_direction_match_ratio_list = []
correlatation_list = []

time_index_list = []

#print(Tele2.df['TheoPrice'])

trade_day_list = []
for date in date_list:
    noon_theo_price = ts.select_date_time(Tele2.df, date, datetime.time(12,0,0))['TheoPrice'][0]
    if (not np.isnan(noon_theo_price)):
        trade_day_list.append(date)

column_list = [trade_day_list]
    
for first_time in tqdm(time_list):
    if first_time < (ts.cal_datetime(ETIME) - datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time():
        time_index_list.append(first_time)
        
        first_start = first_time;
        first_end = (ts.cal_datetime(first_time) + datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time();
        
        first_temp_df = ts.select_timeframe(Tele2.df, first_start, first_end)
#        
        single_column = []
        
        first_move_theo = ts.select_date_time(first_temp_df, date, first_end)[METRIC][0] - ts.select_date_time(first_temp_df, date, first_start)[METRIC][0]
        
        first_time_move_list = []
        
        for date in trade_day_list:
            first_move_theo = ts.select_date_time(first_temp_df, date, first_end)[METRIC][0] - ts.select_date_time(first_temp_df, date, first_start)[METRIC][0]
            
        
            if (not np.isnan(first_move_theo)):
                first_time_move_list.append(first_move_theo)
            else:
                first_time_move_list.append(np.nan)
                
        single_column = first_time_move_list
#        print(column_list)
        column_list.append(single_column)
    
#
#        for second_time in time_list[:-(TIME_FRAME_LENGTH+1)]:
#            if (second_time >= first_time):
#                if  (second_time < (ts.cal_datetime(ETIME) - datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time()):
#                    second_start = second_time
#                    second_end = (ts.cal_datetime(second_start) + datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time()
#            
#                    second_temp_df = ts.select_timeframe(Tele2.df, second_start, second_end)
#                
#                    first_time_move_list = []
#                    second_time_move_list = []
#                    
#                    for date in date_list:
#    #            print(first_temp_df[(first_temp_df['date'] == date) & (first_temp_df['time'] == first_end)]['TheoPrice'])
#    #            print(ts.select_date_time(first_temp_df, date, first_end)['TheoPrice'][0])
#                        first_move_theo = ts.select_date_time(first_temp_df, date, first_end)[METRIC][0] - ts.select_date_time(first_temp_df, date, first_start)[METRIC][0]
#                        second_move_theo = ts.select_date_time(second_temp_df, date, second_end)[METRIC][0] - ts.select_date_time(second_temp_df, date, second_start)[METRIC][0]
#                
#                        if ((not np.isnan(first_move_theo)) & (not np.isnan(second_move_theo))):
#                            first_time_move_list.append(first_move_theo)
#                            second_time_move_list.append(second_move_theo)
#            
#    #        print(first_time_move_list)
#    #        print(second_time_move_list)
#    #            count = 0
#    #        for i in range(len(first_time_move_list)):
#    #            if ((first_time_move_list[i] > 0) & (second_time_move_list[i] > 0)) or ((first_time_move_list[i] < 0) & (second_time_move_list[i] < 0)):
#    #                count += 1
#    #        match_ratio = count/len(first_time_move_list)
#    #        change_direction_match_ratio_list.append(match_ratio)
#                    first_mean_abs_move = np.mean(np.absolute(first_time_move_list))
#                    second_mean_abs_move = np.mean(np.absolute(second_time_move_list))
#                    single_column.append(first_mean_abs_move+second_mean_abs_move)
#        
#            else:
#                single_column.append(np.nan) 
#        column_list.append(single_column)

result = pd.DataFrame(column_list[1:], columns=column_list[0])
result['time'] = time_list[:-(TIME_FRAME_LENGTH+1)]
result.set_index('time', inplace=True)
#output_df = pd.DataFrame(list(zip(time_list, correlatation_list, change_direction_match_ratio_list)), columns =['start time', 'correlation with next 5 min', 'change direction match ratio']) 
#output_df.to_csv(r"C:\Users\achen\Desktop\Monocular\5_min_rolling_correlation_direction_match.csv")
#result.to_excel(r"C:\Users\achen\Desktop\Monocular\5_min_rolling_correlation_direction_match.csv")