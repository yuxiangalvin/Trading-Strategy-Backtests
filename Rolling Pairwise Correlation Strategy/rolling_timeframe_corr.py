# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 16:49:43 2019

@author: achen
"""
import numpy as np
import pandas as pd
import telescope as ts
import telescope_metrics as tm
import datetime
from scipy.stats.stats import pearsonr  

df_2019 = r"Z:\KaiData\Theo\2019\YM.ESTheo.mpk"
df_lst = [df_2019]
START = datetime.datetime(2019, 6, 15, 0, 0)
END = datetime.datetime(2019, 7, 12, 23, 59)
STIME = datetime.time(8, 0, 0)
ETIME = datetime.time(15, 15, 0)

Tele2 = ts.Telescope()
Tele2.load_dfs(df_lst)
Tele2.choose_range(START, END)
Tele2.resample('1T')

tm.apply_all_metrics(Tele2.df, sma_period = 10)

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

first_move_abs_sum = []
second_move_abs_sum = []

#print(Tele2.df['TheoPrice'])

for time in time_list:

    if time < (ts.cal_datetime(datetime.time(15, 15, 0)) - datetime.timedelta(minutes = 10)).time():
        print(time)
        first_start = time;
        first_end = (ts.cal_datetime(time) + datetime.timedelta(minutes = 5)).time();
        second_start = (ts.cal_datetime(time) + datetime.timedelta(minutes = 5)).time()
        second_end = (ts.cal_datetime(time) + datetime.timedelta(minutes = 10)).time()
        
        first_temp_df = ts.select_timeframe(Tele2.df, first_start, first_end)
        second_temp_df = ts.select_timeframe(Tele2.df, second_start, second_end)
        
        first_time_move_list = []
        second_time_move_list = []
        
        for date in date_list:
#            print(first_temp_df[(first_temp_df['date'] == date) & (first_temp_df['time'] == first_end)]['TheoPrice'])
#            print(ts.select_date_time(first_temp_df, date, first_end)['TheoPrice'][0])
            first_move_theo = ts.select_date_time(first_temp_df, date, first_end)['TheoPrice'][0] - ts.select_date_time(first_temp_df, date, first_start)['TheoPrice'][0] 
            second_move_theo = ts.select_date_time(second_temp_df, date, second_end)['TheoPrice'][0] - ts.select_date_time(second_temp_df, date, second_start)['TheoPrice'][0]
            
            if ((not np.isnan(first_move_theo)) & (not np.isnan(second_move_theo))):
                first_time_move_list.append(first_move_theo)
                second_time_move_list.append(second_move_theo)
#        print(first_time_move_list)
#        print(second_time_move_list)
        count = 0
        for i in range(len(first_time_move_list)):
            if ((first_time_move_list[i] > 0) & (second_time_move_list[i] > 0)) or ((first_time_move_list[i] < 0) & (second_time_move_list[i] < 0)):
                count += 1
        
        first_move_abs_mean = np.mean(np.absolute(first_time_move_list)) 
        second_move_abs_mean = np.mean(np.absolute(second_time_move_list)) 
        
        first_time_move_list.append(first_move_abs_mean)
        second_time_move_list.append(second_move_abs_mean)
        
        match_ratio = count/len(first_time_move_list)
        change_direction_match_ratio_list.append(match_ratio)
        
        corr = pearsonr(first_time_move_list,second_time_move_list)[0]
        correlatation_list.append(corr)
          
absolute_corr_list = list(np.absolute(correlatation_list))
match_ratio_away_from_half_list = list(np.absolute(np.array(change_direction_match_ratio_list) - 0.5))

output_df = pd.DataFrame(list(zip(time_list, correlatation_list, change_direction_match_ratio_list, absolute_corr_list, match_ratio_away_from_half_list, first_time_move_list, second_time_move_list)), columns =['start time', 'correlation with next 5 min', 'change direction match ratio', 'abs correlation', 'same direction change proportion (distance from 0.5)', 'first time period mean change', 'second time period mean change']) 
output_df.to_excel(r"C:\Users\achen\Desktop\Monocular\5_min_rolling_correlation_direction_match_v2.xlsx")