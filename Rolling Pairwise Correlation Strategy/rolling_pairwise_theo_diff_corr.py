# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 16:26:07 2019

@author: achen
"""
import datetime
def rolling_pairwise_theo_diff_corr(data_mpk_file_path, start_date, end_date, start_time = datetime.time(8, 0, 0), end_time = datetime.time(15, 15, 0), time_frame_length = 5, rolling_step = '1T', output_excel_path= r"C:\Users\achen\Desktop\Monocular\rolling_pairwise_theo_diff_corr.xlsx"):

    import numpy as np
    import pandas as pd
    import telescope as ts
    import telescope_metrics as tm
    import datetime
    from scipy.stats.stats import pearsonr  
    from tqdm import tqdm
    
    #--------------Input Parameters---------------
    df_2019 = data_mpk_file_path
    START = start_date
    END = end_date
    STIME = start_time
    ETIME = end_time
    TIME_FRAME_LENGTH = time_frame_length
    ROLLING_STEP = rolling_step #(has to be a factor of TIME_FRAME_LENGTH)
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
    
    
    
    df_lst = [df_2019]
    
    Tele2 = ts.Telescope()
    Tele2.load_dfs(df_lst)
    Tele2.choose_range(START, END)
    Tele2.resample(ROLLING_STEP)
    
    tm.apply_all_metrics(Tele2.df, sma_period = 10)
    
    Tele2.parse_datetime()
    
    Tele2.specific_timeframe(STIME, ETIME)
    
    #print(Tele2.df)
    #print(Tele2.grouped)
    #print(Tele2.num_of_groups)
    #print(Tele2.group_names)
    date_list = Tele2.df['date'].unique()
    time_list = Tele2.df['time'].unique()
    
    ignore_col_num = int(TIME_FRAME_LENGTH/int(ROLLING_STEP[0])+1)
#    change_direction_match_ratio_list = []
#    correlatation_list = []
    print(date_list)
    print(time_list[:-ignore_col_num])
    #print(Tele2.df['TheoPrice'])
    column_list = [time_list[:-ignore_col_num]]
    for first_time in tqdm(time_list[:-ignore_col_num]):
        if first_time < (ts.cal_datetime(ETIME) - datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time():
            first_start = first_time;
            first_end = (ts.cal_datetime(first_time) + datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time();
            print(first_start)
            
            first_temp_df = ts.select_timeframe(Tele2.df, first_start, first_end)
            
            single_column = []
            
            for second_time in time_list[:-ignore_col_num]:
                if (second_time >= (ts.cal_datetime(first_time)  + datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time()) & (second_time < (ts.cal_datetime(ETIME) - datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time()):
                    second_start = second_time
                    second_end = (ts.cal_datetime(second_start) + datetime.timedelta(minutes = TIME_FRAME_LENGTH)).time()
            
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
    #            count = 0
    #        for i in range(len(first_time_move_list)):
    #            if ((first_time_move_list[i] > 0) & (second_time_move_list[i] > 0)) or ((first_time_move_list[i] < 0) & (second_time_move_list[i] < 0)):
    #                count += 1
    #        match_ratio = count/len(first_time_move_list)
    #        change_direction_match_ratio_list.append(match_ratio)
                    
                    corr = pearsonr(first_time_move_list,second_time_move_list)[0]
                    single_column.append(corr)

    
    
            
                else:
                    single_column.append(np.nan) 
            print(single_column)
            column_list.append(single_column)
    
    result = pd.DataFrame(column_list[1:], columns=column_list[0])
    result['time'] = time_list[:-ignore_col_num]
    result.set_index('time', inplace=True)
    #output_df = pd.DataFrame(list(zip(time_list, correlatation_list, change_direction_match_ratio_list)), columns =['start time', 'correlation with next 5 min', 'change direction match ratio']) 
    #output_df.to_csv(r"C:\Users\achen\Desktop\Monocular\5_min_rolling_correlation_direction_match.csv")
    result.to_excel(output_excel_path)
    return result