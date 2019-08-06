# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 12:45:21 2019

@author: achen
"""
def dir_match_backtest(df_2019, start_date, end_date):    
    
    import numpy as np
    import pandas as pd
    import telescope as ts
    import telescope_metrics as tm
    import datetime
    from tqdm import tqdm
    
    import rolling_pairwise_theo_diff_dir_match as rptddm
    import rolling_pairwise_theo_diff_sum_profit as rptdsp
    from datetime import timedelta
    
    #--------------Input Parameters---------------
    df_2019 = r"Z:\KaiData\Theo\2019\NQ.ESTheo.mpk"
    START = start_date 
    END = end_date
    
    df_lst = [df_2019]
    
    Tele2 = ts.Telescope()
    Tele2.load_dfs(df_lst)
    Tele2.choose_range(START, END)
    Tele2.parse_datetime()
    #print(Tele2.df)
    #print(Tele2.grouped)
    #print(Tele2.num_of_groups)
    #print(Tele2.group_names)
    date_list = Tele2.df['date'].unique()
    
    #print(Tele2.df['TheoPrice'])
    
    trade_day_list = []
    for date in date_list:
        if ts.select_date_time(Tele2.df, date, datetime.time(12,0,0))['TheoPrice'].empty:
            continue
        else:
            trade_day_list.append(date)

    
    
    
    
    time_length = 60
    #corr_threshold_list = [0.5,0.6,0.7,0.8,0.9]
    #up_threshold_list = [50, 75, 100]
    #down_threshold_list = [50, 75, 100]
    corr_threshold_list = [3]
    up_threshold_list = [60]
    down_threshold_list = [60]
    trading_cost = 30

    
    count = 0
    for corr_thre in corr_threshold_list:
        for up_thre in up_threshold_list:
            for date in tqdm(trade_day_list):   
                print(date)
                
                down_thre = up_thre
                
                if date.weekday() == 4:
                    corr_start_datetime = date - timedelta(days = 4)
                else:
                    corr_start_datetime = date - timedelta(days = 6)
    
                corr_end_datetime = date - timedelta(minutes = 1)
                
                test_start_datetime = date
                test_end_datetime = date + timedelta(days = 1) - timedelta(minutes = 1)
                
                output_file_path_corr = "C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\Direction Match\\NQ_" + str(time_length) + "MIN_" + corr_start_datetime.strftime("%Y_%m_%d") + "_" + corr_end_datetime.strftime("%Y_%m_%d") + "_ROLL5_PAIR_DIRMATCH_auto.xlsx"
                output_file_path_backtest = "C:\\Users\\achen\Desktop\\Monocular\\Automatic Backtest\\Dirmatch PL\\NQ_" + str(time_length) + "MIN_" + test_start_datetime.strftime("%Y_%m_%d")  + "_ROLL5_PAIR_DIRMATCH_PROFIT_U" + str(up_thre) +"_D" + str(down_thre) + "_T" + str(trading_cost) + "_auto.xlsx"
                
                corr_df = rptddm.rolling_pairwise_theo_diff_dir_match("Z:\\KaiData\\Theo\\2019\\NQ.ESTheo.mpk", corr_start_datetime, corr_end_datetime, datetime.time(8, 0, 0), datetime.time(15, 15, 0), time_frame_length = time_length, rolling_step = '5T', output_excel_path = output_file_path_corr )
                if corr_df is None:
                    continue
                else:
                    count += 1
                pl_df = rptdsp.rolling_pairwise_theo_diff_sum_profit(corr_df, "Z:\\KaiData\\Theo\\2019\\NQ.ESTheo.mpk", test_start_datetime, test_end_datetime, datetime.time(8, 0, 0), datetime.time(15, 15, 0), time_frame_length = time_length, rolling_step = '5T', corr_threshold = corr_thre, up_threshold = up_thre, down_threshold = down_thre, trade_cost = trading_cost, output_excel_path = output_file_path_backtest)
                
                if count == 1:
                    result_df = pl_df
                else:
                    result_df = result_df.add(pl_df)