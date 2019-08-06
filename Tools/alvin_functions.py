# -*- coding: utf-8 -*-
"""
Created on Thu Jul 25 15:35:22 2019

@author: achen
"""
import pandas as pd
import telescope as ts
import telescope_metrics as tm
import bounce_match_strategy as bms
import daily_best_feature_record_test as dbfrt
import datetime
import numpy as np
from tqdm import tqdm

def load_data():
    START = datetime.datetime(2018, 1, 1, 0, 0)
    END = datetime.datetime(2018, 12, 31, 23, 59)
    
    Tele2 = ts.Telescope()
    Tele2.load_dfs([r"Z:\KaiData\Theo\2018\RTY.ESTheo.mpk"])
    Tele2.choose_range(START, END)
    
    Tele2.parse_datetime()
    return Tele2

def select_timeframe(df, start_time, end_time):
    return df.loc[start_time : end_time]
def select_date(df, date):
    return df[df['date'] == date]
def select_date_time(df,date,time):
    return df[(df['date'] == date) & (df['time'] == time)]

def cal_datetime(time):
    return datetime.datetime.combine(datetime.datetime.today(),time)
def select_date_timeframe(df, date, start_time, end_time):
    return df[(df['date'] == date) & (df['time'] >= start_time) & (df['time'] <= end_time)]

def tele_resample_new(df, resample_freq):
    df['Total_Volume'] = df['Volume_x'] + df['Volume_y']
    df['Volume*Theo'] = df['Total_Volume'] * df['TheoPrice']
    df = df.resample(resample_freq).sum()
    df['VWAP'] = df['Volume*Theo'] / df['Total_Volume']
    df = df[['Volume_x', 'Volume_y', 'Total_Volume', 'VWAP']]
    df = df.rename(columns = {'VWAP':'TheoPrice'})
    return df
            
#def resample_calc_metrics(Tele2):
#    Tele2.df = Tele2.df.resample('2T', closed = 'right', label = 'right', fill_method = 'bfill')
#    #
#    Tele2.parse_datetime()
#    #
##    tm.apply_all_metrics(Tele2.df, sma_period = 2)
#    
#    Tele2.clip_time(datetime.time(8,30,0), datetime.time(14,0,0))
#    
#    return Tele2


def get_criteria(Tele2):
    criteria_df = Tele2.df.copy()
    criteria_df = ts.tele_resample_new(criteria_df, '1T')
    criteria_df = tm.theochange(criteria_df, period=10)
    criteria_df['date'] = criteria_df.index.date
    criteria_df['time'] = criteria_df.index.time
    criteria_df=criteria_df[criteria_df['time'] == datetime.time(8,40,0)]
    
    criteria_df.index = criteria_df['date']
    criteria_df['Abs Diff'] = np.absolute(criteria_df['Theo Change'])
    criteria_df = criteria_df[criteria_df['Abs Diff'] >= 140]
    
    criteria_df['Log Volume RTY'] = np.log(criteria_df['Volume_x'])
    criteria_df['Log Volume ES'] = np.log(criteria_df['Volume_y'])
    criteria_df['Log Abs Diff'] = np.log(criteria_df['Abs Diff'])
    
    criteria_df['Log Vol ES RTY ratio'] = criteria_df['Log Volume ES'] / criteria_df['Log Volume RTY']
    criteria_df['Log Vol RTY Diff ratio'] = criteria_df['Log Volume RTY'] / np.log(criteria_df['Abs Diff'])
    
    criteria_df = criteria_df[(criteria_df['Log Vol RTY Diff ratio'] <= 1.3) & (criteria_df['Log Vol RTY Diff ratio'] >= 0.9)]
    
    return criteria_df
    
def get_trade_day(df):
    date_list = df['date'].unique()

    trade_day_list = []
    not_trade_day_list = []
    
    for date in date_list:
        if ts.select_date_time(df, date, datetime.time(8,30,0))['TheoPrice'].empty:
            not_trade_day_list.append(date)
        else:
            trade_day_list.append(date)
    return trade_day_list

def tele_adjust(Tele2):
    new_Tele = ts.Telescope()
    
    new_Tele.df = Tele2.df.resample('5S', fill_method = 'bfill')

    new_Tele.parse_datetime()

    tm.apply_all_metrics(new_Tele.df, sma_period = 600)
    
    new_Tele.clip_time(datetime.time(8,30,0), datetime.time(15,15,0))
    
    return new_Tele
    

def run_bounce_match_strategy(Tele2, trade_day_list):
    base_time_start = datetime.time(8,30,0)
    
    base_time_end_list = [datetime.time(8,40,0)]
    
    test_length_list = [300,310,320,330,340,350,360,370,380,390]
    
    sma_list = [0.5]
    
    over_limit_time_list = [100]
    
    bounce_stop_list = [80]
    
    theo_diff_list = [50]
    
    column_list = [['Profit', 'Trade Number', ' Time Stop Count', 'Limite Stop Count', 'Bounce Stop Count', 'Base Start', 'Base End', 'Test Start', 'Test End', 'SMA Side Ratio Threshold', 'Bounce Stop Threshold', 'Over Limit Time', 'Theo Diff Threshold', 'Trading Cost']]
    for base_time_end in base_time_end_list:
        test_time_start = base_time_end
        for test_length in test_length_list:
            test_time_end = (ts.cal_datetime(test_time_start) + datetime.timedelta(minutes = test_length)).time()
            for sma in sma_list:
                for bounce_stop in bounce_stop_list:
                    for theo_diff in theo_diff_list:
                        for over_limit_time in tqdm(over_limit_time_list):
               
                            [daily_profit_result, trade_count, limit_stop_count, bounce_stop_count, time_stop_count, daily_record, bounce_stop_threshold] = bms.bounce_match_strategy(Tele2, trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold = sma, stop_threshold = bounce_stop, bounce_stop_threshold = bounce_stop, over_limit_length = over_limit_time, theo_diff = theo_diff, trade_cost = 30)
                
                            total_profit = np.sum(daily_profit_result[~np.isnan(daily_profit_result)])
                    
                            single_column = [total_profit, trade_count, time_stop_count, limit_stop_count, bounce_stop_count, base_time_start, base_time_end, test_time_start, test_time_end, sma, bounce_stop, over_limit_time, theo_diff, 30]
                            column_list.append(single_column)
    #
    daily_df = pd.DataFrame(daily_record[1:], columns=daily_record[0])
    result_df = pd.DataFrame(column_list[1:], columns=column_list[0])
    
    
#    result_df.to_excel("C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\RTY Morning Trend PL\\RTY_20190101_20190725_BOUNCE_MATCH_VARIOUS_PARAM_NO_FILTER.xlsx")
    return daily_df, result_df
    
def run_daily_best_param(Tele2, trade_day_list, criteria_df):
    base_time_start = datetime.time(8,30,0)

    base_time_end_list = [datetime.time(8,40,0)]
    
    test_length_list = [300]
    
    sma_list = [0.5]
    
    over_limit_time_list = [100]
    
    bounce_stop_list = [80]
    
    column_list = [['Date','Max Profit', 'Above Ratio', 'Below Ratio', 'Max min Spread','Average Theo', 'Std of Theo', 'Base Start', 'Base End', 'Test Start', 'Test End', 'SMA Side Ratio Threshold', 'Bounce Stop Threshold', 'Over Limit Time', 'Trading Cost']]
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
                   
                            [daily_profit_result, trade_count, limit_stop_count, bounce_stop_count, time_stop_count, loss_record, above_ratio, below_ratio, theo_change, absolute_theo_change, max_min_spread, average, std_dev] = dbfrt.daily_best_feature_record_test(Tele2, test_trade_day_list, base_time_start, base_time_end, test_time_start, test_time_end, sma_threshold = sma, stop_threshold = bounce_stop, bounce_stop_threshold = bounce_stop, over_limit_length = over_limit_time, trade_cost = 30)
                    
                            if daily_profit_result[0] > max_profit:
                                max_profit = daily_profit_result[0]
                                single_column = [test_trade_day, daily_profit_result[0], above_ratio, below_ratio, max_min_spread, average, std_dev, base_time_start, base_time_end, test_time_start, test_time_end, sma, bounce_stop, over_limit_time, 30]         
        column_list.append(single_column)
    loss_df = pd.DataFrame(loss_record[1:], columns=loss_record[0])
    
    result_df = pd.DataFrame(column_list[1:], columns=column_list[0])
    result_df.index = result_df['Date']
    del result_df['Date']
    final_result_df = result_df.join(criteria_df, how = 'left')
    del final_result_df['date']
    del final_result_df['time']
#    final_result_df.to_excel("C:\\Users\\achen\\Desktop\\Monocular\\Automatic Backtest\\RTY Morning Trend PL\\RTY_20180101_20181231_BOUNCE_MATCH_LAG100_WITH_ALL_FEATURES.xlsx")

def trendline(data, order=1):
    slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(np.arange(1,data.size+1), list(data))
    return slope, r_value

def find_trend(df):
    from sklearn.linear_model import LinearRegression
    [slope, r_value] = trendline(df['TheoPrice'])
    
    return slope
#    y = df['TheoPrice'].values
#    x = np.arange(1,(y.size+1)).reshape(1, y.size)
#    LR = LinearRegression()
#    print(x[:,1:4])
#    result = LR.fit(x[:,1:4],y[1:4])
#    return result.coef_
    

    
