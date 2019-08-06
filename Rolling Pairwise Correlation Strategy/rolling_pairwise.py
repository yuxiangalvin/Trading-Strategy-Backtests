# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 12:02:17 2019

@author: achen
"""

import pandas as pd
import datetime
import numpy as np
from tqdm import tqdm


# The following global constants state the time period that we would like to investigate.
DATETIME_0 = datetime.datetime(2019, 7, 12, 23, 59, 59)
DATETIME_1= datetime.datetime(2019, 7, 8, 0, 0, 0)
DATETIME_2 = datetime.datetime(2019, 7, 1, 0, 0, 0)

START_TIME = datetime.time(8, 00)
END_TIME = datetime.time(15, 15)
START_DATETIME = DATETIME_1
END_DATETIME = DATETIME_0
DF_PATH = r"Z:\KaiData\Theo\2019\NQ.ESTheo.mpk"
RESAMPLE_FREQ = "1T"


def trim_df(df, start, end):
    ''' This function helps to trim the df so that it only contains the time period that we are interested in. '''
    return df.loc[start : end]


def resample_df(df, resample_freq):
    ''' This function helps to resample the df according to the designated resample frequency. '''
    df['Volume_x_y'] = df['Volume_x'] + df['Volume_y']
    df['Volume_x_y*TheoPrice'] = df['Volume_x_y'] * df['TheoPrice']
    output = df.resample(resample_freq).sum()
    output['VWAP'] = output['Volume_x_y*TheoPrice'] / output['Volume_x_y']
    return output
    

def drop_and_rename(df):
    ''' This function first removes all the useless columns in the df, and then give the remaining columns nicer 
    name. '''
    output = df[['VWAP']]
    output.columns = ['TheoPrice']
    return output


def init_df():
    ''' This function calls all the other initiation helper functions. '''
    global df
    df = pd.read_msgpack(DF_PATH)
    df = trim_df(df, START_DATETIME, END_DATETIME)
    df = resample_df(df, RESAMPLE_FREQ)
    df = drop_and_rename(df)



def select_time(time_lst):
    ''' This function trims the input time_lst so that it shall only contain the time intervals that we are interested
    in. '''
    output_lst = []
    for time in time_lst:
        if time > START_TIME and time < END_TIME:
            output_lst.append(time)
    return output_lst
        

def compare(df):
    # The first part of this function will be grouping the df by times.
    df['time'] = df.index.time
    grouped = df.groupby(['time'])
    
    time_list = list(grouped.groups)
    time_list = select_time(time_list)
    
    column_list = [time_list]
    for i in tqdm(time_list):
        df_i = grouped.get_group(i)
        df_i.reset_index(inplace=True)
        column_i = []
        for j in time_list:
            if j > i:
                df_j = grouped.get_group(j)
                df_j.reset_index(inplace=True)
                current_corr = df_i.corrwith(df_j, axis=0)[0]
                column_i.append(current_corr)
            else:
                column_i.append(np.nan)
        column_list.append(column_i)
    
    global result
    result = pd.DataFrame(column_list[1:], columns=column_list[0])
    result['time'] = time_list
    result.set_index('time', inplace=True)
    
    
        






