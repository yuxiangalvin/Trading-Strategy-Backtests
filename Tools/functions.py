# -*- coding: utf-8 -*-
"""

"""

import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.tsa.stattools as ts
import numpy as np

##########  ##########
########## IMPORT DATASETS ##########
ym = pd.read_msgpack(r"Z:\KaiData\Theo\2019\YM.ESTheo.mpk")
nq = pd.read_msgpack(r"Z:\KaiData\Theo\2019\NQ.ESTheo.mpk")
rty = pd.read_msgpack(r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk")


df1 = ym[["TheoPrice"]]
df2 = nq[["TheoPrice"]]
df3 = rty[["TheoPrice"]]


########## FUNCTIONS TO GET SPECIFIC PERIODS OF DATA ##########
def get_month(month, df):
    """Return a DataFrame object with all entries in specified month.

    Takes in a DataFrame object and an int to specify the month and will
    filter out all data not in the month.
    """
    return df.loc[df.groupby(df.index.month).groups[month]]

def get_hour(hour, df):
    """Return a DataFrame object with all entries in specified hour.

    Takes in a DataFrame object and an int to specify the hour and will
    filter out all data not in the hour.
    """
    return df.loc[df.groupby(df.index.hour).groups[hour]]

def get_day(day, df):
    """Return a DataFrame object with all entries in specified day.

    Takes in a DataFrame object and an int to specify the day and will
    filter out all data not in the day.
    """
    try:
        return df.loc[df.groupby(df.index.day).groups[day]]
    except:
        print("This was not a trading day")
        
def get_minute(minute, df):
    """Return a DataFrame object with all entries in specified minute.

    Takes in a DataFrame object and an int to specify the minute and will
    filter out all data not in the minute.
    """
    return df.loc[df.groupby(df.index.minute).groups[minute]]

def resampler(size, df):   
    """Return resampled Dataframe.

    Takes in a DataFrame object and a string and resamples data 
    accordingly
    """
    return df.resample(size).mean()

def get_period(resample_size, day, hour, month, df):
    """Return DataFrame in specified period

    Takes in a DataFrame object and int values for day, hour, and month
    and returns the specified data. Will return None if there is no data
    for the specified time period. 
    """
    try:
        return resampler(resample_size, get_day(day, get_hour(hour, get_month(month, df))))
    except:
        return None
    
def plot_multiple_days(day1, day2, month, hour, df):
    for i in range(day1, day2):
        try:
            get_day(i, get_hour(hour, get_month(month, df))).plot()
        except:
            print("Not a trading day")
            
def most_move(df):
    ret = []
    first = 0
    last = 0
    for i in range(len(df)-12):
        first = df[i]
        last = df[i+11]
        ret.append({'Difference' : last-first, 'Time' : df.index[i]})
    return pd.DataFrame(ret)

def find_biggest_move(day, month, hour, df, interval):
    df = get_day(day, get_hour(hour, get_month(month, df)))
    largest = 0 
    ret = 1
    for i in range(len(df)-interval):
        if abs(df.TheoPrice[i] - df.TheoPrice[i+interval]) > largest:
            largest = abs(df.TheoPrice[i] - df.TheoPrice[i+interval])
            ret = i
    return df.iloc[ret]

def biggest_moves_days(day1, day2, month1, month2, df):
    index = []
    values = []
    for j in range(month1, month2):
        for i in range(day1, day2):
            try:
                index.append(find_biggest_move(i, j, 14, df, 2).name)
                values.append(find_biggest_move(i, j, 14, df, 2).TheoPrice)
            except:
                print("Not a trading day: ", i, " ", j)
    list_of_tuples = list(zip(index, values))
    return pd.DataFrame(list_of_tuples, columns = ['Time', 'Theo']) 

t = Telescope()
t.load_dfs([r"Z:\KaiData\Theo\2019\RTY.ESTheo.mpk"])
t.tele_resample('1T')
x = t.df
roc(x, 'TheoPrice')
x = get_hour(8, x)
x = get_minute(40, x)