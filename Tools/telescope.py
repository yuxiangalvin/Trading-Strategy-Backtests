import pandas as pd
import datetime
import telescope_metrics as tm
import numpy as np



df_2019 = r"Z:\KaiData\Theo\2019\YM.ESTheo.mpk"
df_2018 = r"Z:\KaiData\Theo\2018\YM.ESTheo.mpk"
df_lst = [df_2018, df_2019]

START = datetime.datetime(2019, 6, 15, 0, 0)
END = datetime.datetime(2019, 7, 12, 23, 59)
STIME = datetime.time(8, 0, 0)
ETIME = datetime.time(15, 15, 0)

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

def tele_resample_2(df, resample_freq):
    df['Total_Volume'] = df['Volume_x'] + df['Volume_y']
    df['Volume*Theo'] = df['Total_Volume'] * df['TheoPrice']
    df = df.resample(resample_freq).sum()
    df['VWAP'] = df['Volume*Theo'] / df['Total_Volume']
    df = df[['Volume_x', 'Volume_y', 'Total_Volume', 'VWAP']]
    df = df.rename(columns = {'VWAP':'TheoPrice'})
    return df
    
    
class Telescope:
    def __init__(self,df = None):
        print("Telescope Initiated...")
        self.df = df
        self.grouped = None
        self.num_of_groups = None
        self.group_names = None
    
    def load_all_dfs(self, lst_of_paths):
        loaded_dfs = []
        for path in lst_of_paths:
            current = pd.read_msgpack(path)
            loaded_dfs.append(current)
        self.df = pd.concat(loaded_dfs)

    
    def load_dfs(self, lst_of_paths):
        loaded_dfs = []
        for path in lst_of_paths:
            current = pd.read_msgpack(path)
            loaded_dfs.append(current)
        self.df = pd.concat(loaded_dfs)
        self.df = self.df[['Volume_x', 'Volume_y', 'TheoPrice']]
    
    
    def choose_range(self, start_dt, end_dt):
        self.df = self.df.loc[start_dt:end_dt]
    
    
    def go_back_n(self, n, interval):
        pass
    
    
    def tele_resample(self, resample_freq):
        self.df['Total_Volume'] = self.df['Volume_x'] + self.df['Volume_y']
        self.df['Volume*Theo'] = self.df['Total_Volume'] * self.df['TheoPrice']
        self.df = self.df.resample(resample_freq).sum()
        self.df['VWAP'] = self.df['Volume*Theo'] / self.df['Total_Volume']
        self.df = self.df[['Volume_x', 'Volume_y', 'Total_Volume', 'VWAP']]
        self.df = self.df.rename(columns = {'VWAP':'TheoPrice'})
#        count = 0
#        for i, row in self.df.iterrows():
#            if (count > 0) & (np.isnan(row['TheoPrice'])):
#                self.df.set_value(i,'TheoPrice',self.df.iloc[count - 1]['TheoPrice'])
#                
#            count += 1
    # NOW, USE FUNCTIONS DEFINED IN telescope_metrics.py
        
    
    def parse_datetime(self):
        self.df['date'] = self.df.index.date
        self.df['time'] = self.df.index.time
        
        
    def group_by_date(self):
        self.grouped = self.df.groupby(['date'])
        self.num_of_groups = len(list(self.grouped.groups))
        temp_df = self.df.drop_duplicates(subset='time', keep='first')
        self.group_names = list(temp_df['time'])
        
    def group_by_time(self):
        self.grouped = self.df.groupby(['time'])
        self.num_of_groups = len(list(self.grouped.groups))
        self.group_names = list(self.grouped.groups)
    
    def specific_timeframe(self, start_time, end_time):
        self.df = self.df.loc[start_time : end_time]
        return
    
    def choose_date(self, start_date, end_date):
        return self.df[(self.df['date'] >= start_date) &(self.df['date'] <= end_date)]
    
    def clip_date(self, start_date, end_date):
        self.df = self.df[(self.df['date'] >= start_date) &(self.df['date'] <= end_date)]
        return
    
    def clip_time(self, start_time, end_time):
        self.df = self.df[(self.df['time'] >= start_time) &(self.df['time'] <= end_time)]
        return

    # NOW, APPLY FILTERS
    
    
