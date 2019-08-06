import pandas as pd

def absolute(df, column):
    output_name = 'abs_' + column
    df[output_name] = df[column].abs()
    

def sma(df, period=10):
    df['SMA'] = df['TheoPrice'].rolling(period).mean()

def bollinger(df, period=10):
    df['Bollinger_Upper'] = df['SMA'] + (df['TheoPrice'].rolling(period).std() * 2)
    df['Bollinger_Lower'] = df['SMA'] - (df['TheoPrice'].rolling(period).std() * 2)
    df['Bollinger_Band'] = df['Bollinger_Upper'] - df['Bollinger_Lower']

        
def roc(df, column, period=10):
    output_name = 'roc_' + column
    df[output_name] = (df[column].diff(period) / df[column].shift(period))*100
    
def theochange(df, period=10):
    
    df['Theo Change'] = (df['TheoPrice'].diff(period))
    return df
    

def apply_all_metrics(df, sma_period = 10, bollinger_period = 10):
#    absolute(df, 'TheoPrice')
    sma(df, sma_period)
#    bollinger(df, bollinger_period)
#    roc(df, 'abs_TheoPrice')
#    roc(df, 'Bollinger_Band')
    