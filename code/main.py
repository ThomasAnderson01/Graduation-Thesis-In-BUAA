# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 14:10:22 2019

@author: Lei Chen
"""


import pandas as pd
import numpy as np
import time
import talib as tlb
import math
import os
import datetime
import sys
import re
import jieba
import jieba.analyse
from os import path
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import tushare as ts
import os
import gensim 
from jqdatasdk import *
ts.set_token('8b0bb5278d24a9c5038bfa7e706856518f9608ad2b31105e3db10031')  
pro = ts.pro_api()
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False


#将date转换为日期string
#可以先将DatetimeIndex转换为一个类型为datetime的数组，然后对该数组进行操作得到一个numpy.ndarray，最后将这个array转化为Series即可

date = pd.date_range('20180101','20190401',freq = 'D')
pydate_array = date.to_pydatetime()
date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array )
dateList = pd.Series(date_only_array)
dateList = pd.DataFrame(dateList)



# =============================================================================
'''新闻指标提取'''
#原始列表：
#日期  时间  标题  来源  |  新闻情绪标记  标题是否包含股票名  



#提取字段：
#每个表以个股命名
#日期  新闻条数  标题含有标的股票的新闻数量  负面新闻数量
#date  NewsNum	IsTitleNum	NegativeNum	


fileList = os.listdir('C:/Users/acer/Desktop/毕设相关/Code/data/NewsData')
for code in sampleList['symbol']:
    news = pd.DataFrame(datelist)
    
    code = str(code)
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/NewsData/' + code + '.xlsx')
    df['count'] = 1
    df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))#时间列转换为str
    df = df.set_index('date')
    df_new = df.groupby(by=['date']).agg({'IsNegative':sum,'InTitle':sum,'count':sum})    #按日期去重，对于IsNegative，InTitle求和
    
    for day in df_new.index:   
        news.ix[day,'num'] = df_new.ix[day,'count']            #新闻数
        news.ix[day,'negative'] = df_new.ix[day,'IsNegative']     #负面新闻数
        news.ix[day,'Intitle'] = df_new.ix[day,'InTitle']         #重要新闻数（标题含有个股）
    news = news.fillna(0)
    news.to_excel('C:/Users/acer/Desktop/毕设相关/Code/data/NEWS/' + code + '.xlsx')




#看这段时间总共多少条新闻
sampleCount = sampleList
sampleCount = sampleCount.set_index('symbol')
for code in sampleList['symbol']:
    code = str(code)
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/NEWS/' + code + '.xlsx')
    sampleCount.ix[code,'num'] = sum(df['num'])
    sampleCount.ix[code,'negative'] = sum(df['negative'])
    sampleCount.ix[code,'Intitle'] = sum(df['Intitle'])




#高新闻报道组
highList = pd.read_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\highList.xlsx')
#high = dateList.set_index(0)
high = dateList.copy(deep=True)

high['num'] = 0
high['negative'] = 0
high['Intitle'] = 0

for code in highList['symbol']:
    code = str(code)
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/NEWS/' + code + '.xlsx')
    high['num'] = high['num'] + df['num']
    high['negative'] = high['negative'] + df['negative'] 
    high['Intitle'] = high['Intitle'] + df['Intitle']
high.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\high.xlsx')





#中新闻报道组
midList = pd.read_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\midList.xlsx')
mid = dateList.copy(deep=True)

mid['num'] = 0
mid['negative'] = 0
mid['Intitle'] = 0

for code in midList['symbol']:
    code = str(code)
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/NEWS/' + code + '.xlsx')
    mid['num'] = mid['num'] + df['num']
    mid['negative'] = mid['negative'] + df['negative'] 
    mid['Intitle'] = mid['Intitle'] + df['Intitle']
mid.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\mid.xlsx')





#低新闻报道组
lowList = pd.read_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\lowList.xlsx')
low = dateList.copy(deep=True)

low['num'] = 0
low['negative'] = 0
low['Intitle'] = 0

for code in lowList['symbol']:
    code = str(code)
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/NEWS/' + code + '.xlsx')
    low['num'] = low['num'] + df['num']
    low['negative'] = low['negative'] + df['negative'] 
    low['Intitle'] = low['Intitle'] + df['Intitle']
low.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\low.xlsx')



# =============================================================================
'''个股投资者情绪指标'''
#组合的话，就加权平均一下吧

from jqdatasdk import *
auth('13126529001','triggeron') #账号是申请时所填写的手机号；密码为聚宽官网登录密码，新申请用户默认为手机号后6位
import tushare as ts
ts.set_token('8b0bb5278d24a9c5038bfa7e706856518f9608ad2b31105e3db10031')  
pro = ts.pro_api()

sampleList = pd.read_excel(dataPath + '/' + 'sampleList_adj.xlsx')

for code in sampleList['jq_code']:
    xueqiu = finance.run_query(query(finance.STK_XUEQIU_PUBLIC).filter(finance.STK_XUEQIU_PUBLIC.day >= '2018-01-01',finance.STK_XUEQIU_PUBLIC.day<'2019-04-01',finance.STK_XUEQIU_PUBLIC.code==code))
    xueqiu.to_excel('C:/Users/acer/Desktop/毕设相关/Code/data/SENTIMENT/'+code + '.xlsx')
    print("本日剩余可调用数据条数：",get_query_count())
    

#ar,br



high_factor_data = get_factor_values(securities=['600588.XSHG'], factors=['AR','BR'], start_date='2018-01-01', end_date='2019-04-01')
high_ar = high_factor_data['AR']
high_br = high_factor_data['BR']
high_ar.to_excel('high_ar.xlsx')
high_br.to_excel('high_br.xlsx')

mid_factor_data = get_factor_values(securities=['600804.XSHG'], factors=['AR','BR'], start_date='2018-01-01', end_date='2019-04-01')
mid_ar = mid_factor_data['AR']
mid_br = mid_factor_data['BR']
mid_ar.to_excel('mid_ar.xlsx')
mid_br.to_excel('mid_br.xlsx')

low_factor_data = get_factor_values(securities=['300421.XSZE'], factors=['AR','BR'], start_date='2018-01-01', end_date='2019-04-01')
low_ar = low_factor_data['AR']
low_br = low_factor_data['BR']
low_ar.to_excel('low_ar.xlsx')
low_br.to_excel('low_br.xlsx')

#turnover rate
high_basic = pro.daily_basic(ts_code='600588.SH', start_date='20180101', end_date = '20190401',fields='trade_date,turnover_rate,volume_ratio,pe,pb')
mid_basic = pro.daily_basic(ts_code='600804.SH', start_date='20180101', end_date = '20190401',fields='trade_date,turnover_rate,volume_ratio,pe,pb')
low_basic = pro.daily_basic(ts_code='300421.SZ', start_date='20180101', end_date = '20190401',fields='trade_date,turnover_rate,volume_ratio,pe,pb')
high_basic.to_excel('high_basic.xlsx')
mid_basic.to_excel('mid_basic.xlsx')
low_basic.to_excel('low_basic.xlsx')

'''midhigh取数取错了'''
midhigh_factor_data = get_factor_values(securities=['300107.XSHE'], factors=['AR','BR'], start_date='2018-01-01', end_date='2019-04-01')
midhigh_ar = midhigh_factor_data['AR']
midhigh_br = midhigh_factor_data['BR']
midhigh_ar.to_excel('midhigh_ar.xlsx')
midhigh_br.to_excel('midhigh_br.xlsx')
midhigh_basic = pro.daily_basic(ts_code='300107.SZ', start_date='20180101', end_date = '20190401',fields='trade_date,turnover_rate,volume_ratio,pe,pb')
midhigh_basic.to_excel('midhigh_basic.xlsx')

midlow_factor_data = get_factor_values(securities=['600827.XSHG'], factors=['AR','BR'], start_date='2018-01-01', end_date='2019-04-01')
midlow_ar = midlow_factor_data['AR']
midlow_br = midlow_factor_data['BR']
midlow_ar.to_excel('midlow_ar.xlsx')
midlow_br.to_excel('midlow_br.xlsx')
midlow_basic = pro.daily_basic(ts_code='600827.SH', start_date='20180101', end_date = '20190401',fields='trade_date,turnover_rate,volume_ratio,pe,pb')
midlow_basic.to_excel('midlow_basic.xlsx')


#high
high = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\high\\high.xlsx')

i = 0
high['AR'] = 0
high['BR'] = 0
high['turnover'] = 0
high['ret'] = 0
ret = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\high\\highRET.xlsx')    

for day in high['date']:
    temp = ret[ret['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       high['AR'].iloc[i] = temp['AR'].iloc[0] 
       high['BR'].iloc[i] = temp['BR'].iloc[0]
       high['turnover'].iloc[i] = temp['turnover_rate'].iloc[0]
       high['ret'].iloc[i] = temp['ret'].iloc[0]
    i = i+1
    print(i)
    
#midhigh
midhigh = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\midhigh\\midhigh.xlsx')

i = 0
midhigh['AR'] = 0
midhigh['BR'] = 0
midhigh['turnover'] = 0
midhigh['ret'] = 0
midhigh['follower'] = 0
midhigh['new_follower'] = 0
midhigh['discussion'] = 0
midhigh['new_discussion'] = 0
midhigh['trade'] = 0
midhigh['new_trade'] = 0
ret = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\midhigh\\midhighret.xlsx')    
xueqiu = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\midhigh\\xueqiu.xlsx')    

for day in midhigh['date']:
    temp = ret[ret['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       midhigh['AR'].iloc[i] = temp['AR'].iloc[0] 
       midhigh['BR'].iloc[i] = temp['BR'].iloc[0]
       midhigh['turnover'].iloc[i] = temp['turnover_rate'].iloc[0]
       midhigh['ret'].iloc[i] = temp['ret'].iloc[0]

       
    temp = xueqiu[xueqiu['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       midhigh['follower'].iloc[i] = temp['follower'].iloc[0]
       midhigh['new_follower'].iloc[i] = temp['new_follower'].iloc[0]
       midhigh['discussion'].iloc[i] = temp['discussion'].iloc[0]
       midhigh['new_discussion'].iloc[i] = temp['new_discussion'].iloc[0]
       midhigh['trade'].iloc[i] = temp['trade'].iloc[0]       
       midhigh['new_trade'].iloc[i] = temp['new_trade'].iloc[0]         
    i = i+1
    print(i)

midhigh['follower'] = midhigh['follower'].replace(0,np.nan)
midhigh['discussion'] = midhigh['discussion'].replace(0,np.nan)
midhigh['trade'] = midhigh['trade'].replace(0,np.nan)

midhigh['follower'] = midhigh['follower'].fillna(method = 'ffill')
midhigh['discussion'] = midhigh['discussion'].fillna(method = 'ffill')
midhigh['trade'] = midhigh['trade'].fillna(method = 'ffill')



#mid
mid = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\mid\\mid.xlsx')

i = 0
mid['AR'] = 0
mid['BR'] = 0
mid['turnover'] = 0
mid['ret'] = 0
mid['follower'] = 0
mid['new_follower'] = 0
mid['discussion'] = 0
mid['new_discussion'] = 0
mid['trade'] = 0
mid['new_trade'] = 0
ret = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\mid\\midret.xlsx')    
xueqiu = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\mid\\midxueqiu.xlsx')    

for day in mid['date']:
    temp = ret[ret['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       mid['AR'].iloc[i] = temp['AR'].iloc[0] 
       mid['BR'].iloc[i] = temp['BR'].iloc[0]
       mid['turnover'].iloc[i] = temp['turnover_rate'].iloc[0]
       mid['ret'].iloc[i] = temp['ret'].iloc[0]

       
    temp = xueqiu[xueqiu['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       mid['follower'].iloc[i] = temp['follower'].iloc[0]
       mid['new_follower'].iloc[i] = temp['new_follower'].iloc[0]
       mid['discussion'].iloc[i] = temp['discussion'].iloc[0]
       mid['new_discussion'].iloc[i] = temp['new_discussion'].iloc[0]
       mid['trade'].iloc[i] = temp['trade'].iloc[0]       
       mid['new_trade'].iloc[i] = temp['new_trade'].iloc[0]         
    i = i+1
    print(i)







#midlow
midlow = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\midlow\\midlow.xlsx')

i = 0
midlow['AR'] = 0
midlow['BR'] = 0
midlow['turnover'] = 0
midlow['ret'] = 0
midlow['follower'] = 0
midlow['new_follower'] = 0
midlow['discussion'] = 0
midlow['new_discussion'] = 0
midlow['trade'] = 0
midlow['new_trade'] = 0
ret = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\midlow\\midlowret.xlsx')    
xueqiu = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\midlow\\xueqiu.xlsx')    

for day in midlow['date']:
    temp = ret[ret['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       midlow['AR'].iloc[i] = temp['AR'].iloc[0] 
       midlow['BR'].iloc[i] = temp['BR'].iloc[0]
       midlow['turnover'].iloc[i] = temp['turnover_rate'].iloc[0]
       midlow['ret'].iloc[i] = temp['ret'].iloc[0]

       
    temp = xueqiu[xueqiu['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       midlow['follower'].iloc[i] = temp['follower'].iloc[0]
       midlow['new_follower'].iloc[i] = temp['new_follower'].iloc[0]
       midlow['discussion'].iloc[i] = temp['discussion'].iloc[0]
       midlow['new_discussion'].iloc[i] = temp['new_discussion'].iloc[0]
       midlow['trade'].iloc[i] = temp['trade'].iloc[0]       
       midlow['new_trade'].iloc[i] = temp['new_trade'].iloc[0]         
    i = i+1
    print(i)

midlow['follower'] = midlow['follower'].replace(0,np.nan)
midlow['discussion'] = midlow['discussion'].replace(0,np.nan)
midlow['trade'] = midlow['trade'].replace(0,np.nan)

midlow['follower'] = midlow['follower'].fillna(method = 'ffill')
midlow['discussion'] = midlow['discussion'].fillna(method = 'ffill')
midlow['trade'] = midlow['trade'].fillna(method = 'ffill')





#low
low = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\low\\low.xlsx')

i = 0
low['turnover'] = 0
low['ret'] = 0
low['follower'] = 0
low['new_follower'] = 0
low['discussion'] = 0
low['new_discussion'] = 0
low['trade'] = 0
low['new_trade'] = 0
ret = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\low\\lowret.xlsx')    
xueqiu = pd.read_excel('C:\\Users\\acer\\Desktop\\中介效应\\low\\lowxueqiu.xlsx')    

for day in low['date']:
    temp = ret[ret['trade_date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       low['turnover'].iloc[i] = temp['turnover_rate'].iloc[0]
       low['ret'].iloc[i] = temp['ret'].iloc[0]

       
    temp = xueqiu[xueqiu['date'].isin([day])] #第day天
    if not temp.empty:      #如果非空，按date值赋值
       low['follower'].iloc[i] = temp['follower'].iloc[0]
       low['new_follower'].iloc[i] = temp['new_follower'].iloc[0]
       low['discussion'].iloc[i] = temp['discussion'].iloc[0]
       low['new_discussion'].iloc[i] = temp['new_discussion'].iloc[0]
       low['trade'].iloc[i] = temp['trade'].iloc[0]       
       low['new_trade'].iloc[i] = temp['new_trade'].iloc[0]         
    i = i+1
    print(i)











# =============================================================================
'''股票收益率'''
import tushare as ts
ts.set_token('8b0bb5278d24a9c5038bfa7e706856518f9608ad2b31105e3db10031')  
pro = ts.pro_api()

dataPath = 'C:/Users/acer/Desktop/毕设相关/Code/data'
sampleList = pd.read_csv(dataPath + '/' + 'sampleList.csv',engine='python')
    
for code in sampleList['ts_code']:
    ohlc = pro.query('daily', ts_code = code, start_date='20180101', end_date='20190401')
    #不用算个股收益率，pct_chg就是收益率
    ohlc.rename(columns={'pct_chg':'ret'}, inplace = True)
    ohlc.to_excel('C:/Users/acer/Desktop/毕设相关/Code/data/OHLC/' + code + '.xlsx')
    
    
    
    

tsdate = pd.date_range('20180101','20190401',freq = 'D') 
tsdate_array = tsdate.to_pydatetime()
date_only_array_ts = np.vectorize(lambda s: s.strftime('%Y%m%d'))(tsdate_array )
tsdateList = pd.Series(date_only_array_ts)
tsdateList = pd.DataFrame(tsdateList)




#计算组合收益率
highList = pd.read_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\highList.xlsx')
midList = pd.read_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\midList.xlsx')
lowList = pd.read_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\lowList.xlsx')
#高
#high = dateList.set_index(0)
HighOHLC = tsdateList.copy(deep=True)
HighOHLC.rename(columns={0:'date'}, inplace = True)

#HighOHLC = df.copy(deep=True)
HighOHLC['open'] = 0
HighOHLC['high'] = 0
HighOHLC['low'] = 0
HighOHLC['close'] = 0
HighOHLC['change'] = 0
HighOHLC['ret'] = 0
HighOHLC['vol'] = 0
HighOHLC['amount'] = 0
#HighOHLC = HighOHLC.drop(['pre_close'],axis=1)
#HighOHLC = HighOHLC.set_index('date')


for code in highList['ts_code']:
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/OHLC/' + code + '.xlsx')
#    df = df.rename(columns={'trade_date':'date'}, inplace = True)
    df = df.drop(['ts_code'],axis=1)
    df = df.drop(['pre_close'],axis=1)
#    df = df.set_index('trade_date')
#    HighOHLC = HighOHLC + df
#    HighOHLC['open'] = HighOHLC['open'] + df['open']
#    HighOHLC['high'] = HighOHLC['high'] + df['high']
#    HighOHLC['low'] = HighOHLC['low'] + df['low']
#    HighOHLC['close'] = HighOHLC['close'] + df['close']
#    HighOHLC['vol'] = HighOHLC['vol'] + df['vol']
#    HighOHLC['amount'] = HighOHLC['amount'] + df['amount']
    
#    HighOHLC = HighOHLC.add(df)
    i = 0
    for day in HighOHLC['date']:
        temp = df[df['trade_date'].isin([day])] #第day天
        if not temp.empty:      #如果非空，按date值赋值
            HighOHLC['open'].iloc[i] = HighOHLC['open'].iloc[i] + temp['open'].iloc[0]
            HighOHLC['high'].iloc[i] = HighOHLC['high'].iloc[i] + temp['high'].iloc[0]
            HighOHLC['low'].iloc[i] = HighOHLC['low'].iloc[i] + temp['low'].iloc[0]
            HighOHLC['close'].iloc[i] = HighOHLC['close'].iloc[i] + temp['close'].iloc[0]
            HighOHLC['change'].iloc[i] = HighOHLC['change'].iloc[i] + temp['change'].iloc[0]
            HighOHLC['vol'].iloc[i] = HighOHLC['vol'].iloc[i] + temp['vol'].iloc[0]
            HighOHLC['amount'].iloc[i] = HighOHLC['amount'].iloc[i] + temp['amount'].iloc[0]    
        i = i+1
        print(day,'已完成')    
    print('=================================',code,'已完成')
HighOHLC.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\ohlc\\high_return.xlsx')
HighOHLC['open'] = HighOHLC['open'].replace(0,np.nan)
HighOHLC['close'] = HighOHLC['close'].replace(0,np.nan)
HighOHLC['high'] = HighOHLC['high'].replace(0,np.nan)
HighOHLC['low'] = HighOHLC['low'].replace(0,np.nan)
HighOHLC['open'] = HighOHLC['open'].fillna(method = 'ffill')
HighOHLC['close'] = HighOHLC['close'].fillna(method = 'ffill')
HighOHLC['high'] = HighOHLC['high'].fillna(method = 'ffill')    #值得商榷
HighOHLC['low'] = HighOHLC['low'].fillna(method = 'ffill')
HighOHLC['amount'] = HighOHLC['amount'].fillna(method = 'ffill')
HighOHLC['change'] = HighOHLC['change'].fillna(0)
HighOHLC['ret'] = (HighOHLC['close']-HighOHLC['close'].shift(1))/HighOHLC['close'].shift(1)
HighOHLC.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\ohlc\\highadj_return.xlsx')
print('高新闻组完成')


#中
MidOHLC = tsdateList.copy(deep=True)
MidOHLC.rename(columns={0:'date'}, inplace = True)
MidOHLC['open'] = 0
MidOHLC['high'] = 0
MidOHLC['low'] = 0
MidOHLC['close'] = 0
MidOHLC['change'] = 0
MidOHLC['ret'] = 0
MidOHLC['vol'] = 0
MidOHLC['amount'] = 0


for code in midList['ts_code']:
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/OHLC/' + code + '.xlsx')
    df = df.drop(['ts_code'],axis=1)
    df = df.drop(['pre_close'],axis=1)

    i = 0
    for day in MidOHLC['date']:
        temp = df[df['trade_date'].isin([day])] #第day天
        if not temp.empty:      #如果非空，按date值赋值
            MidOHLC['open'].iloc[i] = MidOHLC['open'].iloc[i] + temp['open'].iloc[0]
            MidOHLC['high'].iloc[i] = MidOHLC['high'].iloc[i] + temp['high'].iloc[0]
            MidOHLC['low'].iloc[i] = MidOHLC['low'].iloc[i] + temp['low'].iloc[0]
            MidOHLC['close'].iloc[i] = MidOHLC['close'].iloc[i] + temp['close'].iloc[0]
            MidOHLC['change'].iloc[i] = MidOHLC['change'].iloc[i] + temp['change'].iloc[0]
            MidOHLC['vol'].iloc[i] = MidOHLC['vol'].iloc[i] + temp['vol'].iloc[0]
            MidOHLC['amount'].iloc[i] = MidOHLC['amount'].iloc[i] + temp['amount'].iloc[0]    
        i = i+1
        print(day,'已完成')    
    print('=================================',code,'已完成')
MidOHLC.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\ohlc\\mid_return.xlsx')

MidOHLC['open'] = MidOHLC['open'].replace(0,np.nan)
MidOHLC['close'] = MidOHLC['close'].replace(0,np.nan)
MidOHLC['high'] = MidOHLC['high'].replace(0,np.nan)    #值得商榷
MidOHLC['low'] = MidOHLC['low'].replace(0,np.nan)

MidOHLC['open'] = MidOHLC['open'].fillna(method = 'ffill')
MidOHLC['close'] = MidOHLC['close'].fillna(method = 'ffill')
MidOHLC['high'] = MidOHLC['high'].fillna(method = 'ffill')    #值得商榷
MidOHLC['low'] = MidOHLC['low'].fillna(method = 'ffill')
MidOHLC['amount'] = MidOHLC['amount'].fillna(method = 'ffill')
MidOHLC['change'] = MidOHLC['change'].fillna(0)
MidOHLC['ret'] = (MidOHLC['close']-MidOHLC['close'].shift(1))/MidOHLC['close'].shift(1)
MidOHLC.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\ohlc\\midadj_return.xlsx')
print('中新闻组完成')
#低
LowOHLC = tsdateList.copy(deep=True)
LowOHLC.rename(columns={0:'date'}, inplace = True)
LowOHLC['open'] = 0
LowOHLC['high'] = 0
LowOHLC['low'] = 0
LowOHLC['close'] = 0
LowOHLC['change'] = 0
LowOHLC['ret'] = 0
LowOHLC['vol'] = 0
LowOHLC['amount'] = 0


for code in lowList['ts_code']:
    df = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/OHLC/' + code + '.xlsx')
    df = df.drop(['ts_code'],axis=1)
    df = df.drop(['pre_close'],axis=1)

    i = 0
    for day in LowOHLC['date']:
        temp = df[df['trade_date'].isin([day])] #第day天
        if not temp.empty:      #如果非空，按date值赋值
            LowOHLC['open'].iloc[i] = LowOHLC['open'].iloc[i] + temp['open'].iloc[0]
            LowOHLC['high'].iloc[i] = LowOHLC['high'].iloc[i] + temp['high'].iloc[0]
            LowOHLC['low'].iloc[i] = LowOHLC['low'].iloc[i] + temp['low'].iloc[0]
            LowOHLC['close'].iloc[i] = LowOHLC['close'].iloc[i] + temp['close'].iloc[0]
            LowOHLC['change'].iloc[i] = LowOHLC['change'].iloc[i] + temp['change'].iloc[0]
            LowOHLC['vol'].iloc[i] = LowOHLC['vol'].iloc[i] + temp['vol'].iloc[0]
            LowOHLC['amount'].iloc[i] = LowOHLC['amount'].iloc[i] + temp['amount'].iloc[0]    
        i = i+1
        print(day,'已完成')    
    print('=================================',code,'已完成')
LowOHLC.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\ohlc\\low_return.xlsx')

LowOHLC['open'] = LowOHLC['open'].replace(0,np.nan)
LowOHLC['close'] = LowOHLC['close'].replace(0,np.nan)
LowOHLC['high'] = LowOHLC['high'].replace(0,np.nan)    #值得商榷
LowOHLC['low'] = LowOHLC['low'].replace(0,np.nan)


LowOHLC['open'] = LowOHLC['open'].fillna(method = 'ffill')
LowOHLC['close'] = LowOHLC['close'].fillna(method = 'ffill')
LowOHLC['high'] = LowOHLC['high'].fillna(method = 'ffill')    #值得商榷
LowOHLC['low'] = LowOHLC['low'].fillna(method = 'ffill')
LowOHLC['amount'] = LowOHLC['amount'].fillna(method = 'ffill')
LowOHLC['change'] = LowOHLC['change'].fillna(0)
LowOHLC['ret'] = (LowOHLC['close']-LowOHLC['close'].shift(1))/LowOHLC['close'].shift(1)
LowOHLC.to_excel('C:\\Users\\acer\\Desktop\\毕设相关\\Code\\data\\CONCAT\\ohlc\\lowadj_return.xlsx')
print('低新闻组完成')


    
    
    
    
    
# =============================================================================
'''中介效应分析'''


#数据预处理


concat文件夹，将每只股票的所有数据，以日期为index汇总



最后做个allData的列表文件夹
总共三个excel，包含高中低三个组，以日期为index


allData = dateList









zyNews = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/NEWS/920.xlsx')
zyNews['date'] = pd.to_datetime(zyNews['date'], format = '%Y-%m-%d') 
#zyNews = zyNews.set_index('date')
zyRet = pd.read_excel('C:/Users/acer/Desktop/毕设相关/Code/data/OHLC/000920.SZ.xlsx')
zyRet['trade_date'] = pd.to_datetime(zyRet['trade_date'], format = '%Y%m%d') 
#zyRet  = zyRet.set_index('trade_date')


plt.subplots(figsize=(16,8))
plt.subplot(211)
plt.plot(zyNews['date'],zyNews['num'],label='NewsNum')
plt.plot(zyNews['date'],zyNews['negative'],label='NegativeNum')
plt.plot(zyNews['date'],zyNews['Intitle'],label='IntitleNum')
plt.legend()
plt.subplot(212)
plt.plot(zyRet['trade_date'],zyRet['ret'],'r',label='Return')

plt.legend()

plt.savefig('nfht.png')

