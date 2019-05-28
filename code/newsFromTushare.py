# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 13:40:46 2019

@author: Lei Chen
"""


import tushare as ts
ts.set_token('')
pro = ts.pro_api()
#import jqdatasdk as jq
#jq.auth('', '')
import talib as tlb
import numpy as np
import pandas as pd
import math
import os
import time
import datetime
import sys
import re
import jieba

'''get news data'''
#单次最大1000条新闻,每分钟最多调用10次接口(6秒1次)
#开始日期为20181008
#


startDate = datetime.datetime(2018,10,8,00,00,00)
delta = datetime.timedelta(days=1)
endDate = datetime.datetime(2019,4,9,00,00,00)

ex5 = pro.news(src='sina', start_date=startDate.strftime('20180921'), end_date=endDate.strftime('20180930')) 


# =============================================================================
# 
# sina = pro.news(src='sina', start_date=startDate.strftime('%Y-%m-%d %H:%M:%S'), end_date=endDate.strftime('%Y-%m-%d %H:%M:%S')) 
# 
# stampDate = startDate + delta
# nextDate = stampDate + delta       #每次取一天，即stampDate                   #新浪财经
# sina2 = pro.news(src='sina', start_date='20181002', end_date='20181009')  
# 
# 
# =============================================================================
#新浪财经	sina	获取新浪财经实时资讯
#华尔街见闻	wallstreetcn	华尔街见闻快讯
#同花顺	10jqka	同花顺财经新闻
#东方财富	eastmoney	东方财富财经新闻
#云财经	yuncaijing	云财经新闻


#news_sina = []
#news_wallstreetcn = []
#news_10jqka = []
#news_eastmoney = []
news_yuncaijing = []

stampDate = startDate

for i in range(500): 
    if stampDate<=endDate:
        stampDate = startDate + i*delta
        nextDate = stampDate + delta       #每次取一天，即stampDate
#        sina = pro.news(src='sina', start_date=stampDate.strftime('%Y-%m-%d %H:%M:%S'), end_date=nextDate.strftime('%Y-%m-%d %H:%M:%S'))                    #新浪财经
#        time.sleep(6)
#        wallstreetcn = pro.news(src='wallstreetcn', start_date=stampDate.strftime('%Y-%m-%d %H:%M:%S'), end_date=nextDate.strftime('%Y-%m-%d %H:%M:%S'))    #华尔街见闻
#        time.sleep(6)
#        ths = pro.news(src='10jqka', start_date=stampDate.strftime('%Y-%m-%d %H:%M:%S'), end_date=nextDate.strftime('%Y-%m-%d %H:%M:%S'))                #同花顺
#        time.sleep(6)
#        eastmoney = pro.news(src='eastmoney',start_date=stampDate.strftime('%Y-%m-%d %H:%M:%S'), end_date=nextDate.strftime('%Y-%m-%d %H:%M:%S'))          #东方财富
#        time.sleep(6)
        yuncaijing = pro.news(src='yuncaijing',start_date=stampDate.strftime('%Y-%m-%d %H:%M:%S'), end_date=nextDate.strftime('%Y-%m-%d %H:%M:%S'))          #东方财富
        time.sleep(6)
       
#        news_sina.append(sina)
#        news_wallstreetcn.append(wallstreetcn)
#        news_10jqka.append(ths)
#        news_eastmoney.append(eastmoney)
        news_yuncaijing.append(yuncaijing)
        
        sys.stdout.flush()
     
    else:
        break





#sina_text = news_sina[0]
#jqka_text = news_10jqka[0]
#eastmoney_text = news_eastmoney[0]
#wallstreetcn_text = news_wallstreetcn[0]
yuncaijing_text = news_yuncaijing[0]



for i in range(1,185):
#    sina_text = sina_text.append(news_sina[i])
#    jqka_text = jqka_text.append(news_10jqka[i])
#    eastmoney_text = eastmoney_text.append(news_eastmoney[i])
#    wallstreetcn_text = wallstreetcn_text.append(news_wallstreetcn[i])
    yuncaijing_text = yuncaijing_text.append(news_yuncaijing[i])



#sina_text.to_csv("sina_text.csv")
#jqka_text.to_csv("jqka_text.csv")
#eastmoney_text.to_csv("eastmoney_text.csv")
#wallstreetcn_text.to_csv("wallstreetcn_text.csv")
yuncaijing_text.to_csv("yuncaijing_text.csv")




