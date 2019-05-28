# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 00:20:33 2019

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
ts.set_token('')  
pro = ts.pro_api()
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False





fileList = os.listdir('C:/Users/acer/Desktop/毕设相关/Code/data/newsList')

#news = pd.read_csv(fileList[0])
#negativeNews = news[news['title'].str.contains('↓')]
#for i in negativeNews['index']:
#    news['negative'] = -1
#else:
#    news['negative'] = 0 #不意味着情绪为中性
#in excel:=IF(ISNUMBER(FIND("↓",C2)),"-1","0")


# =============================================================================
'''
提取字段：
每个表以个股命名
日期  新闻条数  标题含有标的股票的新闻数量  负面新闻数量
date  NewsNum	IsTitleNum	NegativeNum	












'''
# =============================================================================



















