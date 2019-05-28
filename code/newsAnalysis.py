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

from gensim.models import word2vec  #向量化？'''
import os
import gensim 
ts.set_token('')  
pro = ts.pro_api()

plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False


#查询当前所有正常上市交易的股票列表



# =============================================================================
'''对新闻进行文本分析'''








# 得到所有股票的代码和中文名字，将其作为新词录入词典
def prepare():
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    exist = df[ ~ df['name'].str.contains('ST') ]
    stockList = exist[~(exist['list_date'].apply(int)>= 20181008 )]
    name = exist['name']
    code = exist.index.tolist()
    namelist = exist.values.tolist()
    for c in code:
        jieba.add_word(c)
    for n in namelist:
        jieba.add_word(n)
    
    
stockList = pd.read_csv('stocklist.csv')

sampleList = stockList.sample(n=50)




# 统计词频,由于jieba库没有统计词频的功能，因此这块要额外写
def wordcount(text):
    # 文章字符串前期处理
    strl_ist = jieba.lcut(text, cut_all=True) 
    count_dict = {}
    all_num = 0;
    # 如果字典里有该单词则加1，否则添加入字典
    for str in strl_ist:
        if(len(str) <= 1):
            continue
        else:
            all_num+=1
        if str in count_dict.keys():
            count_dict[str] = count_dict[str] + 1
        else:
            count_dict[str] = 1
    #按照词频从高到低排列
    count_list=sorted(count_dict.items(),key=lambda x:x[1],reverse=True)
    return count_list, all_num



# =============================================================================






#绘制词云图
#def wordCloud(content): #词云函数,输入为dataframe
#    content = content.dropna(how = 'any').values
#    text = ""
#    for t in content:
#        text += t
#        analyze = jieba.analyse.extract_tags(text, topK=50, withWeight=False, allowPOS=())
#        lists = " ".join(analyze).split(' ')
#    print(lists)    #输出这些词
#    seg_list = jieba.cut(text, cut_all=False) 
#    words_split = " ".join(seg_list)
#    wc = WordCloud(font_path="simhei.ttf", max_words = 50, background_color = 'white', width = 800, height = 500)    
#    wordcloud = wc.generate(words_split)
#    return wordcloud

# 分析文本中的词频
#新浪财经	sina	获取新浪财经实时资讯
#华尔街见闻	wallstreetcn	华尔街见闻快讯
#同花顺	10jqka	同花顺财经新闻
#东方财富	eastmoney	东方财富财经新闻
#云财经	yuncaijing	云财经新闻
    
plt.figure(figsize=(16, 8))
plt.subplot(221)
sinaNews = pd.read_csv('sina_text.csv')
sinaContent = sinaNews['content'].dropna(how = 'any').values
sinaText = ""
for q in sinaContent:
    sinaText += q
sinaAnalyze = jieba.analyse.extract_tags(sinaText, topK=50, withWeight=False, allowPOS=())
sinaLists = " ".join(sinaAnalyze).split(' ')
print(sinaLists)
sinaSegList = jieba.cut(sinaText, cut_all=False) 
sinaWords_split = " ".join(sinaSegList)
sinaWC = WordCloud(font_path="simhei.ttf", max_words = 50, background_color = 'white', width = 800, height = 500)    
sinaWordcloud = sinaWC.generate(sinaWords_split)
plt.imshow(sinaWordcloud)
plt.axis("off")
plt.title('新浪财经词云图')

#
plt.subplot(222)
wsNews = pd.read_csv('wallstreetcn_text.csv')
wsContent = wsNews['content'].dropna(how = 'any').values
wsText = ""
for q in wsContent:
    wsText += q
wsAnalyze = jieba.analyse.extract_tags(wsText, topK=50, withWeight=False, allowPOS=())
wsLists = " ".join(wsAnalyze).split(' ')
print(wsLists)
wsSegList = jieba.cut(wsText, cut_all=False) 
wsWords_split = " ".join(wsSegList)
wsWC = WordCloud(font_path="simhei.ttf", max_words = 50, background_color = 'white', width = 800, height = 500)    
wsWordcloud = wsWC.generate(wsWords_split)
plt.imshow(wsWordcloud)
plt.axis("off")
plt.title('华尔街见闻词云图')

#
plt.subplot(223)
jqkaNews = pd.read_csv('jqka_text.csv')
jqkaContent = jqkaNews['content'].dropna(how = 'any').values
jqkaText = ""
for q in jqkaContent:
    jqkaText += q
jqkaAnalyze = jieba.analyse.extract_tags(jqkaText, topK=50, withWeight=False, allowPOS=())
jqkaLists = " ".join(jqkaAnalyze).split(' ')
print(jqkaLists)
jqkaSegList = jieba.cut(jqkaText, cut_all=False) 
jqkaWords_split = " ".join(jqkaSegList)
jqkaWC = WordCloud(font_path="simhei.ttf", max_words = 50, background_color = 'white', width = 800, height = 500)    
jqkaWordcloud = jqkaWC.generate(jqkaWords_split)
plt.imshow(jqkaWordcloud)
plt.axis("off")
plt.title('同花顺词云图')

#
#plt.subplot(234)
#eastNews = pd.read_csv('eastmoney_text.csv')
#eastContent = eastNews['content'].dropna(how = 'any').values
#eastText = ""
#for q in eastContent:
#    eastText += q
#eastAnalyze = jieba.analyse.extract_tags(eastText, topK=50, withWeight=False, allowPOS=())
#eastLists = " ".join(eastAnalyze).split(' ')
#print(eastLists)
#eastSegList = jieba.cut(eastText, cut_all=False) 
#eastWords_split = " ".join(eastSegList)
#eastWC = WordCloud(font_path="simhei.ttf", max_words = 50, background_color = 'white', width = 800, height = 500)    
#eastWordcloud = eastWC.generate(eastWords_split)
#plt.imshow(eastWordcloud)
#plt.axis("off")
#plt.title('东方财富词云图')

#
plt.subplot(224)
ycjNews = pd.read_csv('yuncaijing_text.csv')
ycjContent = ycjNews['content'].dropna(how = 'any').values
ycjText = ""
for q in ycjContent:
    ycjText += q
ycjAnalyze = jieba.analyse.extract_tags(ycjText, topK=50, withWeight=False, allowPOS=())
ycjLists = " ".join(ycjAnalyze).split(' ')
print(ycjLists)
ycjSegList = jieba.cut(ycjText, cut_all=False) 
ycjWords_split = " ".join(ycjSegList)
ycjWC = WordCloud(font_path="simhei.ttf", max_words = 50, background_color = 'white', width = 800, height = 500)    
ycjWordcloud = ycjWC.generate(ycjWords_split)
plt.imshow(ycjWordcloud)
plt.axis("off")
plt.title('云财经词云图')

plt.savefig('词云图.png')
plt.show()
# =============================================================================





#抽取信息地雷,LDA匹配

eastNews = eastNews.dropna()
sinaData = sinaData.dropna()
sinaData = sinaData.dropna()
sinaData = sinaData.dropna()
wsData = wsNews.drop(columns=['title'])


test = eastNews[eastNews['content'].str.contains('百联股份')]
test = eastNews[eastNews['content'].str.contains('贵人鸟')]
test = wsData[wsData['content'].str.contains('贵人鸟')]












