
# -*- coding: utf-8 -*-
import csv
import datetime
import sqlite3
import dateutil.parser
import pandas as pd
import xlsxwriter
from pandas import Series, Timestamp
import numpy as np
import matplotlib.pyplot as plt
import researchpy as rp
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import pingouin as pg
from scipy.stats import ttest_ind

pd.set_option('max_columns', None)

conn = sqlite3.connect('./data/RMhistory.db3')
c = conn.cursor()

writer = pd.ExcelWriter('total_new.xlsx', engine='xlsxwriter')
writer1 = pd.ExcelWriter('stat.xlsx', engine='xlsxwriter')
writer5 = pd.ExcelWriter('total5.xlsx', engine='xlsxwriter')
writer6 = pd.ExcelWriter('total6.xlsx', engine='xlsxwriter')
def count_total(series):
    return (series.count()/20)*100

    
def count_group(series):
    return  int((pd.Series.count(series)/(pd.Series.nunique(series)*20))*100)


query01 = 'SELECT * FROM protocol WHERE eventID=4'
query05 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<5'
query06 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<6'
query00 = 'SELECT * FROM passport'
querylast="SELECT * FROM protocol WHERE eventID=4 AND timeStamp > '2020-06-07 00:00:00'"
protocol = pd.read_sql_query(query01, conn)
passport = pd.read_sql_query(query00, conn)
protocol5 = pd.read_sql_query(query05, conn)
protocol6 = pd.read_sql_query(query06, conn)
protocol_last = pd.read_sql_query(querylast, conn)
df = pd.merge(protocol, passport[['SID', 'point', 'object']], on='SID')
df5 = pd.merge(protocol5, passport[['SID', 'point', 'object']], on='SID')
df6 = pd.merge(protocol6, passport[['SID', 'point', 'object']], on='SID')
df_last = pd.merge(protocol_last, passport[['SID', 'point', 'object']], on='SID')
#print(df_last)
df['timeStamp'] = pd.to_datetime(df['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df5['timeStamp'] = pd.to_datetime(df5['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df6['timeStamp'] = pd.to_datetime(df6['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df_last['timeStamp'] = pd.to_datetime(df_last['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df['timeStamp'] = df['timeStamp'].dt.date
df5['timeStamp'] = df5['timeStamp'].dt.date
df6['timeStamp'] = df6['timeStamp'].dt.date
df_last['timeStamp'] = df_last['timeStamp'].dt.date


# grouped_multiple = df.groupby(['object', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
grouped_multiple = df.groupby(['object', 'timeStamp']).agg({'eventTime': ['count', count_total,'mean','std','sem','median'], 'preError': ['count','mean','std','sem','median','sum'], 'postError': ['count','mean','std','sem','median','sum']}).rename(columns={'count':u'Кол-во','count_total' : '% достижения цели','eventTime' : 'Время достижения цели','preError' : 'Ошибки до вкл','postError' : 'Ошибки после вкл','mean' : 'Среднее знач','std' : 'Станд. отклон.','mean' : 'Среднее','median' : 'Медиана','sum' : 'Общее кол-во ошибок','timeStamp' : 'Дата','object' : 'Объект','sem' : 'Ст.ош.'})
grouped_multiple5 = df5.groupby(['object', 'timeStamp']).agg({'eventTime': ['count', count_total,'mean','std','sem','median'], 'preError': ['count','mean','std','sem','median','sum'], 'postError': ['count','mean','std','sem','median','sum']}).rename(columns={'count':u'Кол-во','count_total' : '% достижения цели','eventTime' : 'Время достижения цели','preError' : 'Ошибки до вкл','postError' : 'Ошибки после вкл','mean' : 'Среднее знач','std' : 'Станд. отклон.','mean' : 'Среднее','median' : 'Медиана','sum' : 'Общее кол-во ошибок','timeStamp' : 'Дата','object' : 'Объект','sem' : 'Ст.ош.'})
grouped_multiple6 = df6.groupby(['object', 'timeStamp']).agg({'eventTime': ['count', count_total,'mean','std','sem','median'], 'preError': ['count','mean','std','sem','median','sum'], 'postError': ['count','mean','std','sem','median','sum']}).rename(columns={'count':u'Кол-во','count_total' : '% достижения цели','eventTime' : 'Время достижения цели','preError' : 'Ошибки до вкл','postError' : 'Ошибки после вкл','mean' : 'Среднее знач','std' : 'Станд. отклон.','mean' : 'Среднее','median' : 'Медиана','sum' : 'Общее кол-во ошибок','timeStamp' : 'Дата','object' : 'Объект','sem' : 'Ст.ош.'})

#grouped_multiple5 = df5.groupby(['object', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
#grouped_multiple6 = df6.groupby(['object', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
#stat=rp.summary_cont(df.groupby(['object', 'timeStamp'])['eventTime'])
# grouped=df.groupby(['object', 'timeStamp'])




grouped_multiple.to_excel(writer, sheet_name='total', index=True)
grouped_multiple5.to_excel(writer, sheet_name='total<5', index=True)
grouped_multiple6.to_excel(writer, sheet_name='total<6', index=True)

# 
group=[]
for obj in df['object']:
    if int(obj) in [19,21,23,32,30,37,50,24,11,14]:
        group.append(1)
    elif int(obj) in [9,29,3,5,25,39,41,44,6,26]:
        group.append(2)
    elif int(obj) in [40,47,1,15,20,35,36,43,49,2]:
        group.append(3)
    elif int(obj) in [8,13,16,31,34,42,45,46,38,4]:
        group.append(4)
    elif int(obj) in [7,12,17,22,27,10,18,28,33,48]:
        group.append(5)
    else:
        group.append(6)
group5=[]
for obj in df5['object']:
    if int(obj) in [19,21,23,32,30,37,50,24,11,14]:
        group5.append(1)
    elif int(obj) in [9,29,3,5,25,39,41,44,6,26]:
        group5.append(2)
    elif int(obj) in [40,47,1,15,20,35,36,43,49,2]:
        group5.append(3)
    elif int(obj) in [8,13,16,31,34,42,45,46,38,4]:
        group5.append(4)
    elif int(obj) in [7,12,17,22,27,10,18,28,33,48]:
        group5.append(5)
    else:
        group5.append(6)
group6=[]
for obj in df6['object']:
    if int(obj) in [19,21,23,32,30,37,50,24,11,14]:
        group6.append(1)
    elif int(obj) in [9,29,3,5,25,39,41,44,6,26]:
        group6.append(2)
    elif int(obj) in [40,47,1,15,20,35,36,43,49,2]:
        group6.append(3)
    elif int(obj) in [8,13,16,31,34,42,45,46,38,4]:
        group6.append(4)
    elif int(obj) in [7,12,17,22,27,10,18,28,33,48]:
        group6.append(5)
    else:
        group6.append(6)
df['group'] = group     
df5['group'] = group5 
df6['group'] = group6 

group_count=df.groupby(['group', 'timeStamp']).agg(count_col=pd.NamedAgg(column="object", aggfunc= count_group))
#grouped_group = df.groupby(['group', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
grouped_group = df.groupby(['group', 'timeStamp']).agg({'object': [pd.NamedAgg(column="object", aggfunc=count_group)],'eventTime': ['count', 'mean','std','sem','median'], 'preError': ['count','mean','std','sem','median','sum'], 'postError': ['count','mean','std','sem','median','sum']}).rename(columns={'count':u'Кол-во','count_total' : '% достижений','eventTime' : 'Время достижения цели','preError' : 'Ошибки до вкл','postError' : 'Ошибки после вкл','mean' : 'Среднее знач','std' : 'Станд. отклон.','mean' : 'Среднее','median' : 'Медиана','sum' : 'Общее кол-во ошибок','group' : 'Группа','object' : '% достижения цели','timeStamp' : 'Дата','sem' : 'Ст.ош.'})
grouped_group.to_excel(writer, sheet_name='group by date', index=True)
#grouped_group5 = df5.groupby(['group', 'timeStamp']).agg({'eventTime': ['describe', count_group], 'preError': ['describe', errors_group], 'postError': ['describe', errors_group]})
grouped_group5 = df5.groupby(['group', 'timeStamp']).agg({'object': [pd.NamedAgg(column="object", aggfunc=count_group)],'eventTime': ['count', 'mean','std','sem','median'], 'preError': ['count','mean','std','sem','median','sum'], 'postError': ['count','mean','std','sem','median','sum']}).rename(columns={'count':u'Кол-во','count_total' : '% достижений','eventTime' : 'Время достижения цели','preError' : 'Ошибки до вкл','postError' : 'Ошибки после вкл','mean' : 'Среднее знач','std' : 'Станд. отклон.','mean' : 'Среднее','median' : 'Медиана','sum' : 'Общее кол-во ошибок','group' : 'Группа','object' : '% достижения цели','timeStamp' : 'Дата','sem' : 'Ст.ош.'})
grouped_group5.to_excel(writer, sheet_name='group by date<5', index=True)
#grouped_group6 = df6.groupby(['group', 'timeStamp']).agg({'eventTime': ['describe', count_group], 'preError': ['describe', errors_group], 'postError': ['describe', errors_group]})
grouped_group6 = df6.groupby(['group', 'timeStamp']).agg({'object': [pd.NamedAgg(column="object", aggfunc=count_group)],'eventTime': ['count', 'mean','std','sem','median'], 'preError': ['count','mean','std','sem','median','sum'], 'postError': ['count','mean','std','sem','median','sum']}).rename(columns={'count':u'Кол-во','count_total' : '% достижений','eventTime' : 'Время достижения цели','preError' : 'Ошибки до вкл','postError' : 'Ошибки после вкл','mean' : 'Среднее знач','std' : 'Станд. отклон.','mean' : 'Среднее','median' : 'Медиана','sum' : 'Общее кол-во ошибок','group' : 'Группа','object' : '% достижения цели','timeStamp' : 'Дата','sem' : 'Ст.ош.'})
grouped_group6.to_excel(writer, sheet_name='group by date <6', index=True)
# print(df)
df.to_excel(writer, sheet_name='data', index=True)
group0 = df.groupby(['group', 'timeStamp'])['eventTime']
group_list=[]
# for g in group0.groups.keys():
#     group = group0.get_group(g)
#     group_list.append(g)
#     print(group)
# print(group_list)





tukey = pairwise_tukeyhsd(endog=df['eventTime'],     
                          groups=df['group'],  
                          alpha=0.05)
resultFile = open("table.csv",'w')
resultFile.write(tukey.summary().as_csv())
resultFile.close()
tukey_res = pd.read_csv("table.csv")
df_last.to_excel(writer, sheet_name='data_after 060620', index=True)
tukey_res.to_excel(writer, sheet_name='group statistics', index=True)
# group diff by last days

tukey1 = pairwise_tukeyhsd(endog=df_last['eventTime'],     
                          groups=df_last['point'],  
                          alpha=0.05)
resultFile1 = open("table1.csv",'w')
resultFile1.write(tukey1.summary().as_csv())
resultFile1.close()
tukey_res1 = pd.read_csv("table1.csv")
tukey_res1.to_excel(writer, sheet_name='group statistics after 060620', index=True)

#ttest=ttest_ind(*df.groupby('object')['timeStamp'].apply(lambda x:list(x)))
#print(ttest)
writer.save()
#writer1.save()


print("Writing complete")
