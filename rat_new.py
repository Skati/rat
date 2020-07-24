
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
from statsmodels.sandbox.stats.multicomp import MultiComparison
import pingouin as pg
from statsmodels.stats.anova import AnovaRM
import pingouin as pg
from scipy.stats import zscore

pd.set_option('max_columns', None)
pd.options.display.float_format = '{:,.2f}'.format
conn = sqlite3.connect('./data/RMhistory.db3')
c = conn.cursor()

writer = pd.ExcelWriter('total_new.xlsx', engine='xlsxwriter')
writer1 = pd.ExcelWriter('stat.xlsx', engine='xlsxwriter')
writer5 = pd.ExcelWriter('total5.xlsx', engine='xlsxwriter')
writer6 = pd.ExcelWriter('total6.xlsx', engine='xlsxwriter')


def count_total(series):
    return (series.count()/20)*100


def count_group(series):
    return int((pd.Series.count(series)/(pd.Series.nunique(series)*20))*100)

def z_score(x):
    return lambda x: (x - x.mean()) / x.std()

query01 = 'SELECT * FROM protocol WHERE eventID=4'
query05 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<5'
query06 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<6'
query00 = 'SELECT * FROM passport'
querylast = "SELECT * FROM protocol WHERE eventID=4 AND timeStamp > '2020-06-07 00:00:00'"
protocol = pd.read_sql_query(query01, conn)
passport = pd.read_sql_query(query00, conn)
protocol5 = pd.read_sql_query(query05, conn)
protocol6 = pd.read_sql_query(query06, conn)
protocol_last = pd.read_sql_query(querylast, conn)
df = pd.merge(protocol, passport[['SID', 'point', 'object']], on='SID')
df5 = pd.merge(protocol5, passport[['SID', 'point', 'object']], on='SID')
df6 = pd.merge(protocol6, passport[['SID', 'point', 'object']], on='SID')
df_last = pd.merge(
    protocol_last, passport[['SID', 'point', 'object']], on='SID')
# print(df_last)
df['timeStamp'] = pd.to_datetime(
    df['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df5['timeStamp'] = pd.to_datetime(
    df5['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df6['timeStamp'] = pd.to_datetime(
    df6['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df_last['timeStamp'] = pd.to_datetime(
    df_last['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df['timeStamp'] = df['timeStamp'].dt.date
df['timeStamp']=df['timeStamp'].apply(lambda x: x.strftime('%Y-%m-%d'))
df5['timeStamp'] = df5['timeStamp'].dt.date
df6['timeStamp'] = df6['timeStamp'].dt.date
df_last['timeStamp'] = df_last['timeStamp'].dt.date


# grouped_multiple = df.groupby(['object', 'timeStamp']).agg({'eventTime': ['describe', count_total,z_score], 'preError': ['describe', errors], 'postError': ['describe', errors]})
grouped_multiple = df.groupby(['object', 'timeStamp']).agg({'eventTime': ['count', count_total, 'mean', 'std', 'sem', 'median'], 'preError': ['mean', 'std', 'sem', 'median', 'sum'], 'postError': ['mean', 'std', 'sem', 'median', 'sum']}).rename(columns={
    'count': u'Кол-во', 'count_total': '% достижения цели', 'eventTime': 'Время достижения цели', 'preError': 'Ошибки до вкл', 'postError': 'Ошибки после вкл', 'mean': 'Среднее знач', 'std': 'Станд. отклон.', 'mean': 'Среднее', 'median': 'Медиана', 'sum': 'Общее кол-во ошибок', 'timeStamp': 'Дата', 'object': 'Объект', 'sem': 'Ст.ош.'})
grouped_multiple5 = df5.groupby(['object', 'timeStamp']).agg({'eventTime': ['count', count_total, 'mean', 'std', 'sem', 'median'], 'preError': ['mean', 'std', 'sem', 'median', 'sum'], 'postError': ['mean', 'std', 'sem', 'median', 'sum']}).rename(columns={
    'count': u'Кол-во', 'count_total': '% достижения цели', 'eventTime': 'Время достижения цели', 'preError': 'Ошибки до вкл', 'postError': 'Ошибки после вкл', 'mean': 'Среднее знач', 'std': 'Станд. отклон.', 'mean': 'Среднее', 'median': 'Медиана', 'sum': 'Общее кол-во ошибок', 'timeStamp': 'Дата', 'object': 'Объект', 'sem': 'Ст.ош.'})
grouped_multiple6 = df6.groupby(['object', 'timeStamp']).agg({'eventTime': ['count', count_total, 'mean', 'std', 'sem', 'median'], 'preError': ['mean', 'std', 'sem', 'median', 'sum'], 'postError': ['mean', 'std', 'sem', 'median', 'sum']}).rename(columns={
    'count': u'Кол-во', 'count_total': '% достижения цели', 'eventTime': 'Время достижения цели', 'preError': 'Ошибки до вкл', 'postError': 'Ошибки после вкл', 'mean': 'Среднее знач', 'std': 'Станд. отклон.', 'mean': 'Среднее', 'median': 'Медиана', 'sum': 'Общее кол-во ошибок', 'timeStamp': 'Дата', 'object': 'Объект', 'sem': 'Ст.ош.'})


grouped_multiple.to_excel(writer, sheet_name='total',
                          index=True, float_format="%.2f")
grouped_multiple5.to_excel(
    writer, sheet_name='total<5', index=True, float_format="%.2f")
grouped_multiple6.to_excel(
    writer, sheet_name='total<6', index=True, float_format="%.2f")
#
# group = []# for obj in df['object']:
#     if int(obj) in [19, 21, 23, 32, 30, 37, 50, 24, 11, 14]:
#         group.append(1)
#     elif int(obj) in [9, 29, 3, 5, 25, 39, 41, 44, 6, 26]:
#         group.append(2)
#     elif int(obj) in [40, 47, 1, 15, 20, 35, 36, 43, 49, 2]:
#         group.append(3)
#     elif int(obj) in [8, 13, 16, 31, 34, 42, 45, 46, 38, 4]:
#         group.append(4)
#     elif int(obj) in [7, 12, 17, 22, 27, 10, 18, 28, 33, 48]:
#         group.append(5)
#     else:
#         group.append(6)
# group5 = []
# for obj in df5['object']:
#     if int(obj) in [19, 21, 23, 32, 30, 37, 50, 24, 11, 14]:
#         group5.append(1)
#     elif int(obj) in [9, 29, 3, 5, 25, 39, 41, 44, 6, 26]:
#         group5.append(2)
#     elif int(obj) in [40, 47, 1, 15, 20, 35, 36, 43, 49, 2]:
#         group5.append(3)
#     elif int(obj) in [8, 13, 16, 31, 34, 42, 45, 46, 38, 4]:
#         group5.append(4)
#     elif int(obj) in [7, 12, 17, 22, 27, 10, 18, 28, 33, 48]:
#         group5.append(5)
#     else:
#         group5.append(6)
# group6 = []
# for obj in df6['object']:
#     if int(obj) in [19, 21, 23, 32, 30, 37, 50, 24, 11, 14]:
#         group6.append(1)
#     elif int(obj) in [9, 29, 3, 5, 25, 39, 41, 44, 6, 26]:
#         group6.append(2)
#     elif int(obj) in [40, 47, 1, 15, 20, 35, 36, 43, 49, 2]:
#         group6.append(3)
#     elif int(obj) in [8, 13, 16, 31, 34, 42, 45, 46, 38, 4]:
#         group6.append(4)
#     elif int(obj) in [7, 12, 17, 22, 27, 10, 18, 28, 33, 48]:
#         group6.append(5)
#     else:
#         group6.append(6)
# df['group'] = group
# df5['group'] = group5
# df6['group'] = group6


# group_count = df.groupby(['group', 'timeStamp']).agg(
#     count_col=pd.NamedAgg(column="object", aggfunc=count_group))
# #grouped_group = df.groupby(['group', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
# grouped_group = df.groupby(['group', 'timeStamp']).agg({'object': [pd.NamedAgg(column="object", aggfunc=count_group)], 'eventTime': ['count', 'mean', 'std', 'sem', 'median'], 'preError': ['count', 'mean', 'std', 'sem', 'median', 'sum'], 'postError': ['count', 'mean', 'std', 'sem', 'median', 'sum']}).rename(columns={
#     'count': u'Кол-во', 'count_total': '% достижений', 'eventTime': 'Время достижения цели', 'preError': 'Ошибки до вкл', 'postError': 'Ошибки после вкл', 'mean': 'Среднее знач', 'std': 'Станд. отклон.', 'mean': 'Среднее', 'median': 'Медиана', 'sum': 'Общее кол-во ошибок', 'group': 'Группа', 'object': '% достижения цели', 'timeStamp': 'Дата', 'sem': 'Ст.ош.'})
# grouped_group.to_excel(writer, sheet_name='group by date',
#                        index=True, float_format="%.2f")
# #grouped_group5 = df5.groupby(['group', 'timeStamp']).agg({'eventTime': ['describe', count_group], 'preError': ['describe', errors_group], 'postError': ['describe', errors_group]})
# grouped_group5 = df5.groupby(['group', 'timeStamp']).agg({'object': [pd.NamedAgg(column="object", aggfunc=count_group)], 'eventTime': ['count', 'mean', 'std', 'sem', 'median'], 'preError': ['count', 'mean', 'std', 'sem', 'median', 'sum'], 'postError': ['count', 'mean', 'std', 'sem', 'median', 'sum']}).rename(columns={
#     'count': u'Кол-во', 'count_total': '% достижений', 'eventTime': 'Время достижения цели', 'preError': 'Ошибки до вкл', 'postError': 'Ошибки после вкл', 'mean': 'Среднее знач', 'std': 'Станд. отклон.', 'mean': 'Среднее', 'median': 'Медиана', 'sum': 'Общее кол-во ошибок', 'group': 'Группа', 'object': '% достижения цели', 'timeStamp': 'Дата', 'sem': 'Ст.ош.'})
# grouped_group5.to_excel(writer, sheet_name='group by date<5',
#                         index=True, float_format="%.2f")
# #grouped_group6 = df6.groupby(['group', 'timeStamp']).agg({'eventTime': ['describe', count_group], 'preError': ['describe', errors_group], 'postError': ['describe', errors_group]})
# grouped_group6 = df6.groupby(['group', 'timeStamp']).agg({'object': [pd.NamedAgg(column="object", aggfunc=count_group)], 'eventTime': ['count', 'mean', 'std', 'sem', 'median'], 'preError': ['count', 'mean', 'std', 'sem', 'median', 'sum'], 'postError': ['count', 'mean', 'std', 'sem', 'median', 'sum']}).rename(columns={
#     'count': u'Кол-во', 'count_total': '% достижений', 'eventTime': 'Время достижения цели', 'preError': 'Ошибки до вкл', 'postError': 'Ошибки после вкл', 'mean': 'Среднее знач', 'std': 'Станд. отклон.', 'mean': 'Среднее', 'median': 'Медиана', 'sum': 'Общее кол-во ошибок', 'group': 'Группа', 'object': '% достижения цели', 'timeStamp': 'Дата', 'sem': 'Ст.ош.'})
# grouped_group6.to_excel(
#     writer, sheet_name='group by date <6', index=True, float_format="%.2f")
# # print(df)
df.to_excel(writer, sheet_name='data', index=True, float_format="%.2f")


#zscores=df.groupby(['object', 'timeStamp'])[['eventTime','preError','postError']].apply(lambda x: (x - x.mean())/x.std())
#zscores=df.groupby(['object', 'timeStamp'],group_keys=True,as_index=True)[['eventTime','preError','postError']].transform(lambda x : zscore(x,ddof=1))

# zscores=df[['object', 'timeStamp','eventTime','preError','postError']]
# zscores[['eventTime','preError','postError']]=zscores[['eventTime','preError','postError']].transform(lambda x : zscore(x,ddof=1)).apply(abs)

# print(zscores)
# zscores.to_excel(writer, sheet_name='z_scores', index=True)
# zscores=zscores.groupby(['object', 'timeStamp']).mean()
# zscores.to_excel(writer, sheet_name='z_scores_mean', index=True)
# df_group_1 = df.loc[df['group']==1]
# df_group_2 = df.loc[df['group']==2]
# df_group_3 = df.loc[df['group']==3]
# df_group_4 = df.loc[df['group']==4]
# df_group_5 = df.loc[df['group']==5]

# df['Subject']= df.index
# df = df.astype({'point': int,'object': int,'timeStamp':str})
# anova1=pg.pairwise_ttests(dv='eventTime',between=['timeStamp'],data=df,alpha=0.05 )
# anova2=pg.pairwise_ttests(dv='eventTime',between=['timeStamp'],data=df_group_2,alpha=0.05 )
# anova3=pg.pairwise_ttests(dv='eventTime',between=['timeStamp'],data=df_group_3,alpha=0.05 )
# anova4=pg.pairwise_ttests(dv='eventTime',between=['timeStamp'],data=df_group_4,alpha=0.05 )
# anova5=pg.pairwise_ttests(dv='eventTime',between=['timeStamp'],data=df_group_5,alpha=0.05 )
# anova=pg.pairwise_ttests(dv='eventTime',between=['point'],data=df,alpha=0.05 )

# aov = pg.rm_anova(dv='eventTime', within='object',subject='Subject',  data=df)
# anova1.to_excel(writer, sheet_name='stat 1', index=True)
# anova2.to_excel(writer, sheet_name='stat 2', index=True)
# anova3.to_excel(writer, sheet_name='stat 3', index=True)
# anova4.to_excel(writer, sheet_name='stat 4', index=True)
# anova5.to_excel(writer, sheet_name='stat 5', index=True)
# anova.to_excel(writer, sheet_name='stat by group', index=True)
    


# aov.to_excel(
#     writer, sheet_name='statistics', index=True)
writer.save()



print("Writing complete")
