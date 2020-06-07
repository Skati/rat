#!/usr/bin/env python3
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

conn = sqlite3.connect('./data/RMhistory.db3')
c = conn.cursor()

writer = pd.ExcelWriter('total_new.xlsx', engine='xlsxwriter')
writer1 = pd.ExcelWriter('stat.xlsx', engine='xlsxwriter')
writer5 = pd.ExcelWriter('total5.xlsx', engine='xlsxwriter')
writer6 = pd.ExcelWriter('total6.xlsx', engine='xlsxwriter')
def count_total(series):
    return (series.count()/20)*100

def errors(series):
    return (np.count_nonzero(series)/20)*100

query01 = 'SELECT * FROM protocol WHERE eventID=4'
query05 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<5'
query06 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<6'
query00 = 'SELECT * FROM passport'
protocol = pd.read_sql_query(query01, conn)
passport = pd.read_sql_query(query00, conn)
protocol5 = pd.read_sql_query(query05, conn)
protocol6 = pd.read_sql_query(query06, conn)
df = pd.merge(protocol, passport[['SID', 'point', 'object']], on='SID')
df5 = pd.merge(protocol5, passport[['SID', 'point', 'object']], on='SID')
df6 = pd.merge(protocol6, passport[['SID', 'point', 'object']], on='SID')
df['timeStamp'] = pd.to_datetime(df['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df5['timeStamp'] = pd.to_datetime(df5['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df6['timeStamp'] = pd.to_datetime(df6['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
df['timeStamp'] = df['timeStamp'].dt.date
df5['timeStamp'] = df5['timeStamp'].dt.date
df6['timeStamp'] = df6['timeStamp'].dt.date


grouped_multiple = df.groupby(['object', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
grouped_multiple5 = df5.groupby(['object', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
grouped_multiple6 = df6.groupby(['object', 'timeStamp']).agg({'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
#stat=rp.summary_cont(df.groupby(['object', 'timeStamp'])['eventTime'])
print()
tukey = pairwise_tukeyhsd(endog=df['eventTime'],     # Data
                          groups=df['object'],   # Groups
                          alpha=0.05)
#print(tukey)
resultFile = open("table.csv",'w')
resultFile.write(tukey.summary().as_csv())
resultFile.close()

tukey_res = pd.read_csv("table.csv")
grouped_multiple.to_excel(writer, sheet_name='total', index=True)
grouped_multiple5.to_excel(writer, sheet_name='total<5', index=True)
grouped_multiple6.to_excel(writer, sheet_name='total<6', index=True)
#tukey_res.to_excel(writer, sheet_name='group statistics', index=True)

#data_groups = [df.groupby('object')['timeStamp'].get_group(g).to_numpy() for g in groups]
stat=pg.anova(data=df, dv='eventTime', between='object')
#stat=pg.homoscedasticity(data_groups)
print(stat)
#
writer.save()
#writer1.save()


print("Writing complete")
