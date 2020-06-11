
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

writer = pd.ExcelWriter('total.xlsx', engine='xlsxwriter')


def count_total(series):
    return (series.count()/20)*100
   
def count_group(series):
    return (series.count()/200)*100

queryPassport = 'SELECT * FROM passport'
queryProtocol = 'SELECT * FROM protocol WHERE eventID=4'
protocol = pd.read_sql_query(queryProtocol , conn)
passport = pd.read_sql_query(queryPassport, conn)
df = pd.merge(protocol, passport[['SID', 'point', 'object']], on='SID')
df= df[['timeStamp','eventTime','preError','postError','point','object']]
df5=df.loc[df['eventTime']<5]
df6=df.loc[df['eventTime']<5]
df.to_excel(writer, sheet_name='data', index=True)
df5.to_excel(writer, sheet_name='data<5', index=True)
df6.to_excel(writer, sheet_name='data<6', index=True)
print(df6.head)
writer.save()
print("Writing complete")
