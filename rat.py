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
conn = sqlite3.connect('./data/RMhistory.db3')
c = conn.cursor()

# with open('rat.csv', 'w', encoding='utf-8', errors='ignore') as csvfile:
#     fieldnames = [u'Время', u'Лабиринт', u'Сессия', u'Цикл', u'№ крысы', u'№ точки',
#                   'Event_Time', u'Ошибки до вкл', u'Ошибки после вкл','Event step','Event id']
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#     writer.writeheader()

#     for row in c.execute('SELECT * FROM protocol INNER JOIN passport ON passport.SID = protocol.SID WHERE eventID=4 GROUP BY object'):
#         d = dateutil.parser.parse(row[1])
#         a = d.strftime('%d/%m/%Y %H:%M:%S')
#         writer.writerow({u'Время': a, u'Лабиринт': row[2], 'Сессия': row[3], u'Цикл': row[4], u'№ крысы': row[15], u'№ точки': row[14], 'Event_Time': row[7], u'Ошибки до вкл': row[10], u'Ошибки после вкл': row[11],'Event step':row[5],'Event id':row[6]})

# read_file = pd.read_csv('rat.csv')
#read_file.to_excel('rat.xlsx', index=None, header=True)
writer = pd.ExcelWriter('total.xlsx', engine='xlsxwriter')
writer1 = pd.ExcelWriter('stat.xlsx', engine='xlsxwriter')
writer5 = pd.ExcelWriter('total5.xlsx', engine='xlsxwriter')
writer6 = pd.ExcelWriter('total6.xlsx', engine='xlsxwriter')
for rat_num in range(1, 53):
    # query = 'SELECT * FROM protocol INNER JOIN passport ON passport.SID = protocol.SID WHERE eventID=4 and object={}'.format(rat_num)
    query01 = 'SELECT * FROM protocol WHERE eventID=4'
    query05 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<5'
    query06 = 'SELECT * FROM protocol WHERE eventID=4 AND eventTime<6'
    query00 = 'SELECT * FROM passport WHERE object={}'.format(rat_num)
    protocol = pd.read_sql_query(query01, conn)
    passport = pd.read_sql_query(query00, conn)
    protocol5 = pd.read_sql_query(query05, conn)
    protocol6 = pd.read_sql_query(query06, conn)
    df = pd.merge(protocol, passport[['SID', 'point', 'object']], on='SID')
    df5 = pd.merge(protocol5, passport[['SID', 'point', 'object']], on='SID')
    df6 = pd.merge(protocol6, passport[['SID', 'point', 'object']], on='SID')
    df['timeStamp'] = pd.to_datetime(
        df['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
    df5['timeStamp'] = pd.to_datetime(
        df5['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
    df6['timeStamp'] = pd.to_datetime(
        df6['timeStamp'], format='%Y-%m-%d %H:%M:%S.%f')
    df['timeStamp'] = df['timeStamp'].dt.date
    df5['timeStamp'] = df5['timeStamp'].dt.date
    df6['timeStamp'] = df6['timeStamp'].dt.date
    
    def count_total(series):
        return (series.count()/20)*100

    def errors(series):
        return (np.count_nonzero(series)/20)*100
    mean = df.groupby('timeStamp').agg(
        {'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
    fig, axes = plt.subplots(figsize=(12,4), sharey=True)
    plot = df.groupby('timeStamp').plot(kind='hist')
    
    plt.show()
    try:
        mean5 = df5.groupby('timeStamp').agg(
            {'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
    except AttributeError:
        print('lol')
        mean5 = 'error'
    try:
        mean6 = df6.groupby('timeStamp').agg(
            {'eventTime': ['describe', count_total], 'preError': ['describe', errors], 'postError': ['describe', errors]})
    except AttributeError:
        print('lol')
        mean6 = 'error'
    # print(mean6)
    df.to_excel(writer, sheet_name=str(rat_num), index=True)
    df5.to_excel(writer5, sheet_name=str(rat_num), index=True)
    df6.to_excel(writer6, sheet_name=str(rat_num), index=True)
    mean.to_excel(writer1, sheet_name=str(rat_num), index=True)

    try:
        mean5.to_excel(writer1, sheet_name=str(
            rat_num), index=True, startrow=15)
    except AttributeError:
        print("There's no item with that code")
    try:
        mean6.to_excel(writer1, sheet_name=str(
            rat_num), index=True, startrow=30)
    except AttributeError:
        print("There's no item with that code")
    # workbook = writer.book
    # worksheet = writer.sheets[str(rat_num)]

    # chart = workbook.add_chart({'type': 'column'})


    # chart.add_series({'name': 'Service Sales',
    #               'categories': [str(rat_num), 0, 4, 8, 4],
    #               'values':     [str(rat_num), 1, 4, 8, 4]})

    # worksheet.insert_chart('B50', chart)
  
writer.save()
writer1.save()
writer5.save()
writer6.save()

# print(df)
# mean.to_excel('mean.xlsx')
print("Writing complete")
