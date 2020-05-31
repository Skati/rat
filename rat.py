#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import dateutil.parser
import csv
import pandas as pd
conn = sqlite3.connect('RMhistory.db3')
c = conn.cursor()

with open('rat.csv', 'w') as csvfile:
    fieldnames = [u'Время', u'Лабиринт', u'Сессия', u'Цикл', u'Событие', u'Event_ID',
                  'Event_Time', 'Current_Trap', u'Целевой рукав', u'Ошибки до вкл', u'Ошибки после вкл']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in c.execute('SELECT * FROM protocol WHERE eventID=4'):
        d = dateutil.parser.parse(row[1])
        a = d.strftime('%d/%m/%Y %H:%M:%S')
        writer.writerow({u'Время': a, u'Лабиринт': row[2], 'Сессия': row[3], u'Цикл': row[4], 'Событие': row[5], 'Event_ID': row[6],
                         'Event_Time': row[7], 'Current_Trap': row[8], u'Целевой рукав': row[9], u'Ошибки до вкл': row[10], u'Ошибки после вкл': row[11]})

read_file = pd.read_csv('rat.csv')
read_file.to_excel('rat.xlsx', index=None, header=True)
print("Writing complete")
