# -*- coding:utf-8 -*-
'''
Author : JW Park, github : https://github.com/jwpark9010
Last working date : 2019/08/02 (JW Park)

'''

from __future__ import print_function
import pprint
from pymongo import MongoClient
import time
import pandas as pd
import os
import sys
import math
import xlsxwriter
import json
import requests
import influxdb

def printProgressBar(iteration, total, prefix = 'Progress', suffix = 'Complete',\
                      decimals = 1, length = 100, fill = '█'): 
    # 작업의 진행상황을 표시
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' %(prefix, bar, percent, suffix), end='\r')
    sys.stdout.flush()
    if iteration == total:
        print()
        
def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:19])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))

    return epoch
    

######################   MongoDB   #############################
def MongoDB_query(_db_name, skip_value):
    client = MongoClient('125.140.110.217:27017', username='guest', password='keti',\
            authSource='guest')
    db = client[_db_name]
    coll = db["201906"]
    Elex_all = coll.find(no_cursor_timeout=True).skip(skip_value)
    record_line = coll.count()
    return Elex_all, record_line

######################   InfluxDB   #############################
def InfluxDB_input(df, start_range, end_range, measurement_name, batch):
    client = influxdb.DataFrameClient(host='10.0.1.186', port=8086, username='jwpark', password='jwpark', database ='ELEX_ORI_DATA')
    field_list = ['RECORD_TIME', 'ENGINE_OIL_TEMPER', 'GPS_LAT', 'AMBIENT_TEMPER', 'CHANGE_GEAR_STEPS', 'PHONE_NUM',
                'TEMPER1', 'DTC', 'ACCEL_PEDAL', 'GPS_LONG', 'BATTERY_VOLTAGE',
                'TEMPER2', 'ACCEL_Y', 'DRIVE_STATUS', 'ACCEL_X', 'DRIVE_SPEED',
                'DRIVE_LENGTH_TOTAL', 'REQ_TIME', 'AMP', 'VEHICLE_TYPE', 'FUEL_CONSUM_TOTAL',
                'ORIGIN_TYPE', 'DEVICE_STATUS_CD', 'GPS_ANGLE', 'FUEL_CONSUM_DAY', 'ENGINE_TORQUE',
                'ENGINE_GAUGE', 'COOLANT_TEMPER', 'RPM', 'INTAKE_TEMPER', 'MAF', 'AIR_GAUGE', 'TIME']

    _list = []

    for i in range(start_range, end_range):
        _list.append(i)
    
    df['TIME'] = _list
    
    for i in field_list:
            df[i] = df[i].astype(str)
    
    df_dict = df.to_dict('records')

    result = pd.DataFrame(df_dict, columns=field_list)
    result = result.set_index(['TIME'])
    result.index = pd.to_datetime(result.index, unit='s')
    client.write_points(dataframe=result, batch_size = batch, measurement=measurement_name, time_precision='s')
    

if __name__ =='__main__':
    MONGO_DB_LIST = ['elex', 'hanuri', 'carssum', 'public']
    skip_value = 0
    cursor = MongoDB_query(MONGO_DB_LIST[0], skip_value)
    count = 0
    limit = 500000
    i = 0
    start = 0
    
    remain = cursor[1] % limit

    _list = []
    
    for _dict in cursor[0]:
        start_time = time.time() #시작 시간 저장
        if (len(_dict)>=33):
            i +=1   
            _list.append(_dict)
            
        count +=1
        printProgressBar(count, limit)    

        if count == limit:
            print(i)
            count = 0
            i = 0
            
            df = pd.DataFrame(_list)
            df = df.fillna("NaN")

            end = len(_list)
            end = start + end    

            InfluxDB_input(df, start, end, '201906', 1000)
            print("Influx에 누적 row수 : %d\n" %end)
                
        elif count == remain:
            print(i)
            
            df = pd.DataFrame(_list)
            df = df.fillna("NaN")

            end = len(_list)
            end = start + end    

            InfluxDB_input(df, start, end, '201906', 1000)
        
        print("Influx에 누적 row수 : %d\n" %end)    
        start = end
        _list = []
        print("Run time :", time.time() - start_time)
        print('\t')
        