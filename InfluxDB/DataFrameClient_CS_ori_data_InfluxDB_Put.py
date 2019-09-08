# -*- coding: utf-8 -*-

"""
    Author : JW Park / github : https://github.com/jwpark9010
    Last working date : 2019/07/17 (JW Park)
"""
from __future__ import print_function #코드의 가장 상단 부분에 위치해야 돌아감.
import influxdb
import pandas as pd
import matplotlib.pyplot as plt
import csv
import sys
import json
import time
import os
import numpy as np

def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))

    return epoch

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

if __name__ =='__main__':

    # database 정보 : CS_Analysis, Hanuri_Analysis
    client = influxdb.DataFrameClient(host='10.0.1.186', port=8086, username='cschae', password='cschae', database ='CS_ORI_DATA')
    path_dir = u'../csv_file/CS_data/test/'
    file_list = os.listdir(path_dir)

    field_list = ['CAR_CD', 'LONGITUDE', 'LATITUDE', 'RPM', 'RUN_SPEED', 'DRV_DISTANCE',
                  'ODO', 'TDM', 'TTS', 'TIS', 'AFR', 'IFR',
                  'APS', 'TPS', 'CLV', 'COT', 'IAT', 'OAT', 'MAP', 'MAF',
                  'IFC', 'ALTITUDE', 'EFR', 'ATQ', 'EGR', 'EST', 'BAP']
    
    for file_name in file_list:
        m_name = "CS"+file_name[5:10]+"_"+file_name[0:4]

        print("%s reading...\n" %file_name)
        df = pd.read_excel(path_dir + file_name)
        df.columns = df.loc[0]
        df = df.drop(0).sort_values(by=['GPS_TIME'], axis=0).reset_index(drop=True).fillna(-1.0)
        
        _list = []
        for i in range(len(df['GPS_TIME'])):
            value = convertTimeToEpoch(df['GPS_TIME'][i])
            _list.append(value)
        
        df['TIMESTAMP'] = _list
        
        for i in field_list:
            df[i] = df[i].astype(float)

        df_dict = df.to_dict('records')
        result = pd.DataFrame(df_dict, index=df['TIMESTAMP'], columns=field_list)

        result.index = pd.to_datetime(result.index, unit='s')
        #import pdb; pdb.set_trace()
        
        client.write_points(dataframe=result, batch_size = 10000, measurement=m_name, time_precision='s')
        