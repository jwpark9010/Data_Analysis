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
    
def json_body(mesurement_name):

    point = {
        "measurement" : mesurement_name,
        "tags":{},
        "time":"",

        "fields":{}
    }

    return point

if __name__ =='__main__':

    # database 정보 : CS_Analysis, Hanuri_Analysis
    client = influxdb.InfluxDBClient(host='10.0.1.186', port=8086, username='cschae', password='cschae', database ='CS_ORI_DATA')
    path_dir = u'../csv_file/CS_data/ori_2018/'
    file_list = os.listdir(path_dir)

    field_list = ['LONGITUDE', 'LATITUDE', 'RPM', 'RUN_SPEED', 'DRV_DISTANCE',
                  'ODO', 'TDM', 'TTS', 'TIS', 'AFR', 'IFR',
                  'APS', 'TPS', 'CLV', 'COT', 'IAT', 'OAT', 'MAP', 'MAF',
                  'IFC', 'ALTITUDE', 'EFR', 'ATQ', 'EGR', 'EST', 'BAP']

    for file_name in file_list:
        m_name = "CS"+file_name[5:10]+"_"+file_name[0:4]

        print("%s reading...\n" %file_name)
        df = pd.read_excel(path_dir + file_name)
        df.columns = df.loc[0]
        df = df.drop(0).sort_values(by=['GPS_TIME'], axis=0).reset_index(drop=True).fillna(-1.0)
    
        df_dict = df.to_dict('records')
        
        result = []
        count = 0
        
        print("%s to InfluxDB putting...\n" %file_name)
        for i in range(len(df_dict)):
            time = df_dict[i]['GPS_TIME']

            fomat = json_body(m_name)
            for j in range(len(field_list)) :
                        
                if df_dict[i][field_list[j]] == 'NaN':
                    continue
                elif df_dict[i][field_list[j]] == 'nan':
                    continue

                fomat["tags"]['carid'] = df_dict[i]['CAR_CD']
                fomat["time"] = time
                fomat["fields"][field_list[j]] = np.float64(df_dict[i][field_list[j]])
                
                count += 1
            
                result.append(fomat)

                if count >= 2000:
                    #import pdb; pdb.set_trace()
                    client.write_points(result, time_precision='ms')
                    result = []
                    count = 0

            printProgressBar(i, len(df_dict))
            print("\t")
        if len(result) != 0:
            '''
        마지막 50개 이하로 남는 경우 나머지 전부를 put 한다.
        '''
        client.write_points(result)
        exit()

