#-*- coding: utf-8 -*-
from __future__ import print_function #코드의 가장 상단 부분에 위치해야 돌아감.
import requests
import time
import json
from collections import OrderedDict
import pandas as pd
import sys

def Data_file():
    df = pd.read_excel('00513_accel.xlsx', sheet_name='accel_data')

    return df

def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[-8:-6], _time[-5:-3], _time[-2:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))

    return epoch


def put_data(_list, __opentsdb_url):
    headers = { 'content-type' : 'application/json' }
    sess = requests.Session()

    url = __opentsdb_url+str('/api/put')
    

    try:
        response = sess.post(url, data=json.dumps(_list), headers = headers)

        while response.status_code > 204:
            print("[Bad Request] Put status: %s" % (response.status_code))
            print("[Bad Request] we got bad request, Put will be restarted after 3 sec!\n")
            time.sleep(3)
            
            print("[Put]" + json.dumps(_list, ensure_ascii=False, indent=4))
            response = sess.post(url, data=json.dumps(_list), headers = headers)

            print("[Put finish and out]")

    except Exception as e:
        print("[exception] : %s" % (e))

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

def put_data_form(data_file, __opentsdb_url, metric_name):
    data_file = data_file.fillna(value='NoneValue') # 엑셀에서 비어있는 칸은 value가 NaN이기 때문에 처리하기 쉽게 텍스트로 바꾼다

   # tag_list = ['carid', 'gps_lat', 'gps_long', 'speed', 'time', 'time_state']
    tag_list = ['gps_lat', 'gps_long', 'speed']

    data_len = len(data_file)
    _buffer = []
    count = 0

    for i in range(len(data_file)):
        ts = convertTimeToEpoch(str(data_file["time"].iloc[i]))

        for j in range(len(tag_list)):

            value = data_file[tag_list[j]][i]
            
            # value가 NaN인 칸들은 그냥 스킵
            if value == 'NoneValue':
                continue

            hansol_data_prac = dict()
            hansol_data_prac['metric'] = metric_name
            hansol_data_prac["tags"] = dict()
            hansol_data_prac['timestamp'] = ts
            hansol_data_prac["value"] = value

            hansol_data_prac["tags"]['VEHICLE_NUM'] = str(data_file['carid'].iloc[0])
            hansol_data_prac["tags"]['content'] = tag_list[j]

            count +=  1
            _buffer.append(hansol_data_prac)
            
            if count >= 50:
                put_data(_buffer, __opentsdb_url)
                _buffer = []
                count = 0
    
        printProgressBar(i , data_len)

    if len(_buffer) != 0:
        '''
        마지막 50개 이하로 남는 경우 나머지 전부를 put 한다.
        '''
        put_data(_buffer, __opentsdb_url)


if __name__ == "__main__":

    opentsdb_url = "http://125.140.110.217:64242"
    metric = 'hansol_data' 

    read_df = Data_file()
    #print(read_df.head())
    put_data_form(read_df, opentsdb_url, metric)