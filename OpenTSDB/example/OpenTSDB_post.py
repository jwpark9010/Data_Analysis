# -*- coding: utf-8 -*- 
import requests
import time
import json
from collections import OrderedDict
import csv
import pandas as pd

url ="__"
headers = {'content-type'   :   'application/json'}

''' 
### OpenTSDB는 형태로 put 시켜야함 ###
cv_data_prac =  {
                    'metric' : 'cv_data',
                    'timestamp' : '14',
                    'value' : '0',
                    'tags' : {}
                }
'''


def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[-8:-6], _time[-5:-3], _time[-2:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))

    return epoch

def put_data(_ts, _value, _tags, _carID):
    cv_data_prac = dict()
    cv_data_prac['metric'] = 'cv_data'
    cv_data_prac["tags"] = dict()
    cv_data_prac['timestamp'] = _ts
    cv_data_prac["value"] = _value
    cv_data_prac["tags"]['content'] = _tags
    cv_data_prac["tags"]['CarID'] = _carID[-3:]

    return cv_data_prac


f = open('0000000021-20140901.csv', 'r')
rdr = csv.reader(f)
df = pd.DataFrame(rdr, columns=["SET_ID", "Data_time", "D_dist", "A_dist", "GPS_dist", "Speed", "rpm", "GPS_l", "GPS_h", "gps_a", "Accel_x", "Accel_y", "D_fuel", "A_fuel", "Status_code", "Brake_signal"])
df_drop = df.drop(index=0, axis=1)
f.close()

df_dict = df_drop.to_dict('records')

sess = requests.Session()

list_ = []
name = ["D_dist", "A_dist", "GPS_dist", "Speed", "rpm", "GPS_l", "GPS_h", "gps_a", "Accel_x", "Accel_y", "D_fuel", "A_fuel", "Status_code", "Brake_signal"]


count = 0

for height in range(len(df_dict)):

    ts = convertTimeToEpoch(df_drop['Data_time'].iloc[height])
    carID = df_drop["SET_ID"].iloc[height]
    
    for width in name:

        value = df_drop[width].iloc[height]
        list_.append(put_data(ts, value, width, carID))
        

        if len(list_) == 42:
    
            try:
                response = sess.post(url, data=json.dumps(list_), headers = headers)

                while response.status_code > 204:
                    print "[Bad Request] Query status: %s" % (response.status_code)
                    print "[Bad Request] we got bad request, Query will be restarted after 3 sec!\n"
                    time.sleep(3)
                    
                    print "[Querying]" + json.dumps(list_, ensure_ascii=False, indent=4)
                    response = sess.post(url, data=json.dumps(list_), headers = headers)

                print "[Query finish and out]"

            except Exception as e:
                print "[exception] : %s" % (e)
        
            list_=[]
            count +=1
            print(count)

list_2_json = json.dumps(list_, ensure_ascii=False, indent=4)
response = sess.post(url, list_2_json)
count +=1
print(count)