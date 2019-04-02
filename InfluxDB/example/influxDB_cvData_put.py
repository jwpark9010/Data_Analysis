# -*- coding: utf-8 -*-

import influxdb
import pandas as pd
import matplotlib.pyplot as plt
import csv
import json

### influxDB 접속 ###
client = influxdb.InfluxDBClient(host='10.0.1.186', port=8086, username='cschae', password='cschae')
client.switch_database('cv_data')

client.drop_measurement('sheet1')
### cv_data file을 DataFrame으로 가져오기(불필요 항목 제거하였음) ###
f = open('0000000021-20140901.csv', 'r')
rdr = csv.reader(f)
df = pd.DataFrame(rdr, columns=["SET_ID", "Data_time", "D_dist", "A_dist", "GPS_dist", "Speed", "rpm", "GPS_l", "GPS_h", "gps_a", "Accel_x", "Accel_y", "D_fuel", "A_fuel", "Status_code", "Brake_signal"])
df_drop = df.drop(index=0, axis=1)


### InfluxDB는 list안에 dict이 있는 형태로 값을 받음 ###
df_dict = df_drop.to_dict('records')

result = []

def reset_point():
    point = {
        "measurement" : "sheet1",
        "tags":{

        },
        "time": "2019-04-01T09:57:00Z",

        "fields":{
            "fld1" : 0
        }
    }

    return point

count = 0

for i in df_dict :

    count = count + 1
    point = reset_point()
    point["tags"] = i
    point["time"] = i['Data_time']
    result.append(point)
    
client.write_points(result)

result = client.query('select * from "sheet1"')
