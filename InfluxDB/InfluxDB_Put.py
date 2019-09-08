# -*- coding: utf-8 -*-

"""
    Author : JW Park / github : https://github.com/jwpark9010
    Last working date : 2019/07/10 (JW Park)
"""

import influxdb
import pandas as pd
import matplotlib.pyplot as plt
import csv
import json

def json_body():
    point = {
        "measurement" : "decel_data",
        "tags":{},
        "time":"",

        "fields":{}
    }

    return point

if __name__ =='__main__':

    # database 정보 : CS_Analysis, Hanuri_Analysis
    client = influxdb.InfluxDBClient(host='10.0.1.186', port=8086, username='cschae', password='cschae', database ='CS_Analysis')
    
    df = pd.read_excel("accel_and_decel_count_add_gps_info_test.xlsx", sheet_name="decel_data")
    
    field_list = ['gps_lat', 'gps_long', 'speed', 'time_state']

    df_dict = df.to_dict('records')
 
    result = []
    for i in range(len(df_dict)):

        time = df_dict[i]['time']
        fomat = json_body()
        for j in range(len(field_list)) :
        
            fomat["tags"]['carid'] = df_dict[i]['carid']
            fomat["time"] = time
            fomat["fields"][field_list[j]] = df_dict[i][field_list[j]]

        result.append(fomat)
    
    client.write_points(result)
    
    query_result = client.query('select * from "decel_data"')
    print(query_result)
