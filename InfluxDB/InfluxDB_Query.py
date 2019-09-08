# -*- coding: utf-8 -*-
import influxdb
import pandas as pd
import matplotlib.pyplot as plt
import csv
import json
import 

if __name__ =='__main__':

    client = influxdb.DataFrameClient(host='10.0.1.186', port=8086, username='cschae', password='cschae', database ='ELEX_ORI_DATA')
    
    query_result = client.query('select * from "201905"')
    
    ### query_result['accel_data'] <---- 없을 시 column명이 제대로 안나옴 ###
    result = query_result['DTC'] #'select * from "decel_data"'
    print(result)
