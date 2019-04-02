# -*- coding: utf-8 -*-

import influxdb
import pandas as pd
import matplotlib.pyplot as plt
import json
import pprint



### 1step : DB connetion ###
client = influxdb.InfluxDBClient(host='10.0.1.186', port=8086, username='cschae', password='cschae')
client.switch_database('cv_data')

### 2step : data query & organizing data ###
result = client.query('select * from "sheet1"')

query_data = list(result.get_points(measurement='sheet1'))
print(len(query_data))

df = pd.DataFrame(query_data, columns=["SET_ID", "Data_time", "D_dist", "A_dist", "GPS_dist", "Speed", "rpm", "GPS_l", "GPS_h", "gps_a", "Accel_x", "Accel_y", "D_fuel", "A_fuel", "Status_code", "Brake_signal"])
print(df)
