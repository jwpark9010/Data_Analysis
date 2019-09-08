# -*- coding:utf-8 -*-
'''
Author : JW Park, github : https://github.com/jwpark9010
Last working date : 2019/09/06 (JW Park)
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
        
######################   MongoDB   #############################
def MongoDB_query(_db_name, CarID, skip_value, db_list):
    client = MongoClient('125.140.110.217:27027', username='jwpark', password='jwpark')
    db = client[_db_name]
    coll = db[db_list]
    Elex_all = coll.find({"PHONE_NUM":"0"+str(CarID)}, no_cursor_timeout=True).skip(skip_value)
    record_line = coll.find({"PHONE_NUM":"0"+str(CarID)}).count()
    print("0"+str(CarID)+' record_line 수: %s' %record_line)
    return Elex_all, record_line

if __name__ =='__main__':

    db_list = ["201906", "201907"]
    for db_name in db_list:

        MONGO_DB_LIST = ['elex', 'hanuri', 'carssum', 'public']
        df = pd.read_excel("ELEX_PHONENUM_COUNT.xlsx", sheet_name=db_name)
        df = df.sort_values(["COUNT"], ascending=[False]).reset_index(drop=True)
        CarID = df["PHONE_NUM"].unique()
        CarID = CarID.tolist()
        
        i = 0
        for Vechile_num in CarID:
            i += 1
            print("[%s] 차량ID : 0%s   %s / %s \n" %(db_name, Vechile_num, i, len(CarID)))
            count = 0
            cursor, record_line = MongoDB_query(MONGO_DB_LIST[0], Vechile_num, 0, db_name)

            _list = []

            for _dict in cursor:
                if (len(_dict)>=33):
                    _list.append(_dict)

                count +=1
                printProgressBar(count+1, record_line)
            print("\n")

            df = pd.DataFrame(_list)
            df = df.fillna("NaN")

            if db_name  == db_list[0]:
                df.to_excel("csv_file/201906/0%s.xlsx" %str(Vechile_num))
            else:
                df.to_excel("csv_file/201907/0%s.xlsx" %str(Vechile_num))
        