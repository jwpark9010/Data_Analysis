# -*- coding:utf-8 -*-
'''
Author : ChulseoungChae, github : https://github.com/ChulseoungChae
'''

from __future__ import print_function # 파이썬2에서 파이썬3의 print문처럼 사용할 수 있게하는 라이브러리
import pprint
from pymongo import MongoClient
import pandas as pd
import os
import sys
import math


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
    # DB 연결
    MONGO_DB_LIST = ['elex', 'hanuri', 'umc', 'public']  # ReleaseDB에 mongoDB의 database 목록들
    collection_name = "A201810"
    client = MongoClient('125.140.110.217:27017', username='guest', password='keti',\
            authSource='guest')
    db = client[MONGO_DB_LIST[1]]      # Select Hanuri Tien database
    #coll = db["ALL"]                   # Select ALL collection of Hanuri Tien database
    coll = db[collection_name]
    hanuri_all = coll.find()           # Find all documents of ALL collection

    count = 0
    per_count = 0
    _list = []
    data_line_num = 500000 # 하나의 파일에 쓰여질 line수 설정

    coll_total_data_len = hanuri_all.count()
    print(coll_total_data_len)

    # 코드가 있는 위치에서 데이터를 저장할 폴더가 있는지 확인하고 없으면 만들어준다
    if not(os.path.isdir('data_file')):
        os.makedirs(os.path.join('data_file'))

    # for문을 돌면서 라인 하나씩 리스트에 추가하여 설정 line개수가 되면 데이터프레임으로 만들고 그 데이터프레임을 excel이나 csv파일로 저장
    for i in hanuri_all:
        count += 1
        per_count += 1
        _list.append(i)  # 하나의 라인씩 리스트에 추가
        #rintProgressBar(per_count, data_line_num)  # 작업의 진행정도를 진행바로 보이도록함

        # collection의 데이터 라인수가 설정된 파일 데이터 라인수보다 적을때
        if coll_total_data_len <= data_line_num:
            printProgressBar(per_count, coll_total_data_len)  # 작업의 진행정도를 진행바로 보이도록함
            if count == coll_total_data_len:
                df = pd.DataFrame(_list)  # 한라인씩 추가된 리스트를 데이터프레임으로 변환
                print('\ncomplete make dataframe')
                df.to_excel('data_file/file_'+str(collection_name)+'_'+str(count)+'.xlsx', encoding='utf-8') # 데이터 프레임을 excel파일로 만들때
                _list = []
                print('success make file...\n')
                per_count = 0

        # collection의 데이터 라인수가 설정된 파일 데이터 라인수보다 많을때
        else:
            printProgressBar(per_count, data_line_num)  # 작업의 진행정도를 진행바로 보이도록함
            if count % data_line_num == 0:
                df = pd.DataFrame(_list)  # 한라인씩 추가된 리스트를 데이터프레임으로 변환
                #df.to_csv('csv_file/file_'+str(count)+'.csv', encoding='utf-8') # 데이터 프레임을 csv파일로 만들때
                df.to_excel('csv_file/file_'+str(count)+'.xlsx', encoding='utf-8') # 데이터 프레임을 excel파일로 만들때
                _list = []
                print('success make file...\n')
                per_count = 0

'''
_dict = {u'GPS_TIME': u'20180928153800', u'GPS_LAT': 37.429765, u'TEMPER2': -5555,\
    u'GPS_STATUS': u'T', u'PHONE_NUM': u'01220662959', u'RECORD_TIME': u'20180928153800',\
    u'TEMPER1': -5555, u'DRIVE_SPEED': 0, u'GPS_LONG': 127.24075, u'GPS_ANGLE': 0.0, \
    u'VENDOR': u'hanuri', u'DRIVE_LENGTH_TOTAL': 84713101, u'VEHICLE_NUM': u'\uacbd\uae3096\uc7901150.',\
    u'DEVICE_STATUS_CD': 0, u'GPS_LENGTH': 84713101, u'_id': '5bb5794ce9a799067e900287',\
    u'FUEL_CONSUM_TOTAL': 0, u'GPS_DT': u'20180928', u'ORIGIN_TYPE': 4}

pprint.pprint(_dict, width=20)
'''