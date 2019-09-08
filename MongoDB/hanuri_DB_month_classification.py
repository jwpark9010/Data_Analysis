# -*- coding:utf-8 -*-
'''
Author : ChulseoungChae, github : https://github.com/ChulseoungChae
'''

from __future__ import print_function
import pprint
import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import time
import pandas as pd
import os
import sys
import math
import re
from datetime import timedelta, datetime
from pytz import timezone
import copy

KST = timezone('Asia/Seoul')


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


def MongoDB_query(_db_name, skip_num):
    # mongodb에서 data를 query하기 위한 함수(read권한만 있는 계정)
    client = MongoClient('125.140.110.217:27017', username='guest', password='keti',\
            authSource='guest')
    db = client[_db_name]         #Select Hanuri Tien database
    coll = db["ALL"]              #Select ALL collection of Hanuri Tien database
    if skip_num == int(0):
        hanuri_all = coll.find(no_cursor_timeout=True)
    else:
        hanuri_all = coll.find(no_cursor_timeout=True).skip(skip_num) # additional syntax.limit(1000000) (no_cursor_timeout=True) # ALL collection에서 모든 documents를 find

    return hanuri_all


def MongoDB_insert(_data, _db_name, _collection_name):
    # mongodb에 data를 insert하기 위한 함수(read and write 권한이 있는 계정)
    MONGO_URI = 'mongodb://root:keti1234@125.140.110.217:27017/admin?authSource=admin'
    with MongoClient(MONGO_URI) as client:
        db = client[_db_name]
        coll = db['A'+_collection_name]  # mongoDB의 collection name은 str이나 chr로 시작해야되므로 앞에 'A'를 붙임
        try:
            coll.insert_many(_data)
        except BulkWriteError as exc:
            print('\n')
            pprint.pprint(exc.details['writeErrors'][0]['errmsg'])
            print('\n')
            pass


if __name__ =='__main__':
    MONGO_DB_LIST = ['elex', 'hanuri', 'umc', 'public']
    mongodb_skip_num = 168735981

    cursor = MongoDB_query(MONGO_DB_LIST[1], mongodb_skip_num) # hanuri database를 선택

    count = 0
    per_count = 0
    _list = []
    input_data_num = 100000    # DB에 한번에 입력할 line수를 지정
    previous_month = '201809'  # 'RECORD_TIME'의 초기 연도,월을 '201809'로 설정
    total_data_len = cursor.count()
    print('Query Info - database : %s  colletion : %s  total length : %s\n' %(MONGO_DB_LIST[1],'ALL',total_data_len))


    # for문을 돌면서 'RECORD_TIME'의 연도,월이 이전line과 비교하여 같으면 리스트에 추가하여 설정 line이되면 insert, 다르면 이전까지 추가된 리스트를 insert하고 리스트 초기화
    for i in cursor:
        count += 1
        per_count += 1


        copied_dict = copy.deepcopy(i)
        if type(i['RECORD_TIME']) == datetime:
            datetype_str = str(i['RECORD_TIME'])[:10]
            transformed_str = re.sub('[-]', '', datetype_str)
            month = transformed_str
        elif type(i['RECORD_TIME']) == unicode:
            month = i['RECORD_TIME'][:6]
            copied_dict['RECORD_TIME'] = KST.localize(datetime.strptime(str(i['RECORD_TIME']), "%Y%m%d%H%M%S"))
        
        # 'RECORD_TIME'의 연도,월이 이전의 line과 비교하여 같으면 리스트에 데이터를 한줄씩 추가하여 설정 line수가 되면 DB에 insert
        if month == previous_month:
            _list.append(copied_dict)
            if len(_list) == input_data_num:
                if len(_list) != 0:
                    MongoDB_insert(_list, MONGO_DB_LIST[1], month)
                    _list = []

        # 'RECORD_TIME'의 연도,월이 이전의 line과 비교하여 다르면 이전의 line까지 저장된 리스트를 DB에 insert하고 새로운 line를 추가하고 계속 진행
        else:
            if len(_list) != 0:
                MongoDB_insert(_list, MONGO_DB_LIST[1], previous_month)
                _list = []
                _list.append(copied_dict)

        
        printProgressBar(per_count, input_data_num)
        if per_count == input_data_num:
            print('\n========== number of %s finish ==========\n' %(count+int(mongodb_skip_num)))
            per_count = 0
            
        previous_month = month

        if count == total_data_len:
            exit()