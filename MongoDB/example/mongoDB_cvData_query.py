# -*- coding: utf-8 -*-

import pymongo
import matplotlib.pyplot as plt
import pandas as pd

connection = pymongo.MongoClient("localhost", 27017)

db = connection.test_database_1
collection  = db.emp

### 방법1 ### Run time -> 1분 3초
'''
df = pd.DataFrame(columns=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "id"])

_count = 0
for _dict in collection.find():
    empty_list = []
    _count += 1
    
    for k, v in _dict.iteritems():
        empty_list.append(v)
        
    df.loc[_count] = empty_list
    print(_count)
    if _count == 10000:
        break

del df['id']
print(df)
'''
### 방법2 ### Run time -> 54초
'''
df = pd.DataFrame(columns=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "id"])

_count = 0
for _dict in collection.find():
    empty_list = []
    _count += 1

    empty_list = _dict.values()
    df.loc[_count] = empty_list
    print(_count)
    if _count == 10000:
        break

del df['id']
print(df)
'''
### 방법3 ### Run time -> 55초
_count = 0
for _dict in collection.find():
    _count += 1
    if _count == 1:
        df1 = pd.DataFrame(_dict, index=[_count])
    else:
        df2 = pd.DataFrame(_dict, index=[_count])
    
        df1 = pd.concat([df1, df2])
    print(_count)
    if _count == 10:
        break

#del df1['id']
print(df1)

plt.title('CV_spd_Data')
plt.xlabel('time')
plt.ylabel('spd')
plt.tight_layout()
plt.plot(df1['D'], 'o')
plt.grid()
plt.show()



