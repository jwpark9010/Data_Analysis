# -*- coding: utf-8 -*-

import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import csv


connection = pymongo.MongoClient("localhost", 27017)

db = connection.test_database_1
collection  = db.emp

f = open('cv_data_50k.csv', 'r')
rdr = csv.reader(f)

df = pd.DataFrame(rdr, columns=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"])
collection.insert_many(df.to_dict('records'))

f.close()