import pymysql

db = pymysql.connect(host='localhost', port=3306, user='jwpark', passwd='35892356a!', db='kimSQL_example', charset='utf8')

cursor = db.cursor()

sql_table = '''
                CREATE TABLE test5(
                   CMF INT,
                   PARTY_NM VARCHAR(20),
                   SEG VARCHAR(20),
                   PIF_AMT INT,
                   INST_AMT INT,
                   OVRS_AMT INT,
                   CASH_AMT INT,
                   PRIMARY KEY(CMF)
                )
               '''
cursor.execute(sql_table)

sql = "INSERT INTO test5 VALUES (" + str(2356) + ", 'KIM ', 'PB', 1234041, NULL, 1301710, NULL )"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(4570) + ", 'PARK', 'MASS', NULL, NULL, 524560, NULL)"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(4563) + ", 'LEE', 'MASS', 213570, NULL, NULL, 3700000)"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(3266) + ", 'CHOI', 'MASS', 86641, NULL, NULL, NULL)"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(8904) + ", 'WOO', 'PB', 1278960, 500000, NULL, NULL)"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(4678) + ", 'JOO', 'MASS', 4567780, NULL, NULL, NULL)"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(1746) + ", 'UN', 'PB', 7836100, 3213400, NULL, NULL)"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(3120) + ", 'PINK', 'PB', NULL, NULL, NULL, NULL)"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(8974) + ", 'RED', 'MASS', 655456, , , )"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(3255) + ", 'BLACK', 'MASS', 213, , , )"
cursor.execute(sql)
sql = "INSERT INTO test5 VALUES (" + str(8977) + ", 'WHITE', 'PB', 1300, , 54000, 100000)"
cursor.execute(sql)

db.commit()
db.close()