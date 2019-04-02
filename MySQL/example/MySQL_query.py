import pymysql
import pandas as pd
import matplotlib.pyplot as plt

db = pymysql.connect(host='10.0.1.209', port=3306, user='jwpark', passwd='35892356a!', db='kimsql_example', charset='utf8', cursorclass = pymysql.cursors.DictCursor)
cursor = db.cursor()

sql_select = "Select * FROM Scientists"
cursor.execute(sql_select)

result = cursor.fetchall()

print(result)

df = pd.DataFrame(result, columns=['ID', 'NAME', 'BORN', 'DIED', 'AGE', 'OCCUPATION', 'COUNTRY'])
print(df)

plt.title('finish_py_to_sql')
plt.xlabel('Index_number')
plt.ylabel('Age')
plt.tight_layout()
plt.plot(df['AGE'], 'o')
plt.grid()
plt.show()

db.close()