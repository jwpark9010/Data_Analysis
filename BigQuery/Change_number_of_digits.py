# -*- coding: utf-8 -*- 
import openpyxl
import pandas as pd


load_wb = pd.read_excel('test.xlsx', encoding = 'utf-sig')

df = pd.DataFrame(load_wb, columns =["TIMESTAMP",  "DC_OUT_S", "DC_OUT_M", "WP_IN_M", "WP_IN_S", "WP_SPD_M", "WP_SPD_S", "WP_LOAD_T", "WP_LOAD_B", "HS_STM", "DISP_DIL_M", "DISP_DIL_S", "DISP_DIL", "DISP_PWR_M", "DISP_PWR_S", "DISP_GAP", "HS_TMPT_M", "HS_TMPT_S", "DC1_LEV", "DISP_VIB", "DC2_LEV", "DISP_ENG", "HS_TMPT_O", "PULP_TMPT_M"])

## TIMESTAMP를 자른 후 LIST를 넣기 위해 만든 tmp ##
tmp = []

for i in range(len(df["TIMESTAMP"])):
    _time = str(df["TIMESTAMP"].iloc[i])
    
    ## df["TIMESTAMP"]에 바로 넣어도 되나 저장된 값 접근에 대한 워닝 발생  <---- 추가적인 공부가 필요 ## 
    tmp.append(_time[:16])

## DATAFRAME에 다시 넣기 위에 시리즈 형태로 바꿈 ##
temp = pd.Series(tmp)

## 기존의 TIMESTAMP 제거 ##
del df["TIMESTAMP"]

## 새로운 TIMESTAMP 추가 및 CSV 파일로 내보내기전 정렬을 위한 작업 ##
df["TIMESTAMP"] = temp
column_pos = df
output_csv = pd.DataFrame(column_pos, columns =["TIMESTAMP",  "DC_OUT_S", "DC_OUT_M", "WP_IN_M", "WP_IN_S", "WP_SPD_M", "WP_SPD_S", "WP_LOAD_T", "WP_LOAD_B", "HS_STM", "DISP_DIL_M", "DISP_DIL_S", "DISP_DIL", "DISP_PWR_M", "DISP_PWR_S", "DISP_GAP", "HS_TMPT_M", "HS_TMPT_S", "DC1_LEV", "DISP_VIB", "DC2_LEV", "DISP_ENG", "HS_TMPT_O", "PULP_TMPT_M"])

## 행의 이름 Index를 출력하려면 index=True 출력하기 싫으면 False ##
output_csv.to_csv('data.csv', index=False)


print output_csv
