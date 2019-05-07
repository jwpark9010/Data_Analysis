#-*- coding: utf-8 -*-
import requests
import time
import json
from collections import OrderedDict
import pprint
import pandas as pd
import numpy as np
import seaborn as sb

query_params = {
        "start" : "2018-09-23 00:00:00",
        "end"   : "2018-09-24 23:59:59",
        "aggregator"    :   "none",
        "metric"    :   "hansol_data"
        }

query_tags ={
        "content"   :   "*"
        }

def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[-8:-6], _time[-5:-3], _time[-2:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))

    return epoch


def Query(_session, _url, _queries, _tags):
    headers = {'content-type'   :   'application/json'}

    dp = OrderedDict()
    dp["start"] = convertTimeToEpoch(_queries["start"])
    dp["end"] = convertTimeToEpoch(_queries["end"])
    ## -> 출력한번 해보기

    temp = OrderedDict()
    temp["aggregator"] = _queries["aggregator"]
    temp["metric"] = _queries["metric"]
    temp["tags"] = _tags
    
    dp["queries"] = []
    dp["queries"].append(temp)
    
    print ("[Querying]" + json.dumps(dp, ensure_ascii=False, indent=4))

    try:
        response = _session.post(_url, data=json.dumps(dp), headers = headers)
        
        while response.status_code > 204:
            print ("[Bad Request] Query status: %s" % (response.status_code))
            print ("[Bad Request] we got bad request, Query will be restarted after 3 sec!\n")
            time.sleep(3)
            
            print ("[Querying]" + json.dumps(dp, ensure_ascii=False, indent=4))
            response = _session.post(_url, data=json.dumps(dp), headers = headers)

        print ("[Query finish and out]")

        return response


    except Exception as e:
        print ("[exception] : %s" % (e))

def make_dataframe(query_data):
    '''
    query_data 데이터 형식
    [{"metric":"Hansol_test","tags":{"content":"DC_OUT_S"},"aggregateTags":[],"dps":{"1537655070":5.0,"1537655071":5.0,"1537655072":5.0,"1537655073":5.0}},
     {"metric":"Hansol_test","tags":{"content":"DC_OUT_M"},"aggregateTags":[],"dps":{"1537655070":3.0,"1537655071":3.0,"1537655072":3.0,"1537655073":3.0}},
     {"metric":"Hansol_test","tags":{"content":"WP_IN_M"},"aggregateTags":[],"dps":{"1537655070":71.0,"1537655071":72.0,"1537655072":72.0,"1537655073":74.0}},
     {"metric":"Hansol_test","tags":{"content":"WP_IN_S"},"aggregateTags":[],"dps":{"1537655070":2450.0,"1537655071":2450.0,"1537655072":2450.0,"1537655073":2450.0}},
     {"metric":"Hansol_test","tags":{"content":"WP_SPD_M"},"aggregateTags":[],"dps":{"1537655070":0.0,"1537655071":0.0,"1537655072":0.0,"1537655073":0.0}},
     {"metric":"Hansol_test","tags":{"content":"WP_SPD_S"},"aggregateTags":[],"dps":{"1537655070":17.0,"1537655071":17.0,"1537655072":17.0,"1537655073":17.0}},
     {"metric":"Hansol_test","tags":{"content":"WP_LOAD_T"},"aggregateTags":[],"dps":{"1537655070":0.0,"1537655071":0.0,"1537655072":0.0,"1537655073":0.0}},
     {"metric":"Hansol_test","tags":{"content":"WP_LOAD_B"},"aggregateTags":[],"dps":{"1537655070":0.0,"1537655071":0.0,"1537655072":0.0,"1537655073":0.0}}
     ···(총 23개의 content tag)
    ]
        '''
    dataframe_dict = dict()
    for _data in query_data:
        content_name = _data['tags']['content']
        dataframe_dict[content_name] = _data['dps']
    
    df = pd.DataFrame(dataframe_dict)
    print(df.head())

    return df


if __name__ == "__main__":
        
    url ="__"
    
    with requests.Session() as s:
    
        result_queryData = Query(s, url, query_params, query_tags)
    
    Query_data = result_queryData.json()
    
    make_dataframe(Query_data)
   
    

    