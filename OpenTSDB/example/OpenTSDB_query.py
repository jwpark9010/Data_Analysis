# -*- coding: utf-8 -*- 
import requests
import time
import json
from collections import OrderedDict
import pprint
import pandas as pd
import numpy as np

query_params = {
    "start" : "2014-08-31 00:00:00",
    "end"   : "2014-09-02 23:59:59",
    "aggregator" : "none",
    "metric"     : "cv_data"
}

query_tags = {
    "content"   :   "Speed",
    "CarID"     :   "021"
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
    
    print "[Querying]" + json.dumps(dp, ensure_ascii=False, indent=4)

    try:
        response = _session.post(_url, data=json.dumps(dp), headers = headers)
        
        while response.status_code > 204:
            print "[Bad Request] Query status: %s" % (response.status_code)
            print "[Bad Request] we got bad request, Query will be restarted after 3 sec!\n"
            time.sleep(3)
            
            print "[Querying]" + json.dumps(dp, ensure_ascii=False, indent=4)
            response = _session.post(_url, data=json.dumps(dp), headers = headers)

        print "[Query finish and out]"

        return response


    except Exception as e:
        print "[exception] : %s" % (e)


if __name__ == "__main__":
    
    url ="http://125.140.110.217:64242/api/query"
    
    with requests.Session() as s:
    
        result_queryData = Query(s, url, query_params, query_tags)

    Query_data = result_queryData.json()
    print(Query_data)

    ps = pd.Series(Query_data['dps'])
    print(ps)



