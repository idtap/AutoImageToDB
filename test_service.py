# -*- coding: utf-8 -*-

#////////////////////////////////////////////////////////////////////////////
# 測試 ArcGIS Enterprise Service 呼叫 

import json
import requests

# request object
session_requests = requests.Session()

# 取得 token
token_url = 'https://demo.igis.com.tw/portal/sharing/rest/generateToken'
token_payload = {
    'username'   : 'saga', 
    'password'   : 'shryu7041', 
    'referer'    : 'https://demo.igis.com.tw/',
    'expiration' : 60, 
    'f'          : 'JSON' 
}

# request post 呼叫以產生 token
token_response = session_requests.post(token_url, data=token_payload)
if token_response.status_code==200:
    #print(token_response.text)
    token_json = json.loads(token_response.text)    # response 轉 json 
    if not ('error' in token_json) and ('token' in token_json):
        token_for_URL = token_json['token']
        print('回傳 token:'+token_for_URL)

        # 以 polygon ring 查詢城市以此模擬安康以 extent 查訂單
        find_order = False
        Query_URL = 'https://demo.igis.com.tw/server/rest/services/SampleWorldCities/MapServer/2/query'
        Query_payload = {
            'f'              : 'JSON',
            'token'          : token_for_URL,
            'where'          : '1=1',
            'geometry'       : '{"rings":[[[-10.0,-40.0],[-10.0,-60.0],[-16.0,-60.0],[-16.0,-40.0]]]}',
            'geometryType'   : 'esriGeometryPolygon',
            'inSR'           : '4326',
            'spatialRel'     : 'esriSpatialRelIntersects',
            'outFields'      : '*',
            'returnGeometry' : 'false'
        }
        Query_response = session_requests.post(Query_URL, data= Query_payload)
        if Query_response.status_code==200:
            query_json = json.loads(Query_response.text)
            if ('features' in query_json) and len(query_json['features'])>0 :
                find_order = True
                print( '查詢回傳首筆各欄:'+str(query_json['features'][0]['attributes']) )
            else:
                print( '此範圍查無訂單' )
        else:
            print('連不上訂單服務網址，可能網址錯誤')

        # 查無定單則以此 extent 查目標，此同樣以 sampleWorldCities 模擬安康目標點
        #if not find_order:
        Query_URL = 'https://demo.igis.com.tw/server/rest/services/SampleWorldCities/MapServer/0/query'
        Query_payload = {
            'f'              : 'JSON',
            'token'          : token_for_URL,
            'where'          : '1=1',
            'geometry'       : '{"rings":[[[-40.0,-10.0],[-60.0,-10.0],[-60.0,-16.0],[-40.0,-16.0]]]}',
            'geometryType'   : 'esriGeometryPolygon',
            'inSR'           : '4326',
            'spatialRel'     : 'esriSpatialRelIntersects',
            'outFields'      : '*',
            'returnGeometry' : 'false'
        }
        Query_response = session_requests.post(Query_URL, data= Query_payload)
        if Query_response.status_code==200:
            query_json = json.loads(Query_response.text)
            if ('features' in query_json) and len(query_json['features'])>0 :
                print( '查詢回傳目標各筆:'+str(query_json['features']) )
            else:
                print( '此範圍查不到任何目標點位' )
        else:
            print('連不上目標服務網址，可能網址錯誤')
    else:
        print('取 token 有誤!!，可能 user 或 password 有誤')
else:
    print('取 token 服務網址有誤')

