# -*- coding: utf-8 -*-
# !/usr/bin/env python

# 環境啟動
import sys, getopt
import os
import math
import arcpy
from arcpy import *
from datetime import datetime, timedelta
import zipfile
import shutil
import csv 
from os import walk
from os.path import join

import pyodbc
from filelock import Timeout, FileLock
import patoolib
import xml.etree.ElementTree as ET

import json

import base64

#//////////////////////////////////////////////////
# 資料定義

# 取下目前路徑
sys_path = sys.path[0] 
sys_path += '/'

# 執行計次檔
run_times_file = sys_path + 'runtimes.txt'

# 系統參數檔
syspara_path = sys_path + 'sysparam_postgre.csv'
sys_args = {
    'logdb_mode'     : '1',                         # 監控LOG資料庫種類(1/mdb,2/postgre,3/SQL)
    'is_MultiRun'    : 'Y',                         # 多工執行
    'is_sdeDB'       : 'Y',                         # 使用 sde 資料庫
    'limit_doFiles'  : 3,                           # 每次執行處理幾個檔(配合每5分鐘執行一次，以控制同時執行數)
    'zips_path'      : sys_path + 'input_zips/',    # zip 檔存放路徑
                                                    # 流程控制資料 connect string( MS-Access .mdb)
    'flowConnectStr' : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',
    'config_path'    : sys_path + 'config_sde.csv',     # 星種參數檔
    'defaultPriority': '9999',                          # 預設優先序
    'zips_stub'      : sys_path + 'zips_stub/',         # zip 轉後留存路徑
    'sys_store'      : sys_path + 'sysStore.gdb',       # 系統暫存用
    'store_root_path': '',                              # 圖檔存放根路徑
    'zip_broken_path': '',                              # zip 有誤時移至路徑
    'xml_rasterdataset' : ''                            # 8 bit 共用的 raster dataset 指示
}

# 系統暫存用路徑
tempZip_path     = sys_path + 'tempZip/'

# zip history 資料
histfile = sys_path + 'History.csv'
histlines = []

# 星種資料行
config_lines = []

# 定義目前判定處理的影像內容
nowImage_args = {
    'rasterType_id'    : 'WV03',       
    'rasterType_name'  : 'WorldView-3',
    'bits'             : '16',                           # 定義此星種bits(8/16)
    'filter'           : '.IMD',                         # 定義目前處理影像的 metafile 附加名
    'pansharpen'       : 'Y',                            # 定義目前處理影像是否需做 pansharpen
    'gdbName'          : sys_path+'rasterStore.gdb',
    'fileStore_16'     : '',                             # 存放路徑不從config讀取，改依 sys_args['store_root_path']+年月+星種+16bit
    'fileStore_8'      : '',
    'datasetName_16'   : 'WV03_16',
    'datasetName_8'    : 'WV03_8',
    'panBit_1'         : '3',
    'panBit_2'         : '2',
    'panBit_3'         : '1',
    'panBit_4'         : '4',
    'pathPAN'          : '',                             # PanSharpening 檔名(此動態搜尋後填入)
    'pathMUL'          : '',
    'raster_id'        : '',                             # 年月日+type_id+流水
    'self_pansharpen'  : 'N',                            # 自行 pansharpen 提供 RGB MD add raster
    'xml_raster_type'  : ''                              # 使用 xml 做 raster type 指示
}

# 存檔用自定錯誤訊息
step_errmsg = ''

# 目前處理影像 metadata 
# (ps:此處統一以字串保存，MD 中則用正確型態存檔)
image_MetaData = {
    'img_id'           : '',                            # Text 影像編號
    'acq_time'         : '',                            # Date 拍攝日期
    'sun_elev'         : '',                            # Num  太陽高度
    'sun_azu'          : '',                            # Num  太陽仰角
    'cloud_rate'       : '',                            # Num  雲覆蓋率,最小0,最大1
    'band'             : '',                            # Int  波段,應為3或4
    'cen_x'            : '',                            # Num  中心經度, WGS84 
    'cen_y'            : '',                            # Num  中心緯度, WGS84 
    'ul_x'             : '',                            # Num  左上經度, WGS84 
    'ul_y'             : '',                            # Num  左上緯度, WGS84 
    'ur_x'             : '',                            # Num  右上經度, WGS84 
    'ur_y'             : '',                            # Num  右上緯度, WGS84 
    'll_x'             : '',                            # Num  左下經度, WGS84 
    'll_y'             : '',                            # Num  左下緯度, WGS84 
    'lr_x'             : '',                            # Num  右下經度, WGS84 
    'lr_y'             : '',                            # Num  右下緯度, WGS84 
    'ak_num'           : '',                            # Text AK Number, i.e. ... 
    'geomWKT'          : '',                            # Text 影像範圍 Polygon 表示式
    'sat_type'         : '',                            # Text 星種
    'receive_time'     : '',                            # Date 影像到貨時間
    'image_desc'       : '',                            # Text 產品等級
    'row_gsd'          : '',                            # Num  meanCollectedRowGSD 
    'col_gsd'          : '',                            # Num  mean Collected GSD
    'sat_az'           : '',                            # Num  meanSatAz
    'sat_el'           : '',                            # Num  meanSatel
    'metadata'         : '',                            # Text 把IMD全部文字存入
    'gen_time'         : '',                            # Date generation Time
    'path'             : '',                            # Text 存放檔案的絕對路徑
    'band_id'          : '',                            # Text IMD's bandid
    'catalog_id'       : '',                            # Text 12/23/16 Added
    'target_id'        : '',                            # Text mapping targets if not mapping with orders

    # 以下舊資料PostgreSQL 中並無，部分為 img_metadata 欄位
    'shoot_type'       : '',                            # Text 偵照模式
    'issendmail'       : '',                            # Text 是否已寄信通知
    'thumbnail'        : '',                            # Text 縮圖
    'note'             : '',                            # 
    'rimgid'           : '',                            # 線上編報影像 ID 
    'source_type'      : '',     
    'img_ovr'          : '',                            # 是否製作金字塔
    'img'              : ''                             # 影像解壓縮資料夾路徑
}

# 20211002 目前獲得的 runtime 保留(供 rar 7z 解壓用)
now_runtime = '99'

#///////////////////////////////////////////////////////////////////////////////////
# 共用

#////////////////////////////////////////////////////
def is_null(val,ret_val) :
    try:
        if val is None: 
            return ret_val
    except NameError:
        return ret_val

    return val

#////////////////////////////////////////////////////
def fprint(w_str) :
    print( w_str ) 
    theTime_e = datetime.now()
    with open(sys_path+'LOG.txt', 'a',encoding='UTF-8') as f:
        f.write( '['+datetime.now().strftime('%Y/%m/%d %H:%M:%S')+'] '+w_str )

#////////////////////////////////////////////////////
# 讀取系統參數
def load_sys_param():
    # 讀入檔案
    lines = []
    with open(syspara_path, 'r',encoding='UTF-8') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            if i>0 :    # 首行為註解跳過
                lines.append([each for each in each_arr])
    # 解譯到 sys_args
    sys_args['logdb_mode']      = lines[0][0]
    sys_args['is_MultiRun']     = lines[0][1]
    sys_args['is_sdeDB']        = lines[0][2]
    sys_args['limit_doFiles']   = int(lines[0][3])
    sys_args['zips_path']       = lines[0][4]
    sys_args['flowConnectStr']  = lines[0][5]
    sys_args['config_path']     = lines[0][6]
    sys_args['defaultPriority'] = lines[0][7]
    sys_args['zips_stub']       = lines[0][8]
    sys_args['sys_store']       = lines[0][9]
    sys_args['store_root_path'] = lines[0][10]
    sys_args['zip_broken_path'] = lines[0][11]
    sys_args['xml_rasterdataset'] = lines[0][12]

    return True

#////////////////////////////////////////////////////
# 讀取各星種參數後用
def load_config():
    with open(sys_args['config_path'], 'r',encoding='UTF-8') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            if i>0 :
                config_lines.append([each for each in each_arr])

    return True

#////////////////////////////////////////////////////
# 讀入 history
def read_history():
    if os.path.exists(histfile):
        with open(histfile,'r',encoding='UTF-8') as f:
           for line in f:
              line = line.strip('\n')
              histlines.append(line)
           f.close()

#////////////////////////////////////////////////////
# 寫出 history
def save_history():
    with open(histfile, 'w',encoding='UTF-8') as f:
        for line in histlines:
           f.write(line+'\n')
        f.close()

#////////////////////////////////////////////////////
# 依 id 取得 config 該星種參數
def getConfigById(id) :
    bo = False
    for line in config_lines:
        if line[0] == id :
            #fprint('find:'+''.join(line))
            bo = True
            nowImage_args['rasterType_id']    = line[0]
            nowImage_args['rasterType_name']  = line[1]
            nowImage_args['bits']             = line[2]
            nowImage_args['filter']           = line[3]
            nowImage_args['pansharpen']       = line[4]
            nowImage_args['gdbName']          = line[5]
            nowImage_args['fileStore_16']     = ''
            nowImage_args['fileStore_8']      = ''
            nowImage_args['datasetName_16']   = line[6]
            nowImage_args['datasetName_8']    = line[7]
            nowImage_args['panBit_1']         = line[8]
            nowImage_args['panBit_2']         = line[9]
            nowImage_args['panBit_3']         = line[10]
            nowImage_args['panBit_4']         = line[11]
            nowImage_args['self_pansharpen']  = line[12]
            nowImage_args['xml_raster_type']  = line[13]
    return bo

#///////////////////////////////////////////////////////
# 找出檔案中字串位置
def findFileStrPos( file_name, find_str ) :
    filea = open(file_name, "r",encoding='UTF-8')        
    fileaString = filea.read()               
    idFilter = find_str            
    idPosition = fileaString.find(idFilter)  
    #filea.seek(idPosition+33,0)              
    #str = filea.read(4)               
    filea.close()

    return idPosition

#//////////////////////////////////////////////////////////////////////////////////////////////////////////
# 以下為各星種判斷

#///////////////////////////////////////////////////////
# 判斷是否是 WordView2 星種，是則一併取得星種參數回傳
def checkWorldView2( check_path ):
    # 找 .IMD meta 檔
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # 有找到則開始檢查
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "WV02"')
    # pos>-1 則找到
    bo = False
    if pos>-1 :
        bo = getConfigById('WV02')
    return bo

#///////////////////////////////////////////////////////
# 判斷是否是 WordView1 星種，是則一併取得星種參數回傳
def checkWorldView1( check_path ):
    # 找 .IMD meta 檔
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # 有找到則開始檢查
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "WV01"')
    # pos>-1 則找到
    bo = False
    if pos>-1 :
        bo = getConfigById('WV01')
    return bo

#/////////////////////////////////////////////////////////
# 判斷是否是 WordView3 星種，是則一併取得星種參數回傳
def checkWorldView3( check_path ):
    # 找 .IMD meta 檔
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # 有找到則開始檢查
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "WV03"')
    # pos>-1 則找到
    bo = False
    if pos>-1 :
        bo = getConfigById('WV03')
    return bo

#////////////////////////////////////////////////////////
# 判斷是否是 WordView4 星種，是則一併取得星種參數回傳
def checkWorldView4( check_path ):
    # 找 .IMD meta 檔
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # 有找到則開始檢查
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "WV04"')
    # pos>-1 則找到
    bo = False
    if pos>-1 :
        bo = getConfigById('WV04')
    return bo

#////////////////////////////////////////////////////////
# 判斷是否是舊版 GeoEye-1 星種，是則一併取得星種參數回傳
def checkOldGeoEye( check_path ):
    # 找 xxxx_metadata.txt 檔
    meta_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('_metadata.txt'):                 # GeoEye-1 以檔名找出其 metadata 檔
                meta_file_name = fullpath
                #fprint(fullpath)

    # 有找到則開始檢查
    if meta_file_name == '' :
        return False
    pos = findFileStrPos( meta_file_name, 'Sensor Name: GeoEye-1')   # 此檔中需有此才是 GeoEye-1

    # pos>-1 則找到，找到後依 id 由 config 讀取相關資料
    bo = False
    if pos>-1 :
        bo = getConfigById('oldGE01')

    return bo

#////////////////////////////////////////////////////////
# 判斷是否是新版 GeoEye-1 星種，是則一併取得星種參數回傳
# 新版與 WorldView 相同
def checkNewGeoEye( check_path ):
    # 找 .IMD meta 檔
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # 有找到則開始檢查
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "GE01"')
    # pos>-1 則找到
    bo = False
    if pos>-1 :
        bo = getConfigById('GE01')
    return bo

#////////////////////////////////////////////////////////
# 判斷是否是 BlackSky 星種，是則一併取得星種參數回傳
def checkBlackSky( check_path ):
    # 找 xxxx_metadata.json 檔
    meta_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('_metadata.json'):                 
                meta_file_name = fullpath
                #fprint(fullpath)

    # 有找到則開始檢查
    if meta_file_name == '' :
        return False

    # 開啟 json 找關鍵字(此處可暫不 json parser，當一般 txt find 即可)
    pos = findFileStrPos( meta_file_name, '"sensorName" : "Global-8"')   # 此檔中需有此才是

    # pos>-1 則找到，找到後依 id 由 config 讀取相關資料
    bo = False
    if pos>-1 :
        bo = getConfigById('BS01')            # 回傳星種 id

    return bo

#////////////////////////////////////////////////////////
# 判斷是否是 PlanetScope 星種
# ps: 此判斷方式是先找 .tif 檔，再由檔名.xml 是否存在?
#     是/RGB(轉此xml metadata)，否/即 PlanetScope(只轉rasterid path)
def checkPlanetScope( check_path ):
    # 找 .tif 檔
    tif_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.tif'):                 
                tif_file_name = fullpath

    # 有找到則開始檢查
    if tif_file_name == '' :
        return False

    # tif.xml 檔不存在即是
    if not os.path.exists(tif_file_name.split('.')[0]+'.xml') :
        return getConfigById('PS01')            # 讀取星種

    return False

#//////////////////////////////////////////////////////////
# 判斷是否是 Pleiades 星種
# ps: 此判斷方式是先找 .dim 檔，再找檔案是否有 _MS_ _P_ 檔
def checkPleiades( check_path ):
    # 找 .dim 檔
    dim_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.dim'):                 
                dim_file_name = fullpath

    # 有找到則開始檢查
    if dim_file_name == '' :
        return False

    # 檔案列表中有 _MS_ _P_ 的即是
    bo = False
    ok_n = 0
    for root, dirs, files in walk(check_path):
        for f in files:
            if f.endswith('.tif') and '_MS_' in f:
                ok_n = ok_n + 1                
            if f.endswith('.tif') and '_P_' in f:
                ok_n = ok_n + 1 
    if ok_n >= 2 :                           
        return getConfigById('PHR01')            # 讀取星種資料

    return bo

#////////////////////////////////////////////////////////
# 判斷是否是 SkySat 星種，是則一併取得星種參數回傳
def checkSkySat( check_path ):
    # 找 .json 檔
    meta_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.json'):                 
                meta_file_name = fullpath
                #fprint(fullpath)

    # 有找到則開始檢查
    if meta_file_name == '' :
        return False

    # 開啟 json 找關鍵字(此處可暫不 json parser，當一般 txt find 即可)
    pos = findFileStrPos( meta_file_name, 'SkySat')   # 此檔中需有此才是

    # pos>-1 則找到，找到後依 id 由 config 讀取相關資料
    bo = False
    if pos>-1 :
        bo = getConfigById('SKY01')            # 回傳星種 id

    return bo


#////////////////////////////////////////////////////
# 判斷星種
def checkImageType(image_root_path):
    # 處理邏輯，判斷出 Raster Type 後從 config.csv 搜尋取得 id 及其他參數
    bo = False
    if not bo : bo = checkWorldView1( image_root_path )         # 找 WorldView1
    if not bo : bo = checkWorldView2( image_root_path )         # 找 WorldView2
    if not bo : bo = checkWorldView3( image_root_path )         # 找 WorldView3
    if not bo : bo = checkWorldView4( image_root_path )         # 找 WorldView4
    if not bo : bo = checkOldGeoEye( image_root_path )          # 找舊版 GeoEye-1
    if not bo : bo = checkNewGeoEye( image_root_path )          # 找新版 GeoEye-1
    if not bo : bo = checkPleiades( image_root_path )           # 找 Pleiades
    if not bo : bo = checkBlackSky( image_root_path )           # 找 BlackSky
    if not bo : bo = checkSkySat( image_root_path )             # 找 SkySat
    if not bo : bo = checkPlanetScope( image_root_path )        # 找 PlanetScope(此圖僅.TIF，條件太少，必需最後判斷)
    return bo


#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# metadata 相關

#//////////////////////////////////////////////
# 找 MUL PAN 
# ps:回傳圖檔檔名
def getMetaDataName( sour_imagePath ) :

    # 不同星種有不同找法
    # (ps:有 PanSharpen 亦一併將 PAN 檔找出)

    # WorldView-1 因 ArcGIS 無此 type，改用 xml 以 RGB 處理
    if nowImage_args['rasterType_name'] in ['WorldView-1'] :
        # 先找 PAN 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith( nowImage_args['filter'] ):
                    pos = findFileStrPos( fullpath, 'bandId = "P"')
                    if pos>-1 :
                        # 找到後要改用 .tif 以便能自行 pansharpen
                        tif_name = fullpath.split('.')[0] + '.tif'
                        if os.path.exists(tif_name) :
                            nowImage_args['pathPAN'] = tif_name
                            bo = True
                        break
            if bo :
                break
        if not bo :
            fprint('找不到任何 PAN 檔，檔案有缺漏，請檢查\n')
            return ''

        # 再找 MUL 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if fullpath.split('.')[0] != nowImage_args['pathPAN'].split('.')[0] and f.endswith( nowImage_args['filter'] ):
                    # 找到後要改用 .tif 以便能自行 pansharpen
                    tif_name = fullpath.split('.')[0] + '.tif'
                    if os.path.exists(tif_name) :
                        nowImage_args['pathMUL'] = tif_name
                        bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到任何 MUL 檔，檔案有缺漏，請檢查\n')
            return ''

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]


    # WorldView 系列找法
    if nowImage_args['rasterType_name'] in ['WorldView-2','WorldView-3','WorldView-4','GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01' :

        # 先找 PAN 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith( nowImage_args['filter'] ):
                    pos = findFileStrPos( fullpath, 'bandId = "P"')
                    if pos>-1 :
                        nowImage_args['pathPAN'] = fullpath
                        bo = True
                        break
            if bo :
                break
        if not bo :
            fprint('找不到任何 PAN 檔，檔案有缺漏，請檢查\n')
            return ''

        # 再找 MUL 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if fullpath != nowImage_args['pathPAN'] and f.endswith( nowImage_args['filter'] ):
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到任何 MUL 檔，檔案有缺漏，請檢查\n')
            return ''

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]

    # 舊版 GeoEye-1
    if nowImage_args['rasterType_id'] == 'oldGE01' :
        # 先找 MUL metadata 檔(endswith filter)
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith( nowImage_args['filter'] ):
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 MUL 檔，檔案有缺漏，請檢查\n')
            return ''

        # 再找 PAN 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.ntf') and f.find('_pan_'):     # ntf 檔且中間有 pan 字樣
                    nowImage_args['pathPAN'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 PAN 檔，檔案有缺漏，請檢查\n')
            return ''

        # 圖名用 pan 檔去除 pan 字樣
        sArr = os.path.basename(nowImage_args['pathPAN']).split('.')[0].split('_')

        # 因找 MD 該筆只能依靠 metadata 檔名，故自行定義圖名方式作罷
        #return sArr[0]+'_'+sArr[1]+'_'+sArr[3]

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]

    # BlackSky
    if nowImage_args['rasterType_name'] in ['BlackSky'] :
        # 先找 MUL 主圖檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and not f.endswith('-pan.tif'):     # tif 檔且無 pan 字樣
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 MUL 檔，檔案有缺漏，請檢查\n')
            return ''
        # 再找 PAN 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and f.find('-pan'):     # tif 檔且中間有 pan 字樣
                    nowImage_args['pathPAN'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 PAN 檔，檔案有缺漏，請檢查\n')
            return ''

        # 回傳圖檔名可改( 因自行 pansharpen addraster )
        return os.path.basename(nowImage_args['pathMUL']).split('.')[0].split('_')[0]

    # PlanetScope
    if nowImage_args['rasterType_name'] in ['PlanetScope'] :
        # 先找 MUL 主圖檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') :
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 MUL 檔，檔案有缺漏，請檢查\n')
            return ''

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]

    # Pleiades
    if nowImage_args['rasterType_name'] in ['Pleiades-1'] :
        # 先找 MUL 主圖檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and f.find('_MS_'):     # tif 檔且中間有 _MS_
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 MUL 檔，檔案有缺漏，請檢查\n')
            return ''
        # 再找 PAN 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and f.find('_P_'):     # tif 檔且中間有 _P_ 字樣
                    nowImage_args['pathPAN'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 PAN 檔，檔案有缺漏，請檢查\n')
            return ''

        # 回傳圖檔名為 MUL
        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]

    # SkySat
    if nowImage_args['rasterType_name'] in ['SkySat-C'] :
        # 先找 MUL 主圖檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and not f.endswith('pan.tif'):     # tif 檔且無 pan.tif 結尾
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 MUL 檔，檔案有缺漏，請檢查\n')
            return ''
        # 再找 PAN 檔
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and f.endswith('pan.tif'):     # tif 檔且有 pan.tif 結尾
                    nowImage_args['pathPAN'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('找不到此星種任何 PAN 檔，檔案有缺漏，請檢查\n')
            return ''

        # 回傳圖檔名可改( 因自行 pansharpen addraster )
        return os.path.basename(nowImage_args['pathMUL']).split('.')[0].split('_')[0]


    return ''

# 取 xml 某節點
def getXmlTagValue(tree_node, tag_path) :
    for el in tree_node.findall(tag_path) :
        # 20211002
        return str(el.text)
    return ''

# 計算xml 節點數，如波段數採立即計算
def countXmlTag(root_node,tag_root_path,tag_head) :
    tree_node = None
    for el in root_node.findall(tag_root_path) :
        tree_node = el
        break
    if tree_node is None:
        return 0
    count = 0
    for child in tree_node :
        if tag_head in child.tag :
            count = count + 1
    return count

#/////////////////////////////////////////
# paraser World-View XML 屬性
def parserWorldViewXML( xml_file_name ) :

    # 由 ET xml 讀取 xml 檔
    tree = ET.parse(xml_file_name)
    root = tree.getroot()

    # 讀取 img_id
    image_MetaData['img_id'] = getXmlTagValue(root,'./IMD/PRODUCTORDERID')
    print( 'img_id:'+image_MetaData['img_id'] )
    image_MetaData['acq_time'] = getXmlTagValue(root,'./IMD/MAP_PROJECTED_PRODUCT/EARLIESTACQTIME').replace('T',' ')
    print( 'acq_time:'+image_MetaData['acq_time'] )
    image_MetaData['sun_elev'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSUNEL')
    print( 'sun_elev:'+image_MetaData['sun_elev'] )
    image_MetaData['sun_azu'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSUNAZ')
    print( 'sun_azu:'+image_MetaData['sun_azu'] )
    image_MetaData['cloud_rate'] = getXmlTagValue(root,'./IMD/IMAGE/CLOUDCOVER')
    print( 'cloud_rate:'+image_MetaData['cloud_rate'] )

    # 波段數採立即計算
    image_MetaData['band'] = str(countXmlTag(root,'./IMD','BAND_'))
    print( 'band:'+image_MetaData['band'] )

    # 範圍
    image_MetaData['ul_x'] = getXmlTagValue(root,'./IMD/BAND_B/ULLON')
    print( 'ul_x:'+image_MetaData['ul_x'] )
    image_MetaData['ul_y'] = getXmlTagValue(root,'./IMD/BAND_B/ULLAT')
    print( 'ul_y:'+image_MetaData['ul_y'] )
    image_MetaData['ur_x'] = getXmlTagValue(root,'./IMD/BAND_B/URLON')
    print( 'ur_x:'+image_MetaData['ur_x'] )
    image_MetaData['ur_y'] = getXmlTagValue(root,'./IMD/BAND_B/URLAT')
    print( 'ur_y:'+image_MetaData['ur_y'] )
    image_MetaData['ll_x'] = getXmlTagValue(root,'./IMD/BAND_B/LLLON')
    print( 'll_x:'+image_MetaData['ll_x'] )
    image_MetaData['ll_y'] = getXmlTagValue(root,'./IMD/BAND_B/LLLAT')
    print( 'll_y:'+image_MetaData['ll_y'] )
    image_MetaData['lr_x'] = getXmlTagValue(root,'./IMD/BAND_B/LRLON')
    print( 'lr_x:'+image_MetaData['lr_x'] )
    image_MetaData['lr_y'] = getXmlTagValue(root,'./IMD/BAND_B/LRLAT')
    print( 'lr_y:'+image_MetaData['lr_y'] )
    image_MetaData['sat_type'] = getXmlTagValue(root,'./IMD/IMAGE/SATID')

    print( 'sat_type:'+image_MetaData['sat_type'] )
    image_MetaData['row_gsd'] = getXmlTagValue(root,'./IMD/IMAGE/MEANCOLLECTEDROWGSD')
    print( 'row_gsd:'+image_MetaData['row_gsd'] )
    image_MetaData['col_gsd'] = getXmlTagValue(root,'./IMD/IMAGE/MEANCOLLECTEDCOLGSD')
    print( 'col_gsd:'+image_MetaData['col_gsd'] )
    image_MetaData['sat_az'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSATAZ')
    print( 'sat_az:'+image_MetaData['sat_az'] )
    image_MetaData['sat_el'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSATEL')
    print( 'sat_el:'+image_MetaData['sat_el'] )
    image_MetaData['gen_time'] = getXmlTagValue(root,'./IMD/GENERATIONTIME').replace('T',' ')
    print( 'gen_time:'+image_MetaData['gen_time'] )
    image_MetaData['band_id'] = getXmlTagValue(root,'./IMD/BANDID')
    print( 'band_id:'+image_MetaData['band_id'] )
    image_MetaData['catalog_id'] = getXmlTagValue(root,'./IMD/PRODUCTCATALOGID')
    print( 'catalog_id:'+image_MetaData['catalog_id'] )
    image_MetaData['shoot_type'] = getXmlTagValue(root,'./IMD/IMAGE/MODE')
    print( 'shoot_type:'+image_MetaData['shoot_type'] )

    return 

#/////////////////////////////////////////
# paraser BlackSky JSON 檔屬性
def parserBlackSkyJSON( json_file_name ) :
    # 開啟 json 檔，解析轉存 image_MetaData
    input_file = open( json_file_name )
    json_data = json.load(input_file)

    # 暫取一欄測試
    image_MetaData['img_id'] = json_data['id'] if ('id' in json_data) else '' 
    print( 'img_id:'+image_MetaData['img_id'] )
    image_MetaData['acq_time'] = json_data['acquisitionDate'].replace('T',' ')  if ('acquisitionDate' in json_data) else ''
    print( 'acq_time:'+image_MetaData['acq_time'] )
    image_MetaData['sun_elev'] = str(json_data['sunElevation']) if ('sunElevation' in json_data) else '0'
    print( 'sun_elev:'+image_MetaData['sun_elev'] )
    image_MetaData['sun_azu'] = str(json_data['sunAzimuth']) if ('sunAzimuth' in json_data) else '0'
    print( 'sun_azu:'+image_MetaData['sun_azu'] )
    image_MetaData['cloud_rate'] = str(json_data['cloudCoverPercent']) if ('cloudCoverPercent' in json_data) else '0'
    print( 'cloud_rate:'+image_MetaData['cloud_rate'] )
    
    # 範圍欄位用計算
    xmin = 0.0
    ymin = 0.0
    xmax = 0.0
    ymax = 0.0
    coord_arr = json_data['geometry']['coordinates'][0]
    for coord in coord_arr:
        if xmin==0 or coord[0]<xmin :
           xmin = coord[0]
        if ymin==0 or coord[1]<ymin :
           ymin = coord[1]
        if xmax==0 or coord[0]>xmax :
           xmax = coord[0]
        if ymax==0 or coord[1]>ymax :
           ymax = coord[1]
    image_MetaData['cen_x'] = str((xmin+xmax)/2)
    print( 'cen_x:'+image_MetaData['cen_x'] )
    image_MetaData['cen_y'] = str((ymin+ymax)/2)
    print( 'cen_y:'+image_MetaData['cen_y'] )
    image_MetaData['ul_x'] = str(xmin)
    print( 'ul_x:'+image_MetaData['ul_x'] )
    image_MetaData['ul_y'] = str(ymax)
    print( 'ul_y:'+image_MetaData['ul_y'] )
    image_MetaData['ur_x'] = str(xmax)
    print( 'ur_x:'+image_MetaData['ur_x'] )
    image_MetaData['ur_y'] = str(ymax)
    print( 'ur_y:'+image_MetaData['ur_y'] )
    image_MetaData['ll_x'] = str(xmin)
    print( 'll_x:'+image_MetaData['ll_x'] )
    image_MetaData['ll_y'] = str(ymin)
    print( 'll_y:'+image_MetaData['ll_y'] )
    image_MetaData['lr_x'] = str(xmax)
    print( 'lr_x:'+image_MetaData['lr_x'] )
    image_MetaData['lr_y'] = str(ymin)
    print( 'lr_y:'+image_MetaData['lr_y'] )

    image_MetaData['sat_type'] = json_data['sensorName']
    print( 'sat_type:'+image_MetaData['sat_type'] )
    image_MetaData['sat_az'] = str(json_data['satelliteAzimuth'])
    print( 'sat_az:'+image_MetaData['sat_az'] )
    image_MetaData['sat_el'] = str(json_data['satelliteElevation'])
    print( 'sat_el:'+image_MetaData['sat_el'] )
    image_MetaData['catalog_id'] = json_data['gemini']['catalogImageId']
    print( 'catalog_id:'+image_MetaData['catalog_id'] )

    return


#/////////////////////////////////////////
# paraser Pleiades-1 屬性
def parserPleiades( dim_file_name ) :

    # 由 ET xml 讀取 dim 檔(此與xml同格式)
    tree = ET.parse(dim_file_name)
    root = tree.getroot()

    # 讀取 img_id
    image_MetaData['img_id'] = getXmlTagValue(root,'./Dataset_Identification/DATASET_NAME')
    print( 'img_id:'+image_MetaData['img_id'] )
    image_MetaData['acq_time'] = getXmlTagValue(root,'./Geometric_Data/Use_Area/Located_Geometric_Values/TIME').replace('T',' ')
    print( 'TIME_RANGE:'+image_MetaData['acq_time'] )
    image_MetaData['sun_elev'] = getXmlTagValue(root,'./Geometric_Data/Use_Area/Located_Geometric_Values/Solar_Incidences/SUN_ELEVATION')
    print( 'SUN_ELEVATION:'+image_MetaData['sun_elev'] )
    image_MetaData['sun_azu'] = getXmlTagValue(root,'./Geometric_Data/Use_Area/Located_Geometric_Values/Solar_Incidences/SUN_AZIMUTH')
    print( 'SUN_AZIMUTH:'+image_MetaData['sun_azu'] )
    image_MetaData['cloud_rate'] = getXmlTagValue(root,'./Dataset_Content/CLOUD_COVERAGE')
    print( 'CLOUD_COVERAGE:'+image_MetaData['cloud_rate'] )

    # 波段數固定填4
    image_MetaData['band'] = "4"
    print( 'band:'+image_MetaData['band'] )

    # 範圍，此找到 ./Dataset_Content/Dataset_Extent 下各 Vertex
    vertexs = root.findall('./Dataset_Content/Dataset_Extent/Vertex')
    image_MetaData['ul_x'] = vertexs[0].find('LON').text
    print( 'ul_x:'+image_MetaData['ul_x'] )
    image_MetaData['ul_y'] = vertexs[0].find('LAT').text
    print( 'ul_y:'+image_MetaData['ul_y'] )
    image_MetaData['ur_x'] = vertexs[1].find('LON').text
    print( 'ur_x:'+image_MetaData['ur_x'] )
    image_MetaData['ur_y'] = vertexs[1].find('LAT').text
    print( 'ur_y:'+image_MetaData['ur_y'] )
    image_MetaData['ll_x'] = vertexs[2].find('LON').text
    print( 'll_x:'+image_MetaData['ll_x'] )
    image_MetaData['ll_y'] = vertexs[2].find('LAT').text
    print( 'll_y:'+image_MetaData['ll_y'] )
    image_MetaData['lr_x'] = vertexs[3].find('LON').text
    print( 'lr_x:'+image_MetaData['lr_x'] )
    image_MetaData['lr_y'] = vertexs[3].find('LAT').text
    print( 'lr_y:'+image_MetaData['lr_y'] )

    # 中心點直接給
    image_MetaData['cen_x'] = getXmlTagValue(root,'./Dataset_Content/Dataset_Extent/Center/LON')
    print( 'cen_x:'+image_MetaData['cen_x'] )
    image_MetaData['cen_y'] = getXmlTagValue(root,'./Dataset_Content/Dataset_Extent/Center/LAT')
    print( 'cen_y:'+image_MetaData['cen_y'] )
    # satId 固定為 PHR01
    image_MetaData['sat_type'] = 'PHR01'
    print( 'sat_type:'+image_MetaData['sat_type'] )

    image_MetaData['catalog_id'] = getXmlTagValue(root,'./Product_Information/Delivery_Identification/Order_Identification/COMMERCIAL_REFERENCE')
    print( 'catalog_id:'+image_MetaData['catalog_id'] )

    return 

#/////////////////////////////////////////
# paraser SkySat JSON 檔屬性
def parserSkySatJSON( json_file_name ) :
    # 開啟 json 檔，解析轉存 image_MetaData
    input_file = open( json_file_name )
    json_data = json.load(input_file)

    image_MetaData['img_id'] = json_data['id'] if ('id' in json_data) else ''
    print( 'img_id:'+image_MetaData['img_id'] )

    json_data_prop = json_data['properties']
    image_MetaData['acq_time'] = json_data_prop['acquired'].replace('T',' ') if ('acquired' in json_data_prop) else ''
    print( 'acq_time:'+image_MetaData['acq_time'] )
    image_MetaData['sun_elev'] = str(json_data_prop['sun_elevation']) if ('sun_elevation' in json_data_prop) else '0'
    print( 'sun_elev:'+image_MetaData['sun_elev'] )
    image_MetaData['sun_azu'] = str(json_data_prop['sun_azimuth']) if ('sun_azimuth' in json_data_prop) else '0'
    print( 'sun_azu:'+image_MetaData['sun_azu'] )
    image_MetaData['cloud_rate'] = str(json_data_prop['cloud_percent']) if ('cloud_percent' in json_data_prop) else '0'
    print( 'cloud_rate:'+image_MetaData['cloud_rate'] )
    image_MetaData['sat_type'] = json_data_prop['satellite_id'] if ('satellite_id' in json_data_prop) else ''
    print( 'sat_type:'+image_MetaData['sat_type'] )
    image_MetaData['receive_time'] = json_data_prop['updated'].replace('T',' ') if ('updated' in json_data_prop) else ''
    print( 'receive_time:'+image_MetaData['receive_time'] )
    image_MetaData['row_gsd'] = str(json_data_prop['gsd']) if ('gsd' in json_data_prop) else '0'
    print( 'row_gsd:'+image_MetaData['row_gsd'] )
    image_MetaData['col_gsd'] = str(json_data_prop['gsd']) if ('gsd' in json_data_prop) else '0'
    print( 'col_gsd:'+image_MetaData['col_gsd'] )
    image_MetaData['sat_az'] = str(json_data_prop['satellite_azimuth']) if ('satellite_azimuth' in json_data_prop) else '0'
    print( 'sat_az:'+image_MetaData['sat_az'] )
    image_MetaData['sat_el'] = str(json_data_prop['satellite_elevation']) if ('satellite_elevation' in json_data_prop) else '0'
    print( 'sat_el:'+image_MetaData['sat_el'] )
    image_MetaData['gen_time'] = json_data_prop['published'].replace('T',' ') if ('published' in json_data_prop) else ''
    print( 'gen_time:'+image_MetaData['gen_time'] )
    
    # 範圍欄位用計算
    xmin = 0.0
    ymin = 0.0
    xmax = 0.0
    ymax = 0.0
    coord_arr = json_data['geometry']['coordinates'][0]
    for coord in coord_arr:
        if xmin==0 or coord[0]<xmin :
           xmin = coord[0]
        if ymin==0 or coord[1]<ymin :
           ymin = coord[1]
        if xmax==0 or coord[0]>xmax :
           xmax = coord[0]
        if ymax==0 or coord[1]>ymax :
           ymax = coord[1]
    image_MetaData['cen_x'] = str((xmin+xmax)/2)
    print( 'cen_x:'+image_MetaData['cen_x'] )
    image_MetaData['cen_y'] = str((ymin+ymax)/2)
    print( 'cen_y:'+image_MetaData['cen_y'] )
    image_MetaData['ul_x'] = str(xmin)
    print( 'ul_x:'+image_MetaData['ul_x'] )
    image_MetaData['ul_y'] = str(ymax)
    print( 'ul_y:'+image_MetaData['ul_y'] )
    image_MetaData['ur_x'] = str(xmax)
    print( 'ur_x:'+image_MetaData['ur_x'] )
    image_MetaData['ur_y'] = str(ymax)
    print( 'ur_y:'+image_MetaData['ur_y'] )
    image_MetaData['ll_x'] = str(xmin)
    print( 'll_x:'+image_MetaData['ll_x'] )
    image_MetaData['ll_y'] = str(ymin)
    print( 'll_y:'+image_MetaData['ll_y'] )
    image_MetaData['lr_x'] = str(xmax)
    print( 'lr_x:'+image_MetaData['lr_x'] )
    image_MetaData['lr_y'] = str(ymin)
    print( 'lr_y:'+image_MetaData['lr_y'] )

    return


#/////////////////////////////////////////
# MUL.imd metadata 檔萃取屬性
# (ps:此由上 getMetaDataName 取得 nowImage_args['pathMUL'] metadata 檔資料後放入 image_MetaData 之後轉存 MD 使用)
def parserMetaData( check_path ) :  
    # 依各星種 parser

    # World-View/新GeoEye-1 目前都相同
    if nowImage_args['rasterType_name'] in ['WorldView-1','WorldView-2','WorldView-3','WorldView-4', 'GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01' :
        imd_name = nowImage_args['pathMUL']
        xml_name = os.path.splitext(imd_name)[0] + ".xml"
        # xml 優先
        if os.path.exists(xml_name) :
            parserWorldViewXML(xml_name)
        else:
            # 暫不處理 .imd
            step_errmsg = 'XML 檔不存在，無法萃取 MetaData'
            return False
    
    # BlackSky
    if nowImage_args['rasterType_name'] in ['BlackSky'] :
        # BlackSky 要重新再取 json 檔名
        json_name = ''
        for root, dirs, files in walk(check_path):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('_metadata.json'):                 
                    json_name = fullpath
        if os.path.exists(json_name) :
            parserBlackSkyJSON(json_name)
        else:
            step_errmsg = 'xxx_metadata.json 檔不存在，無法萃取 MetaData'
            return False
    
    # PlanetScope 無 metadata 檔，不需要 parser

    # Pleiades-1
    if nowImage_args['rasterType_name'] in ['Pleiades-1'] :

        # 找出 .dim 檔，依此檔 parser 
        dim_name = ''
        for root, dirs, files in walk(check_path):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.dim'):                 
                    dim_name = fullpath
        if os.path.exists(dim_name) :
            parserPleiades(dim_name)
        else:
            step_errmsg = '.dim metadata 檔不存在，無法萃取 MetaData'
            return False
    
    # SkySat
    if nowImage_args['rasterType_name'] in ['SkySat-C'] :
        # SkySat 要重新再取 json 檔名
        json_name = ''
        for root, dirs, files in walk(check_path):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.json'):                 
                    json_name = fullpath
        if os.path.exists(json_name) :
            parserSkySatJSON(json_name)
        else:
            step_errmsg = 'xxx.json 檔不存在，無法萃取 MetaData'
            return False
    
    return True

#////////////////////////////////////////////////////////////////////////////////////////////
# arcpy raster 處理相關

#////////////////////////////////////////////////////
# Statistics 運算
def CalculateStatistics(raster,x_skip,y_skip,ignore):
    #arcpy.CalculateStatistics_management(tempRaster_16, "4", "6", "0;255;21")
    arcpy.CalculateStatistics_management(raster, x_skip, y_skip, ignore)

#////////////////////////////////////////////////////
# Pyramids
def BuildPyramids(sourDataset):
    inras = sourDataset
    pylevels = "6"
    skipfirst = "SKIP_FIRST"
    resample = "BILINEAR"
    compress = "JPEG"
    quality = "80"
    skipexist = "SKIP_EXISTING"

    arcpy.BatchBuildPyramids_management(
        inras, pylevels, skipfirst, resample, compress,
        quality, skipexist)

#////////////////////////////////////////////////////
# 將 image 新增到 MD
def addRasterToDataset( ras_type, file_path, file_name, dataset_name, filter ):

    time_f = datetime.now()
    fprint( '執行 addRasterToDataset:' )
    fprint( '使用的 Raster Type:'+ras_type )

    # 設定 AddRasterToMosaicDataset
    mdname  = dataset_name
    rastype = ras_type                             
    inpath  = file_path + "/" + file_name
    updatecs = "UPDATE_CELL_SIZES"
    updatebnd = "UPDATE_BOUNDARY"
    updateovr = "#"        
    maxlevel = "#"                         
    maxcs = "0"
    maxdim = "1500"
    spatialref = "#"                       
    inputdatafilter = "*"+filter  
    subfolder = "SUBFOLDERS"               
    duplicate = "OVERWRITE_DUPLICATES"    
    buildpy = "BUILD_PYRAMIDS"             # 建影像金字塔
    calcstats = "CALCULATE_STATISTICS"     # 建統計資料
    buildthumb = "BUILD_THUMBNAILS"        # 產生縮圖
    comments = "Add Raster Datasets"
    forcesr = "#"
    estimatestats = "ESTIMATE_STATISTICS"

    arcpy.AddRastersToMosaicDataset_management(
        mdname,  rastype, inpath, updatecs, updatebnd, updateovr,
        maxlevel, maxcs, maxdim, spatialref, inputdatafilter,
        subfolder, duplicate, buildpy, calcstats, 
        buildthumb, comments, forcesr, estimatestats)

    time_e = datetime.now()
    fprint( "費時幾秒:{0}\n".format( str((time_e-time_f).seconds )) )

    return True


#////////////////////////////////////////////////////
# pansharpen 

# 20211002 加參數
def pansharpenImage( pathMUL, pathPAN, targ_rasterTemp, rasterType_name ):

    time_f = datetime.now()
    fprint( '\n執行 PanSharpening:' )

    # 清除暫存
    arcpy.Delete_management(targ_rasterTemp)

    #Compute Pan Sharpen Weights  
    #bit_plane = nowImage_args['panBit_1']+' '+nowImage_args['panBit_2']+' '+nowImage_args['panBit_3']+' '+nowImage_args['panBit_4']
    #out_pan_weight = arcpy.ComputePansharpenWeights_management(
    #    nowImage_args['pathMUL'], nowImage_args['pathPAN'], bit_plane)
    #Get results 
    #pansharpen_weights = out_pan_weight.getOutput(0)
    #Split the results string for weights of each band
    #pansplit = pansharpen_weights.split(";")
    #print("Weight,R:"+pansplit[0].split(" ")[1]+",G:"+pansplit[1].split(" ")[1]+",B:"+pansplit[2].split(" ")[1]+",I:"+pansplit[3].split(" ")[1])

    # 開始做 PanSharpening
    # 20211002
    fprint('MUL:'+ pathMUL)
    fprint('PAN:'+ pathPAN)
    arcpy.management.CreatePansharpenedRasterDataset(
        # 20211002
        #nowImage_args['pathMUL'],
        pathMUL, 
        int(nowImage_args['panBit_1']), int(nowImage_args['panBit_2']), int(nowImage_args['panBit_3']), int(nowImage_args['panBit_4']),
        targ_rasterTemp,
        # 20211002
        #nowImage['pathPAN'],
        pathPAN, 
        #"Gram-Schmidt", 0.38, 0.25, 0.2, 0.16, rasterType_name)
        "Gram-Schmidt", 0.166, 0.167, 0.167, 0.5, rasterType_name)
        #"Gram-Schmidt",pansplit[0].split(" ")[1],pansplit[1].split(" ")[1], pansplit[2].split(" ")[1],pansplit[3].split(" ")[1])

    time_e = datetime.now()
    fprint( "費時幾秒:{0}\n".format( str((time_e-time_f).seconds )) )

    return True

#////////////////////////////////////////////////////
# 16bit轉8bit
def copyRaster(sourRaster,targRaster, bits, scale, format):

    time_f = datetime.now()
    fprint( '執行 CopyRaster:' )

    # 20211002
    fprint('Source:'+sourRaster)
    fprint('Target:'+targRaster)

    arcpy.Delete_management(targRaster)
    #arcpy.CopyRaster_management( sourRaster, targRaster,
    #    "#","#","#","NONE","NONE","8 bit unsigned","NONE","NONE")
    arcpy.management.CopyRaster(
        sourRaster, 
        targRaster, '', None, '65535', "NONE", "NONE", 
        bits, scale, "NONE", format, "NONE", "CURRENT_SLICE", "NO_TRANSPOSE")

    time_e = datetime.now()
    fprint( "費時幾秒:{0}\n".format( str((time_e-time_f).seconds )) )

    return True

#////////////////////////////////////////////////////
# 搜尋此圖名是否存在
def searchRasterExist(whereStr,mosaicDataset) :
    count = 0
    with arcpy.da.SearchCursor(mosaicDataset, ['name'], where_clause=whereStr) as cursor:
        for row in cursor:
            count += 1
    if count>0:
        return True
    return False

#////////////////////////////////////////////////////
# 圖名重復自動更名(查不僅僅更名問題，此暫不用 )
def AutoFileName( file_dir, old_name, dataset ) :
    # 先找出唯一新名
    i = 1
    new_name = old_name + '_' + str(i)
    while searchRasterExist("name='"+new_name+"'", dataset) :
       i = i + 1
       new_name = old_name + '_' + str(i)

    fprint('圖名:'+old_name+',重複自動更名為:'+new_name)

    # 再將 file_dir 路徑下有此名檔案更名
    for root, dirs, files in walk(file_dir):
        for f in files:
            file_name = os.path.basename(f).split('.')[0]
            if file_name == old_name :
                old_fullpath = join(root, f)
                new_fullpath = old_fullpath.replace(old_name, new_name)
                os.rename(old_fullpath, new_fullpath)
    return True

#//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# 流程管控相關

# 由FlowCtrl管控表及路徑資料決定下一個待轉檔案，然後轉換
def getNextZip() :

    # 先取得 zip 路徑下各 zip
    allfiles = os.listdir(sys_args['zips_path']+'.')

    # 取至 array 中 
    arr_zip = [ os.path.splitext(fname)[0] for fname in allfiles if fname.lower().endswith('.zip') or fname.lower().endswith('.rar') or fname.lower().endswith('.7z')]
    arr_zip_f = [ fname for fname in allfiles if fname.lower().endswith('.zip') or fname.lower().endswith('.rar') or fname.lower().endswith('.7z')]

    # zip 路徑中無檔案則退出
    if len(arr_zip) == 0 :
        return ''

    # 待轉檔案預設首筆
    choice_zip = arr_zip[0]

    # 開啟 FlowCtrl，逐筆查 FlowCtrl 無此筆則新增
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()

    # Dir 路徑後自動新增管制資料
    # 一併取下目前正在轉的星種
    rasterTypeList = []

    # 避免多工下兩程式同時 insert，作業前先 lock
    lock_dir = FileLock("check_dir.txt.lock")
    try:
        with lock_dir.acquire(timeout=30):
            for zip_name in arr_zip:

                _sql = "select ZipFileName,Progress,Status,RasterTypeID from FlowCtrl"
                _sql += " where ZipFileName='" + zip_name + "'"
                sr.execute(_sql)
                rows = sr.fetchall()

                # 無此 zip 則 LOG 新增一筆
                if (len(rows)<=0) :
                    theTime = datetime.now()
                    theTimeS = theTime.strftime('%Y/%m/%d %H:%M:%S')
                    # 區分 mdb/postgresql/sql 決定新增方式
                    if sys_args['logdb_mode'] == '1':           # mdb
                        _sql = "INSERT INTO FlowCtrl VALUES('"+zip_name+"','',"+sys_args['defaultPriority']
                        _sql += ",'0:待轉','0:未處理','','','','',16,'','','','','','"+theTimeS+"','')"
                        sr.execute(_sql)
                    else:
                        # postgresql 無 isnull
                        if sys_args['logdb_mode'] == '2':       # postgreSQL
                            _sql = "INSERT INTO flowctrl (objectid,zipfilename,priority,progress,status,imagebits,refertime)"
                            _sql += " VALUES( (select COALESCE(MAX(objectid), 0)+1 from flowctrl)"
                            _sql += ",'"+zip_name+"',"+sys_args['defaultPriority']
                            _sql += ",'0:待轉','0:未處理',16,'"+theTimeS+"')"
                            sr.execute(_sql)
                        # 否則即是 ms sql server
                        else:
                            _sql = "INSERT INTO flowctrl (objectid,zipfilename,priority,progress,status,imagebits,refertime)"
                            _sql += " VALUES( (select isnull(MAX(objectid), 0)+1 from flowctrl)"
                            _sql += ",'"+zip_name+"',"+sys_args['defaultPriority']
                            _sql += ",'0:待轉','0:未處理',16,'"+theTimeS+"')"
                            sr.execute(_sql)
                    if (sr.rowcount <= 0) :
                        fprint('無法新增紀錄到 FlowCtrl，請檢查:'+zip_name)
                    sr.commit()
                else:    
                    # progress 99: 已轉過或 status 3:失敗(其他則是轉換中)則自動重轉
                    if rows[0][1].find('99:') != -1 or rows[0][2].find('3:') != -1 :
                        rows[0][1] = '0:待轉'
                        rows[0][2] = '0:未處理'
                        _sql = "update FlowCtrl set Progress='"+rows[0][1]+"',Status='"+rows[0][2]+"',ErrMsg=''"
                        _sql += " where ZipFileName='" + zip_name + "'"
                        sr.execute(_sql)
                         
                    # 處理 fgdb 同星種不可同時轉
                    if sys_args['is_sdeDB'] != 'Y' and sys_args['is_MultiRun'] == 'Y' :
                        # progress 非 0 表示此星種正轉換中，不可轉
                        if (rows[0][1].find('0:') == -1  and rows[0][3] not in rasterTypeList):
                            rasterTypeList.append( rows[0][3] )
            # 解鎖
            lock_dir.release()
    except Timeout:
        fprint('dir lock timeout')
        return ''

    # 依優先序取得 FlowCtrl 0:待轉 資料後取首筆優先轉
    _sql = "select ZipFileName,RasterTypeID from FlowCtrl"
    _sql += " where Progress like '%0:%' order by T50YearMonth,Priority,ZipFileName"
    sr.execute(_sql)
    rows = sr.fetchall()

    # 判斷不是待轉星種且檔案存在即可轉 
    choice_zip = ''
    for row in rows:
        if row[0] in arr_zip and not (row[1]!='' and row[1] in rasterTypeList):
            idx = arr_zip.index(row[0])
            choice_zip = arr_zip_f[idx]

            # 第一個符合的即是優先要轉的
            break

    sr.close()
    cn.close()

    return choice_zip


# 轉同個 zip 檢查
def haveSameZipProcess(zip_name) :
    ret_bo = False
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "select ZipFileName from FlowCtrl"
    _sql += " where ZipFileName='" + zip_name + "'"
    _sql += " and Progress not like '%0:%'"            # 開始轉寫入1:取優先前檢查此，故不是 0:，表示有人先搶 
    sr.execute(_sql)
    rows = sr.fetchall()
    if (len(rows)>0) :
        ret_bo = True    

    sr.close()
    cn.close()

    return ret_bo

# fgdb 同星種只能有一個有一個執行
def haveSameRasterTypeRun(zip_name,raster_type_id) :
    ret_bo = False
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "select ZipFileName from FlowCtrl"
    _sql += " where ZipFileName<>'" + zip_name + "'"
    _sql += " and RasterTypeID='" + raster_type_id + "'"
    _sql += " and Progress not like '%0:%'"
    _sql += " and Progress not like '%99:%'"
    _sql += " and Status not like '%3:%'"
    sr.execute(_sql)
    rows = sr.fetchall()
    if (len(rows)>0) :
        ret_bo = True    

    sr.close()
    cn.close()
    return ret_bo

#///////////////////////////////////////////////////////////////
# 以下為流程管控相關各步驟

# 回復狀態 0:待轉
def FlowCtrl_Step_0(zip_name, rasterTypeID, rasterTypeName) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='0:待轉',Status='0:未處理',StartTime='',EndTime='',ErrMsg=''"
    _sql += " ,ImageName='',FileStore_16='',FileStore_8='',MosaicDataset_16='',MosaicDataset_8=''" 
    _sql += " ,RasterType='"+rasterTypeName+"',RasterTypeID='"+rasterTypeID+"'" 
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# 紀錄 FlowCtrl Step1 (取得優先，開始轉)
def FlowCtrl_Step_1(zip_name, start_time) :
    # 開啟 FlowCtrl，修正 Progress Status StartTime
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='1:獲優先開始轉',Status='1:處理中',StartTime='"+start_time+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# 紀錄 FlowCtrl Step2 (星種判斷)
def FlowCtrl_Step_2(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='2:星種判讀',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
# 紀錄星種欄
# (ps:此要加寫RasterID)
def FlowCtrl_Step_2_RasterType(zip_name, raster_type_id, raster_type, bits) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()

    # 先由type_id 找出 rasterid
    time_s = datetime.now().strftime('%Y%m%d')
    id_head = time_s+raster_type_id
    _sql = "select RasterID from FlowCtrl"
    _sql += " where RasterID like '%"+id_head+"%'"
    _sql += " order by RasterID desc"
    sr.execute(_sql)
    rows = sr.fetchall()
    serialN = "0001"
    if (len(rows)>0) :
        serialN = str(int(rows[0][0][-4:])+1).zfill(4)
    raster_id = id_head+serialN

    # raster_id 先放入       
    nowImage_args['raster_id'] = raster_id

    # 更新 flowctrl
    _sql = "UPDATE FlowCtrl SET RasterTypeId='"+raster_type_id+"',RasterType='"+raster_type+"',ImageBits="+bits
    _sql += ",RasterID='"+raster_id+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 

    sr.close()
    cn.close()
    return True

# 通用 Status ErrMsg 設定
def FlowCtrl_StatusMsg(zip_name, status, add_msg) :

    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()

    if add_msg != '' :
        _sql = "select ErrMsg from FlowCtrl"
        _sql += " where ZipFileName='" + zip_name + "'"
        sr.execute(_sql)
        rows = sr.fetchall()
        if len(rows)>0 :
            msg = ''
            if str(rows[0][0]) != 'None' :
                msg += str(rows[0][0]) + ';'
            #if len(msg)+len(add_msg)>255 :
            msg = add_msg
            #else:
            #    msg += add_msg
            _sql = "UPDATE FlowCtrl SET Status='"+status+"',ErrMsg='"+msg+"'"
            _sql += " WHERE ZipFileName='"+zip_name+"'"
            sr.execute(_sql)
    else:
        _sql = "UPDATE FlowCtrl SET Status='"+status+"'"
        _sql += " WHERE ZipFileName='"+zip_name+"'"
        sr.execute(_sql)

    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 3(圖名檢查)
def FlowCtrl_Step_3(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='3:屬性萃取',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_3_ImageName(zip_name,image_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET ImageName='"+image_name+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 4(移16bit存放)
def FlowCtrl_Step_4(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='4:移16bit存放',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_4_Store(zip_name,file_store) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET FileStore_16='"+file_store+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 5 放入 16 bit MosaicDataset
def FlowCtrl_Step_5(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='5:加入16bitMosaicDataset',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# 此除更新 FlowCtrl 外亦一併更新 mosaic dataset 中 RasterID 欄  
def FlowCtrl_Step_5_MosaicDataset(zip_name,mosaic_dataset,image_name,raster_id,image_path) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET MosaicDataset_16='"+mosaic_dataset+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()

    # 再開啟 mosaic dataset 更新此筆 rasterid(image_name欄已自動放入mosaic dataset-name)
    where_clause = "Name like '%"+image_name+"%'"
    rows = arcpy.UpdateCursor(mosaic_dataset,where_clause)
    # 此時正常僅一筆，多筆亦一併更新
    for row in rows:
        row.setValue("RasterID", raster_id)
        # 圖檔路徑用相對路徑放入
        cmp_path = [image_path, sys_args['store_root_path']]
        root = os.path.commonprefix(cmp_path)
        row.setValue("path",image_path.replace(root,''))

        # 其他欄位依星種決定

        # Wordld-View 系列
        if nowImage_args['rasterType_name'] in ['WorldView-1','WorldView-2','WorldView-3','WorldView-4','GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01':
            row.setValue("productorderId",      image_MetaData['img_id']     )
            row.setValue("earliestAcqTime",     image_MetaData['acq_time']   )
            row.setValue("meanSunEl",           image_MetaData['sun_elev']   )
            row.setValue("meanSunAz",           image_MetaData['sun_azu']    )
            row.setValue("cloudcover",          image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']       )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("meanCollectedRowGSD", image_MetaData['row_gsd']    )
            row.setValue("meanCollectedColGSD", image_MetaData['col_gsd']    )
            row.setValue("meanSatAz",           image_MetaData['sat_az']     )
            row.setValue("meanSatel",           image_MetaData['sat_el']     )
            row.setValue("generationTime",      image_MetaData['gen_time']   )
            row.setValue("bandid",              image_MetaData['band_id']    )
            row.setValue("productCatalogId",    image_MetaData['catalog_id'] )
            row.setValue("mode",                image_MetaData['shoot_type'] )

        # BlackSky
        if nowImage_args['rasterType_name'] in ['BlackSky'] :
            row.setValue("id",                  image_MetaData['img_id']     )
            row.setValue("acquisitionDate",     image_MetaData['acq_time']   )
            row.setValue("sunElevation",        image_MetaData['sun_elev']   )
            row.setValue("sunAzimuth",          image_MetaData['sun_azu']    )
            row.setValue("cloudCoverPercent",   image_MetaData['cloud_rate'] )
            row.setValue("CENLon",              image_MetaData['cen_x']      )
            row.setValue("CENLat",              image_MetaData['cen_y']      )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("sensorName",          image_MetaData['sat_type']   )
            row.setValue("satelliteAzimuth",    image_MetaData['sat_az']     )
            row.setValue("satelliteElevation",  image_MetaData['sat_el']     )
            row.setValue("catalogImageId",      image_MetaData['catalog_id'] )

        # Pleiades-1
        if nowImage_args['rasterType_name'] in ['Pleiades-1'] :
            row.setValue("img_id",              image_MetaData['img_id']     )
            row.setValue("TIME_RANGE",          image_MetaData['acq_time']   )
            row.setValue("SUN_ELEVATION",       image_MetaData['sun_elev']   )
            row.setValue("SUN_AZIMUTH",         image_MetaData['sun_azu']    )
            row.setValue("CLOUD_COVERAGE",      image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']      )
            row.setValue("cen_x",               image_MetaData['cen_x']      )
            row.setValue("cen_y",               image_MetaData['cen_y']      )
            row.setValue("ul_x",                image_MetaData['ul_x']       )
            row.setValue("ul_y",                image_MetaData['ul_y']       )
            row.setValue("ur_x",                image_MetaData['ur_x']       )
            row.setValue("ur_y",                image_MetaData['ur_y']       )
            row.setValue("ll_x",                image_MetaData['ll_x']       )
            row.setValue("ll_y",                image_MetaData['ll_y']       )
            row.setValue("lr_x",                image_MetaData['lr_x']       )
            row.setValue("lr_y",                image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("catalog_id",          image_MetaData['catalog_id'] )

        # SkySat
        if nowImage_args['rasterType_name'] in ['SkySat-C'] :
            row.setValue("id",                  is_null(image_MetaData['img_id'],'')     )
            row.setValue("acquired",            is_null(image_MetaData['acq_time'],'')   )
            row.setValue("sun_eleveation",      is_null(image_MetaData['sun_elev'],'0')   )
            row.setValue("sun_azimuth",         is_null(image_MetaData['sun_azu'],'0')    )
            row.setValue("cloud_percent",       is_null(image_MetaData['cloud_rate'],'0') )
            row.setValue("cen_x",               is_null(image_MetaData['cen_x'],'0')      )
            row.setValue("cen_y",               is_null(image_MetaData['cen_y'],'0')      )
            row.setValue("ul_x",                is_null(image_MetaData['ul_x'],'0')       )
            row.setValue("ul_y",                is_null(image_MetaData['ul_y'],'0')       )
            row.setValue("ur_x",                is_null(image_MetaData['ur_x'],'0')       )
            row.setValue("ur_y",                is_null(image_MetaData['ur_y'],'0')       )
            row.setValue("ll_x",                is_null(image_MetaData['ll_x'],'0')       )
            row.setValue("ll_y",                is_null(image_MetaData['ll_y'],'0')       )
            row.setValue("lr_x",                is_null(image_MetaData['lr_x'],'0')       )
            row.setValue("lr_y",                is_null(image_MetaData['lr_y'],'0')       )
            row.setValue("satellite_id",        is_null(image_MetaData['sat_type'],'')   )
            row.setValue("satellite_azimuth",   is_null(image_MetaData['sat_az'],'0')     )
            row.setValue("satellite_elevation", image_MetaData['sat_el']     )
            row.setValue("row_gsd",             is_null(image_MetaData['row_gsd'],'0')    )
            row.setValue("col_gsd",             is_null(image_MetaData['col_gsd'],'0')    )
            row.setValue("published",           is_null(image_MetaData['gen_time'],'')   )

        rows.updateRow(row)
        del row
    # cursor 要刪除否則會 lock 
    del rows
    return True

# 取下 16 bit thumbnail 準備寫到 8 bit thumbnail
def getThumbnail(mosaic_dataset, raster_id) :
    whereStr = "rasterid= '"+raster_id+"'"
    with arcpy.da.SearchCursor(mosaic_dataset, ['thumbnail'], where_clause=whereStr) as cursor:
        for row in cursor:
           # 寫到 C:\Temp 下
           with open("C:\\Temp\\imageToSave.png", "wb") as fh:
               fh.write(base64.decodebytes(row[0]))
           return row[0]
    return None

# 存入 8 bit thumbnail 欄    
def saveThumbNail(thumbnail, mosaic_dataset, raster_id) :
    where_clause = "rasterid='"+raster_id+"'"
    with arcpy.da.UpdateCursor(mosaic_dataset,['thumbnail'],where_clause) as cursor :
        for row in cursor: 
            row[0] = thumbnail
            cursor.updateRow(row)
    del cursor
    return True


# Step 6 Pan-Sharpen
def FlowCtrl_Step_6(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='6:Pen-Sharpening',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 7 16bit 轉 8bit
def FlowCtrl_Step_7(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='7:16bit轉8bit',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 8 移 8bit 存放
def FlowCtrl_Step_8(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='8:移8bit存放',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_8_Store(zip_name,file_store) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET FileStore_8='"+file_store+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 9 轉 8bit MosaicDataset
def FlowCtrl_Step_9(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='9:加入8bitMosaicDataset',Status='1:處理中'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
# 此與 16bit mosaic dataset 相同，要加更新 raster_id 欄等 metadata 屬性
def FlowCtrl_Step_9_MosaicDataset(zip_name,mosaic_dataset,image_name,raster_id,image_path) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET MosaicDataset_8='"+mosaic_dataset+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()

    # 再開啟 mosaic dataset 更新此筆 rasterid(image_name欄已自動放入mosaic dataset-name)
    where_clause = "Name like '%"+image_name+"%'"
    rows = arcpy.UpdateCursor(mosaic_dataset,where_clause)
    # 此時正常僅一筆，多筆亦一併更新
    for row in rows:
        row.setValue("RasterID", raster_id)
        # 圖檔路徑用相對路徑放入
        cmp_path = [image_path, sys_args['store_root_path']]
        root = os.path.commonprefix(cmp_path)
        row.setValue("path",image_path.replace(root,''))

        # 其他欄位依星種決定
        if nowImage_args['rasterType_name'] in ['WorldView-1','WorldView-2','WorldView-3','WorldView-4','GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01' :
            row.setValue("productorderId",      image_MetaData['img_id']     )
            row.setValue("earliestAcqTime",     image_MetaData['acq_time']   )
            row.setValue("meanSunEl",           image_MetaData['sun_elev']   )
            row.setValue("meanSunAz",           image_MetaData['sun_azu']    )
            row.setValue("cloudcover",          image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']       )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("meanCollectedRowGSD", image_MetaData['row_gsd']    )
            row.setValue("meanCollectedColGSD", image_MetaData['col_gsd']    )
            row.setValue("meanSatAz",           image_MetaData['sat_az']     )
            row.setValue("meanSatel",           image_MetaData['sat_el']     )
            row.setValue("generationTime",      image_MetaData['gen_time']   )
            row.setValue("bandid",              image_MetaData['band_id']    )
            row.setValue("productCatalogId",    image_MetaData['catalog_id'] )
            row.setValue("mode",                image_MetaData['shoot_type'] )

        # BlackSky
        if nowImage_args['rasterType_name'] in ['BlackSky'] :
            row.setValue("id",                  image_MetaData['img_id']     )
            row.setValue("acquisitionDate",     image_MetaData['acq_time']   )
            row.setValue("sunElevation",        image_MetaData['sun_elev']   )
            row.setValue("sunAzimuth",          image_MetaData['sun_azu']    )
            row.setValue("cloudCoverPercent",   image_MetaData['cloud_rate'] )
            row.setValue("CENLon",              image_MetaData['cen_x']      )
            row.setValue("CENLat",              image_MetaData['cen_y']      )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("sensorName",          image_MetaData['sat_type']   )
            row.setValue("satelliteAzimuth",    image_MetaData['sat_az']     )
            row.setValue("satelliteElevation",  image_MetaData['sat_el']     )
            row.setValue("catalogImageId",      image_MetaData['catalog_id'] )

        # Pleiades-1
        if nowImage_args['rasterType_name'] in ['Pleiades-1'] :
            row.setValue("img_id",              image_MetaData['img_id']     )
            row.setValue("TIME_RANGE",          image_MetaData['acq_time']   )
            row.setValue("SUN_ELEVATION",       image_MetaData['sun_elev']   )
            row.setValue("SUN_AZIMUTH",         image_MetaData['sun_azu']    )
            row.setValue("CLOUD_COVERAGE",      image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']      )
            row.setValue("cen_x",               image_MetaData['cen_x']      )
            row.setValue("cen_y",               image_MetaData['cen_y']      )
            row.setValue("ul_x",                image_MetaData['ul_x']       )
            row.setValue("ul_y",                image_MetaData['ul_y']       )
            row.setValue("ur_x",                image_MetaData['ur_x']       )
            row.setValue("ur_y",                image_MetaData['ur_y']       )
            row.setValue("ll_x",                image_MetaData['ll_x']       )
            row.setValue("ll_y",                image_MetaData['ll_y']       )
            row.setValue("lr_x",                image_MetaData['lr_x']       )
            row.setValue("lr_y",                image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("catalog_id",          image_MetaData['catalog_id'] )

        # SkySat
        if nowImage_args['rasterType_name'] in ['SkySat-C'] :
            row.setValue("id",                  is_null(image_MetaData['img_id'],'')     )
            row.setValue("acquired",            is_null(image_MetaData['acq_time'],'')   )
            row.setValue("sun_eleveation",      is_null(image_MetaData['sun_elev'],'0')   )
            row.setValue("sun_azimuth",         is_null(image_MetaData['sun_azu'],'0')    )
            row.setValue("cloud_percent",       is_null(image_MetaData['cloud_rate'],'0') )
            row.setValue("cen_x",               is_null(image_MetaData['cen_x'],'0')      )
            row.setValue("cen_y",               is_null(image_MetaData['cen_y'],'0')      )
            row.setValue("ul_x",                is_null(image_MetaData['ul_x'],'0')       )
            row.setValue("ul_y",                is_null(image_MetaData['ul_y'],'0')       )
            row.setValue("ur_x",                is_null(image_MetaData['ur_x'],'0')       )
            row.setValue("ur_y",                is_null(image_MetaData['ur_y'],'0')       )
            row.setValue("ll_x",                is_null(image_MetaData['ll_x'],'0')       )
            row.setValue("ll_y",                is_null(image_MetaData['ll_y'],'0')       )
            row.setValue("lr_x",                is_null(image_MetaData['lr_x'],'0')       )
            row.setValue("lr_y",                is_null(image_MetaData['lr_y'],'0')       )
            row.setValue("satellite_id",        is_null(image_MetaData['sat_type'],'')   )
            row.setValue("satellite_azimuth",   is_null(image_MetaData['sat_az'],'0')     )
            row.setValue("satellite_elevation", image_MetaData['sat_el']     )
            row.setValue("row_gsd",             is_null(image_MetaData['row_gsd'],'0')    )
            row.setValue("col_gsd",             is_null(image_MetaData['col_gsd'],'0')    )
            row.setValue("published",           is_null(image_MetaData['gen_time'],'')   )

        rows.updateRow(row)
        del row
    # cursor 要刪除否則會 lock 
    del rows

    return True

# Step 99 完成
def FlowCtrl_Step_99(zip_name, end_time, raster_type_id) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    # 完成要修正 Priority 以使網頁能將未處理的排前面
    _sql = "UPDATE FlowCtrl SET Progress='99:完成',Status='2:成功',EndTime='"+end_time+"',Priority=" + sys_args['defaultPriority']
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# 將 zip 檔移動到 zip 保留區
def move_zip_to_stub(sour_zip, zip_file_name) :

    # 取下年月日，準備製作子路徑將 zip 移此處
    theDate = datetime.now()
    theDateS = theDate.strftime('%Y-%m-%d')
    move_path = sys_args['zips_stub'] + theDateS
    if not os.path.exists(move_path) :
        os.mkdir(move_path)
    # 移 zip
    fprint( '將 zip 檔移動到:'+move_path+'/'+zip_file_name+'\n' )
    if os.path.exists(move_path+'/'+zip_file_name):
        os.remove(move_path+'/'+zip_file_name)
    shutil.move( sour_zip, move_path+'/'+zip_file_name )

    return

# 將 zip 檔移動到 broken，並一併寫出 .err 訊息
def move_zip_to_broken(sour_zip, zip_file_name, errmsg) :
    # zip 檔已存在先刪除
    move_path = sys_args['zip_broken_path']
    if os.path.exists(move_path+'/'+zip_file_name):
        os.remove(move_path+'/'+zip_file_name)
    # 移 zip
    fprint( '匯入有誤，將 zip 檔移動到:'+move_path+'/'+zip_file_name+'\n' )
    shutil.move( sour_zip, move_path+'/'+zip_file_name )

    # 寫出錯誤訊息到 .err 檔
    errfile = move_path+'/'+zip_file_name.split('.')[0]+'.err'
    with open(errfile, 'w') as f:
        f.write(errmsg)
        f.close()

    return

# 去除多餘路徑取得正確 tempzip_dir
def checkZipRoot(zip_dir,sub_dir) :
    ret_dir = zip_dir

    # 先判斷是否其下還有 sub_dir，有則指向 sub_dir
    if os.path.isdir(ret_dir+'/'+sub_dir):
        ret_dir += '/'+sub_dir
    else:
        # 判斷 zip_dir 下僅有一個檔案，且為路徑時改以此
        arr = os.listdir(ret_dir+'/.')
        count = 0       
        new_sub_dir = ''    
        for f in arr:
            count = count + 1
            ff = ret_dir+'/'+f
            if os.path.isdir(ff):
                new_sub_dir = f
        if count==1 and new_sub_dir != '' :
            # 路徑名改為 zip(sub_dir) 名稱(zip 名會較正式命名)
            os.rename(ret_dir+'/'+new_sub_dir, ret_dir+'/'+sub_dir)
            ret_dir += '/'+sub_dir

    return ret_dir

# 自動檢查及建立 16/8 bit 圖檔存放區
def checkCreateFileStore(zip_name) :
    # nowImage_args['fileStore_16'] 改依 sys_args['store_root_path']+星種+16/8bit+年月

    # 取下舊系統年月 T50YearMonth 
    YearMonth = ''
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "select T50YearMonth from FlowCtrl"
    _sql += " where ZipFileName='" + zip_name + "'"
    sr.execute(_sql)
    rows = sr.fetchall()
    if (len(rows)>0) :
        YearMonth = str(rows[0][0])
    sr.close()
    cn.close()

    # T50YearMonth 年月空白則依目前轉入的年月，否則依舊年月
    if YearMonth == '' or YearMonth == 'None':
        theDate = datetime.now()
        YearMonth = theDate.strftime('%Y%m')

    # 先檢查星種自動建立
                           # 由存放根路徑開始
    path = sys_args['store_root_path'] + '\\' + nowImage_args['rasterType_id']
    if not os.path.exists(path) :
        os.mkdir(path)
    # 檢查 16bit 自動建立
    fileStore_16 = path+'\\16bit'
    if not os.path.exists(fileStore_16) :
        os.mkdir(fileStore_16)
    # 再查年月不存在自動建立
    fileStore_16 += '\\'+YearMonth
    if not os.path.exists(fileStore_16) :
        os.mkdir(fileStore_16)
    # nowImage_args['fileStore_16'] 根路徑此時放入(之後轉入時再加上 zip)
    nowImage_args['fileStore_16'] = fileStore_16
    # 檢查 8bit 自動建立
    fileStore_8 = path+'\\8bit'
    if not os.path.exists(fileStore_8) :
        os.mkdir(fileStore_8)
    # 再查年月不存在自動建立
    fileStore_8 += '\\'+YearMonth
    if not os.path.exists(fileStore_8) :
        os.mkdir(fileStore_8)
    # nowImage_args['fileStore_8'] 根路徑此時放入(之後轉入時再加上 zip)
    nowImage_args['fileStore_8'] = fileStore_8

    return True

#////////////////////////////////////////////////////////////////////////////////////////////
# 主流程
def main():

    # 讀取各星種參數後用
    load_config()

    # 讀入 zip 轉入後的 history 檔，供判斷是否轉入過依據
    #read_history()       # 改用 FlowCtrl管流程，原 History.csv 仍紀錄留當參考，也許有用

    process_zip_count = 0
    #while process_zip_count < sys_args['limit_doFiles'] :
    while True :

       step_errmsg = ''
       # 由目錄及 FlowCtrl優先序取得下一個待轉 zip
       zip_file_name = getNextZip()
       if zip_file_name == '':
           break

       # 開始轉換此 zip 檔
       try:

           fprint('\n此檔開始轉換:'+zip_file_name+'\n')
           theTime_f = datetime.now()

           zip_name = os.path.splitext(zip_file_name)[0]

           step_errmsg = '取優先序錯誤'
           # 避免兩程序搶轉同一檔，取得優先要先 lock
           lock_zip = FileLock("same_zip.txt.lock")
           try:
               with lock_zip.acquire(timeout=30):
                  if haveSameZipProcess(zip_name) :
                     fprint('發現搶到相同 zip 處理\n')
                     lock_zip.release()
                     continue
                  else:
                     # 紀錄 FlowCtrl Step1 (取得優先，開始轉)
                     FlowCtrl_Step_1(zip_name, theTime_f.strftime('%Y/%m/%d %H:%M:%S'))
                     FlowCtrl_StatusMsg(zip_name, '2:成功','')
                     lock_zip.release()
           except Timeout:
               continue

           fprint( '開始時間:'+theTime_f.strftime('%Y/%m/%d %H:%M')+'\n'  )

           step_errmsg = '解壓縮錯誤'
           # 解壓縮此zip到 Temp 路徑
           tempzip_dir_root = tempZip_path + zip_name
           if os.path.isdir(tempzip_dir_root):
               shutil.rmtree(tempzip_dir_root)
           os.mkdir(tempzip_dir_root)

           # 解壓
           if zip_file_name.endswith(".zip"):      # .zip 用標準方式
               # 開啟 zip 檔
               zfile = zipfile.ZipFile(sys_args['zips_path'] + zip_file_name,'r')
               zfile.extractall(tempzip_dir_root)   
               zfile.close()
           else:                              # 其他用 patool
               #改用patool
               patoolib.extract_archive(sys_args['zips_path'] + zip_file_name, outdir=tempzip_dir_root)

           # 去除多餘路徑取得正確 tempzip_dir
           tempzip_dir = checkZipRoot(tempzip_dir_root,zip_name)

           step_errmsg = '無法判讀星種'
           # 紀錄 Step2 (星種判斷)
           FlowCtrl_Step_2(zip_name)

           # 判斷星種
           if checkImageType(tempzip_dir):
              fprint('判斷出此圖星種為:'+nowImage_args['rasterType_name']+'\n')

              # fgdb 式在寫入星種續轉前要檢查同星種只能有一個
              if sys_args['is_sdeDB'] != 'Y' and sys_args['is_MultiRun'] == 'Y' :
                 lock_check = FileLock("check_rastertype.txt.lock")
                 try:
                     with lock_check.acquire(timeout=30):
                        if haveSameRasterTypeRun(zip_name,nowImage_args['rasterType_id']) :
                           fprint('發現同星種圖正轉入中，此次轉換跳過\n')
                           FlowCtrl_Step_0(zip_name,nowImage_args['rasterType_id'],nowImage_args['rasterType_name'])
                           lock_check.release()
                           process_zip_count = process_zip_count + 1
                           continue
                        else:
                           # 寫入判斷的星種(寫入星種也要加寫RasterID)
                           FlowCtrl_Step_2_RasterType(zip_name, nowImage_args['rasterType_id'], nowImage_args['rasterType_name'], nowImage_args['bits'])
                           FlowCtrl_StatusMsg(zip_name, '2:成功','')
                           lock_check.release()
                 except Timeout:
                     FlowCtrl_Step_0(zip_name,nowImage_args['rasterType_id'],nowImage_args['rasterType_name'])
                     process_zip_count = process_zip_count + 1
                     continue
              else:
                 # 寫入判斷的星種
                 FlowCtrl_Step_2_RasterType(zip_name, nowImage_args['rasterType_id'], nowImage_args['rasterType_name'], nowImage_args['bits'])
                 FlowCtrl_StatusMsg(zip_name, '2:成功','')
           else:
              fprint('此 zip 圖檔無法判讀星種\n')
              # 將 zip 檔移動到 broken，並一併寫出 .err 訊息
              move_zip_to_broken(sys_args['zips_path']+zip_file_name, zip_file_name, step_errmsg)
              FlowCtrl_StatusMsg(zip_name, '3:失敗',step_errmsg)
              continue

           # 紀錄 Step3 (屬性萃取)
           step_errmsg = 'MetaData檔不存在'
           FlowCtrl_Step_3(zip_name)

           # 由 tempzip 檢查及取得 metadata 檔名
           # (ps: 找 MD 該筆只能依靠此 )
           zip_ImageName = getMetaDataName( tempzip_dir )                       # 回傳 metadata 檔名
           if zip_ImageName == '' :
               fprint('此壓縮檔缺少檔案:\n')
               # 將 zip 檔移動到 broken，並一併寫出 .err 訊息
               move_zip_to_broken(sys_args['zips_path']+zip_file_name, zip_file_name, step_errmsg)
               FlowCtrl_StatusMsg(zip_name, '3:失敗', step_errmsg)
               continue

           fprint('取得圖檔名:'+zip_ImageName+'\n')
           # 紀錄 metadata 檔名
           FlowCtrl_Step_3_ImageName(zip_name,zip_ImageName)

           # 接著同樣由 temp MUL.imd metadata 檔萃取屬性
           step_errmsg = 'MetaData檔屬性萃取失敗'
           if not parserMetaData(tempzip_dir) :
               fprint(step_errmsg+'，請檢查:\n')
               # 將 zip 檔移動到 broken，並一併寫出 .err 訊息
               move_zip_to_broken(sys_args['zips_path']+zip_file_name, zip_file_name, step_errmsg)
               FlowCtrl_StatusMsg(zip_name, '3:失敗', step_errmsg)
               continue
           FlowCtrl_StatusMsg(zip_name, '2:成功', '')
           
           # 20211002 保留 C 端 pathMUL pathPAN 供制式 PAN 也由 C 端以加速
           C_pathMUL = nowImage_args['pathMUL']
           C_pathPAN = nowImage_args['pathPAN']

           # pan-sharpen 提前執行，以因應非制式星種，需以此 add raster
           tempPansharpen = tempzip_dir + '\\AutoPan\\' + zip_ImageName + '.tif'
           # 依是否自行 pansharpen 處理(處理 BlackSky 等無此星種)
           if nowImage_args['self_pansharpen'] == 'Y' and nowImage_args['pansharpen'] == 'Y':
               step_errmsg = 'pan-sharping 錯誤'
               FlowCtrl_Step_6(zip_name)
               # 改為 file 式以便 addraster
               os.mkdir(tempzip_dir + '\\AutoPan')
               #out_type = 'UNKNOWN'
               out_type = 'GeoEye-1'

               # 20211002 Pansharpen 改傳 Pan 參數
               pansharpenImage(C_pathMUL, C_pathPAN, tempPansharpen, out_type)
               FlowCtrl_StatusMsg(zip_name, '2:成功', '')

           # 匯入前自動檢查及建立 16/8 bit 圖檔存放區
           checkCreateFileStore(zip_name)

           # 此星種若是 16 bit 則需加轉 8 bit 相關處理
           if nowImage_args['bits'] == '16' :

               step_errmsg = '移16bit存放錯誤'
               # 移16bit存放
               FlowCtrl_Step_4(zip_name)

               # 移轉圖檔到該星種 16 bit 存放區
               file_dir = nowImage_args['fileStore_16'] + '/' + zip_name
               fprint('圖檔移動到各星種16 bit存放:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               #改用copy，使pansharpen仍能用 temp，避免 unc 慢速
               #shutil.move(tempzip_dir,nowImage_args['fileStore_16'])
               shutil.copytree(tempzip_dir,file_dir)

               FlowCtrl_Step_4_Store(zip_name,file_dir)
               FlowCtrl_StatusMsg(zip_name, '2:成功', '')

               # 取16bit路徑 MetaData MUL 及 Pan 檔名後用
               zip_ImageName = getMetaDataName( file_dir )
               if zip_ImageName == '' :
                   fprint('找不到此星圖 MetaData 檔，無法繼續作業，請檢查:\n'+file_dir+'\n')
                   continue

               fprint('raster id:'+nowImage_args['raster_id'])
               
               step_errmsg = 'add raster 16bit MD 錯誤'
               # 放入 16 bit MosaicDataset
               FlowCtrl_Step_5(zip_name)

               # 轉此星種到 16bit mosaic dataset，此無論有無 8 bit 都需原檔轉入
               fprint( '轉入16bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']+'\n' )
               # 依是否自行 pansharpen 處理(處理 BlackSky 等無此星種)
               if nowImage_args['self_pansharpen'] == 'Y' :
                   if nowImage_args['pansharpen'] == 'Y':
                       addRasterToDataset( 'Raster Dataset', file_dir+'\\AutoPan', '',
                                           nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                           '.tif' )
                   # 如 PlanetScope 狀況，僅圖檔(.tif .jp2 等)，無屬性，僅能用原始圖 addraster
                   else:
                       addRasterToDataset( 'Raster Dataset', file_dir, '',
                                           nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                           nowImage_args['filter'] )
               else:
                   #addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                   addRasterToDataset( nowImage_args['xml_raster_type'], file_dir, '',
                                       nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                       nowImage_args['filter'] )

               # mosaic dataset 要加寫屬性(除 RasterID 外 parser metadata 檔各屬性放入)
               step_errmsg = '寫入 16bit MD metadata 錯誤'
               FlowCtrl_Step_5_MosaicDataset(zip_name,nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                             zip_ImageName, nowImage_args['raster_id'], file_dir)
               FlowCtrl_StatusMsg(zip_name, '2:成功', '')

               # 取下 16 bit thumbnail 準備寫到 8 bit thumbnail
               # 此無效暫留(ps:似乎 add raster 並無產生 thumbnail，portal看的是 portal 自己給的)
               #thumbnail_16 = getThumbnail(nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'], nowImage_args['raster_id'])

               # 制式星種，pansharpen 此時做
               tempPansharpen = sys_args['sys_store']+'/R'+datetime.now().strftime("%y%m%d_%H%M%S")
               if (nowImage_args['pansharpen'] == 'Y') and (nowImage_args['self_pansharpen'] == 'N') :
                   step_errmsg = 'pan-sharping 錯誤'
                   # Pan-Sharpen
                   FlowCtrl_Step_6(zip_name)

                   # 20211002 tempPansharpen 改到 C 端獨立路徑
                   pan_path = tempZip_path + zip_name + '_P'
                   if os.path.isdir(pan_path):
                       shutil.rmtree(pan_path)
                   os.mkdir(pan_path)
                   tempPansharpen = pan_path + '\\' + zip_ImageName + '.tif'
                   # 20211002 pathMUL PAN 同樣用 C 端的
                   pansharpenImage(C_pathMUL, C_pathPAN, tempPansharpen, nowImage_args['rasterType_name'])
                   FlowCtrl_StatusMsg(zip_name, '2:成功', '')

               step_errmsg = 'Copy 16bit 到 8bit 錯誤'
               # 16 轉 8(跳過 step 7)
               FlowCtrl_Step_8(zip_name)

               # 轉16bit到8bit
               dir_8bit = nowImage_args['fileStore_8'] + '/' + zip_name
               if os.path.isdir(dir_8bit):
                  shutil.rmtree(dir_8bit)
               os.mkdir(dir_8bit)
               file_8bit = dir_8bit + '/' + zip_ImageName + '.TIF'
               if (nowImage_args['pansharpen'] == 'Y'):

                   # 如果是自行 pansharpen，因已提前於 temp 下執行，此處要將路徑改到 16 bit 下
                   if nowImage_args['self_pansharpen'] == 'Y' :
                       #仍用 temp 下做 copy 以加快執行速度 
                       #tempPansharpen = file_dir + '\\AutoPan\\' + zip_ImageName + '.tif'
                       tempPansharpen = tempzip_dir + '\\AutoPan\\' + zip_ImageName + '.tif'

                   copyRaster(tempPansharpen, file_8bit, "8_BIT_UNSIGNED", "ScalePixelValue", "TIFF")

                   # 若是自組不能刪 tempPansharpen
                   # 20211002 都改用 C 端，最後統一刪除，此處無須處理
                   #if nowImage_args['self_pansharpen'] != 'Y' :
                   #    arcpy.Delete_management(tempPansharpen)
               else:                        # 讀取此筆16bit Raster 轉存至如上pansharpen結果
                   # 20211002 無 pansharpen 也是從 C 端 Copy 到 8 bit 
                   #copyRaster(nowImage_args['pathMUL'], file_8bit, "8_BIT_UNSIGNED", "ScalePixelValue", "TIFF")
                   copyRaster(C_pathMUL, file_8bit, "8_BIT_UNSIGNED", "ScalePixelValue", "TIFF")

               FlowCtrl_Step_8_Store(zip_name,dir_8bit)
               FlowCtrl_StatusMsg(zip_name, '2:成功', '')

               step_errmsg = 'add raster到8bit MD錯誤'
               # 轉 8 bit MosaicDataset
               FlowCtrl_Step_9(zip_name)

               # 轉入 8 bit mosaic dataset
               fprint( '轉入8bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8']+'\n' )
               #addRasterToDataset( "Raster Dataset", dir_8bit, '',
               addRasterToDataset( sys_args['xml_rasterdataset'], dir_8bit, '',
                                   nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'], 
                                   ".TIF" )
               # 存入 rasterid 等 metadata 屬性
               FlowCtrl_Step_9_MosaicDataset(zip_name,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'],
                                             zip_ImageName, nowImage_args['raster_id'], dir_8bit)
               FlowCtrl_StatusMsg(zip_name, '2:成功', '')

               # 此無效暫留(因 thumbnail 是非空但無jpgpng資料)
               #if thumbnail_16 != None :
               #   saveThumbNail(thumbnail_16, nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'],nowImage_args['raster_id'])

               # 轉完在 history.csv 寫入一筆
               #histlines.append(zip_name)

               step_errmsg = 'zip 移保留區錯誤'
               # 完成後將 zip 檔移動到zip保留區
               move_zip_to_stub(sys_args['zips_path']+zip_file_name,zip_file_name)

           # 否則處理 8 bit
           else :

               step_errmsg = '圖檔移8bit存放區錯誤'
               # 移 8bit 存放
               FlowCtrl_Step_8(zip_name)

               # 移轉圖檔到該星種 8 bit 存放區
               file_dir = nowImage_args['fileStore_8'] + '/' + zip_name
               fprint('圖檔移動到各星種8 bit存放:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_8'])

               FlowCtrl_Step_8_Store(zip_name,nowImage_args['fileStore_8'])
               FlowCtrl_StatusMsg(zip_name, '2:成功', '')

               step_errmsg = 'add raster到8bit MD錯誤'
               # 轉 8 bit MosaicDataset
               FlowCtrl_Step_9(zip_name)

               # 轉此星種到 8bit mosaic dataset
               fprint( '轉入8bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8']+'\n' )
               addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                   nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_8'],
                                   nowImage_args['filter'] )

               FlowCtrl_Step_9_MosaicDataset(zip_name,nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_8'],
                                             zip_ImageName, nowImage_args['raster_id'])
               FlowCtrl_StatusMsg(zip_name, '2:成功', '')

               # 轉完在 history.csv 寫入一筆
               #histlines.append(zip_name)

               step_errmsg = 'zip移保留區錯誤'
               # 完成後將 zip 檔移動到zip保留區
               move_zip_to_stub(sys_args['zips_path']+zip_file_name,zip_file_name)

           theTime_e = datetime.now()
           fprint( '此檔轉換結束時間:'+theTime_e.strftime('%Y/%m/%d %H:%M')+'\n'  )
           fprint( "轉此ZIP費時幾秒:{0}\n".format( str((theTime_e-theTime_f).seconds )) )

           # 完成
           FlowCtrl_Step_99(zip_name,theTime_e.strftime('%Y/%m/%d %H:%M:%S'),nowImage_args['rasterType_id'])

           # 已處理檔案+1
           process_zip_count = process_zip_count + 1
           
           # 20211002 轉完清除 Temp
           if os.path.isdir(tempzip_dir_root):
               arcpy.Delete_management(tempzip_dir_root)
               #shutil.rmtree(tempzip_dir_root)
           pan_path = tempZip_path + zip_name + '_P'
           if os.path.isdir(pan_path):
               arcpy.Delete_management(pan_path)
               #shutil.rmtree(pan_path)

       except Exception as e:
           fprint('\n發生錯誤:')
           err_msg = repr(e)
           fprint(err_msg)

           # 將 zip 檔移動到 broken，並一併寫出 .err 訊息
           move_zip_to_broken(sys_args['zips_path']+zip_file_name, zip_file_name, str(e))
           # 先移圖檔，再寫狀態，否則另個程序 dir 到會失敗自動重轉
           FlowCtrl_StatusMsg(zip_name, '3:失敗', step_errmsg)
           break

    # 最後將 history 存回
    #save_history()         

    return True


#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

# 檢查是否可執行(最多10支程式)
def haveFullRun() :
    run_times = 0
    with open(run_times_file, 'r') as f:
       for line in f:
           line = line.strip('\n')
           run_times = int(line)
       f.close()

    if run_times>=10 :
       return True 

    return False

# 將計次+1
def addRunTimes() :
    # 先鎖定再寫入，避免同時寫入
    lock_times = FileLock("lock_times.txt.lock")
    try:
        with lock_times.acquire(timeout=30):
            # 讀取檔案+1寫入
            run_times = 0
            with open(run_times_file, 'r') as f:
               for line in f:
                   line = line.strip('\n')
                   run_times = int(line)
               f.close()
            run_times = run_times + 1

            # 20211002 保留此值，以便解壓 rar .7z
            now_runtime = str(run_times)

            with open(run_times_file, 'w') as f:
                f.write(str(run_times))
                f.close()
        lock_times.release()
    except Timeout:
        return False

    return True

# 將計次-1
def minusRunTimes() :
    # 先鎖定再寫入，避免同時寫入
    lock_times = FileLock("lock_times.txt.lock")
    try:
        with lock_times.acquire(timeout=30):
            # 讀取檔案+1寫入
            run_times = 0
            with open(run_times_file, 'r') as f:
               for line in f:
                   line = line.strip('\n')
                   run_times = int(line)
               f.close()
            run_times = run_times - 1
            if run_times < 0 :
                run_times = 0
            with open(run_times_file, 'w') as f:
                f.write(str(run_times))
                f.close()
        lock_times.release()
    except Timeout:
        return False

    return True

#////////////////////////////////////////////////////////
# 執行主程序
# 先查 license 也許可加快速度
#if arcpy.CheckProduct("ArcInfo") != "Available":

# 檢查及將計次+1，以保持最多10支程式同時轉
if haveFullRun() :
    print("已滿格執行")
else:
    # 執行前將計次+1
    addRunTimes()
    # 讀取系統參數
    load_sys_param()
    if sys_args['is_MultiRun'] != 'Y' :         # 獨佔模式要用鎖定
        lock = FileLock("high_ground.txt.lock")
        try:
            with lock.acquire(timeout=1):
                main()
        except Timeout:
            print("Another instance of this application currently holds the lock.")
    else:
        main()
    # 執行完將計次-1
    minusRunTimes()

