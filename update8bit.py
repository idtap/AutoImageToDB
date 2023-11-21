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
run_times_file = sys_path + 'upd_runtimes.txt'

# 系統參數檔
syspara_path = sys_path + 'sysparam_upd8bit.csv'
sys_args = {
    'limit_doFiles'  : 1,                           # 每次執行處理幾個檔(配合每5分鐘執行一次，以控制同時執行數)
                                                    # 流程控制資料 connect string( MS-Access .mdb)
    'flowConnectStr' : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',
    'config_path'    : sys_path + 'config_sde.csv',  # 星種參數檔
    'xml_rasterdataset' : ''                        # 8 bit 共用的 raster dataset 指示
}

# 系統暫存用路徑
tempUpd_path     = sys_path + 'tempUpd/'

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
    'xml_raster_type'  : '',                              # 使用 xml 做 raster type 指示
    't50yearmonth'     : ''
}

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

#///////////////////////////////////////////////////////////////////////////////////
# 共用

#////////////////////////////////////////////////////
# 檔案是否正使用中
def is_use(file_name) :

    try: 
        os.rename(file_name, file_name+"_") 
        #print( file_name+":正常" )
        os.rename(file_name+"_",file_name) 
    except OSError as e: 
        #print( file_name+":正寫入中，不可使用" )
        return True

    return False

#////////////////////////////////////////////////////
# 20211027
def is_empty_dir(dir_name):
    if [f for f in os.listdir(dir_name) if not f.startswith('.')] == []:
        return True
    return False

#////////////////////////////////////////////////////
def is_null(val,ret_val) :
    try:
        if val is None or val=='None': 
            return ret_val
    except NameError:
        return ret_val

    return val

#////////////////////////////////////////////////////
def fprint(w_str) :
    print( '['+datetime.now().strftime('%Y/%m/%d %H:%M:%S')+'] '+w_str  )

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
    sys_args['limit_doFiles']   = int(lines[0][0])
    sys_args['flowConnectStr']  = lines[0][1]
    sys_args['config_path']     = lines[0][2]
    sys_args['xml_rasterdataset'] = lines[0][3]

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

#/////////////////////////////////////////////////////////////////////////////////////////////
# 待轉相關程序

# 取得下一個待更新 pleiadis 星種檔
def getNextUpd( raster_type ):
    fprint( raster_type )
    return False

#////////////////////////////////////////////////////////////////////////////////////////////
# 主流程
def main():

    # 讀取各星種參數後用
    load_config()

    # 清 temp
    #clean_TEMP()

    process_zip_count = 0
    while process_zip_count < sys_args['limit_doFiles'] :
       # 取得下一個待轉 pleiades 16 bit 路徑
       have_next = getNextUpd('Pleiades-1')
       if not have_next:
           break


#//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# 執行主程序

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
    lock_times = FileLock("upd_lock_times.txt.lock")
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
    lock_times = FileLock("upd_lock_times.txt.lock")
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


# 檢查及將計次+1，以保持最多10支程式同時轉
if haveFullRun() :
    print("已滿格執行")
else:
    # 執行前將計次+1
    addRunTimes()

    # 讀取系統參數
    load_sys_param()

    main()

    # 執行完將計次-1
    minusRunTimes()

