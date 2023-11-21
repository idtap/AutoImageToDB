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

# 系統參數檔
syspara_path = sys_path + 'itas_zip_sysparam.csv'
sys_args = {
    'sourImage_T50' : sys_path + 'itas_local_T50/',       # 待自動壓縮T50圖檔根路徑
    'targZips_path' : sys_path + 'itas_input_zips/',      # 壓縮後自動移至根路徑(此即待自動匯入)
    'ConnectStr'    : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',      # flowctrl 流程LOG資料庫
    'limit_doFiles' : 100,                                 # 每次執行壓縮幾檔
    'zip_temp'      : sys_path + 'itas_zip_temp/',
    'histlines'     : []
}

#///////////////////////////////////////////////////////////////////////////////////
# 共用副程式

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
    sys_args['sourImage_T50']     = lines[0][0]
    sys_args['targZips_path']     = lines[0][1]
    sys_args['ConnectStr']        = lines[0][2]
    sys_args['limit_doFiles']     = int(lines[0][3])
    sys_args['zip_temp']          = lines[0][4]

    return True


#////////////////////////////////////////////////////////////////////////////////////////////
# 自動壓縮程序

# 取得下一個待壓縮路徑
def getNextDirPath() :

    find_dir_info = ''
    # 搜尋待壓縮根路徑下檔案
    arr = os.listdir(sys_args['sourImage_T50']+'.')            # 取到 list 中
    for f in arr:
        # 是子目錄才處理
        if not os.path.isdir(sys_args['sourImage_T50']+f):
            continue

        # 此時即是待壓縮處理的子路徑
        # 依路徑名取下年月，後續新增 itas_flowctrl 依此加到 t50yearmonth
        arr = f.split('_')
        if len(arr)<2 :
            return ''

        yearmonth = arr[1][:6]
        find_dir_info = yearmonth+','+f

        if find_dir_info != '':
           break

    return  find_dir_info


# 開始壓縮及移待轉路徑
def CompressDir(dir_name,dir_year_month) :
    
    zip_name = sys_args['sourImage_T50']+dir_name
    print('\nzip name:'+zip_name)

    # 壓縮
    relroot = os.path.abspath(os.path.join(zip_name, os.pardir))
    zf = zipfile.ZipFile((sys_args['zip_temp']+'{}.zip').format(dir_name), 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(zip_name):
        zf.write(root, os.path.relpath(root, relroot))
        for file in files:
            filename = os.path.join(root, file)
            if os.path.isfile(filename): # regular files only
                arcname = os.path.join(os.path.relpath(root, relroot), file)
                zf.write(filename, arcname)
    zf.close()

    # 壓縮完 zip_name 路徑即可刪除
    if os.path.isdir(zip_name) :
        shutil.rmtree(zip_name)

    # 移轉 zip 到待轉目錄前先寫一筆到 itas_flowctrl
    cn = pyodbc.connect(sys_args['ConnectStr'],autocommit=True)
    sr = cn.cursor()

    theTime = datetime.now()
    theTimeS = theTime.strftime('%Y/%m/%d %H:%M:%S')

    # 仍要先查此筆是否存在，是則用修改方式
    _sql = "select ZipFileName from itas_FlowCtrl"
    _sql += " where ZipFileName='" + dir_name + "'"
    sr.execute(_sql)
    rows = sr.fetchall()

    # 無則新增
    if (len(rows)<=0) :
        _sql = "INSERT INTO itas_flowctrl (objectid,zipfilename,priority,progress,status,refertime,T50YearMonth)"
        _sql += " VALUES( (select COALESCE(MAX(objectid), 0)+1 from itas_flowctrl)"
        _sql += ",'"+dir_name+"',99999"
        _sql += ",'0:待轉','0:未處理','"+theTimeS+"','"+dir_year_month+"')"
        sr.execute(_sql)
    else:
        _sql = "update itas_FlowCtrl set Progress='0:待轉',Status='0:未處理',ErrMsg=''"
        _sql += ",ReferTime='"+theTimeS+"',T50YearMonth='"+dir_year_month+"'"
        _sql += " where ZipFileName='" + dir_name + "'"
        sr.execute(_sql)

    sr.commit() 
    sr.close()
    cn.close()

    # 移動 zip 到待轉路徑
    move_path = sys_args['targZips_path']
    if os.path.exists(move_path+dir_name+'.zip' ):
        os.remove(move_path+dir_name+'.zip')
    shutil.move( sys_args['zip_temp']+dir_name+'.zip', move_path+dir_name+'.zip' )

    return zip_name

#////////////////////////////////////////////////////////////////////////////////////////////
# 主流程
def main():

    process_dir_count = 0          # 已處理待壓縮路徑
    while process_dir_count < sys_args['limit_doFiles'] :
    
       # 取得下一個待壓縮路徑
       dir_info = getNextDirPath()
       if dir_info == '':
           break
       dir_year_month = dir_info.split(',')[0]
       dir_name = dir_info.split(',')[1]
       print('\n待壓縮路徑:'+dir_name)

       # 開始壓縮及移待轉路徑
       zip_name = CompressDir(dir_name,dir_year_month)

       process_dir_count = process_dir_count + 1


    return True


#////////////////////////////////////////////////
# 執行主程序

# 讀取系統參數
load_sys_param()

main()

