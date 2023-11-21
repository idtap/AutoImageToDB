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
syspara_path = sys_path + 'zip_sysparam.csv'
sys_args = {
    'sourImage_T50' : sys_path + 'sourImage_T50/',        # 待自動壓縮T50圖檔根路徑
    'targZips_path' : sys_path + 'input_zips/',           # 壓縮後自動移至根路徑(此即待自動匯入)
    'ConnectStr'    : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',      # flowctrl 流程LOG資料庫
    'limit_doFiles' : 100,                                 # 每次執行壓縮幾檔
    'zip_temp'      : sys_path + 'zip_temp/',
    'histlines'     : []
}

# 壓縮歷史資料
histfile = sys_path + 'zip_history.csv'


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

#////////////////////////////////////////////////////
# 讀入 history
def load_history():
    sys_args['histlines'] = []
    if os.path.exists(histfile):
        with open(histfile,'r',encoding='UTF-8') as f:
           for line in f:
              line = line.strip('\n')
              sys_args['histlines'].append(line)
           f.close()

#////////////////////////////////////////////////////
# 寫出 history
def save_history(line):
    with open(histfile, 'a',encoding='UTF-8') as f:
        f.write(line+'\n')
        f.close()

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
        # 目錄名是數字才處理
        if not f.isdigit():
            continue
        # 檢查數字必須介於 200000-203000 才是待壓縮
        if int(f)<200000 or int(f)>203000 :
            continue
        # 此時即是待壓縮處理的年月路徑
        # 取下此年月路徑下各子目錄，檢查 history 中沒有的即是
        arr_sub = os.listdir(sys_args['sourImage_T50']+f+'\\.')            # 取到 list 中
        for f_sub in arr_sub:
           dir_name = sys_args['sourImage_T50']+f+'\\'+f_sub
           if not os.path.isdir( dir_name ):
               continue
           # 檢查 history，不存在即可壓縮
           # 檢查前 lock 避免同時查
           lock_dir = FileLock("zip_dir.txt.lock")
           try:
               with lock_dir.acquire(timeout=30):
                   load_history()       
                   if dir_name not in sys_args['histlines']:
                       find_dir_info = f+','+f_sub            # 將年月路徑及壓縮路徑名回傳
                       save_history(dir_name)                 # 先存入才可判斷是否待轉
                       lock_dir.release()
                       break
                   else:
                       lock_dir.release()
                       continue
           except Timeout:
               continue
        if find_dir_info != '':
           break

    return  find_dir_info


# 開始壓縮及移待轉路徑
def CompressDir(dir_name,dir_year_month) :
    
    zip_name = sys_args['sourImage_T50']+dir_year_month+'\\'+dir_name
    print('\nzip name:'+zip_name)
    temp_zip_name = sys_args['zip_temp']+dir_name
    print('\ntemp zip name:'+temp_zip_name)

    # 先複製到 Temp 端再壓縮
    if os.path.isdir(temp_zip_name):
        shutil.rmtree(temp_zip_name)
    shutil.copytree(zip_name,temp_zip_name)

    # 先壓縮
    relroot = os.path.abspath(os.path.join(temp_zip_name, os.pardir))
    zf = zipfile.ZipFile((sys_args['zip_temp']+'{}.zip').format(dir_name), 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(temp_zip_name):
        zf.write(root, os.path.relpath(root, relroot))
        for file in files:
            filename = os.path.join(root, file)
            if os.path.isfile(filename): # regular files only
                arcname = os.path.join(os.path.relpath(root, relroot), file)
                zf.write(filename, arcname)
    zf.close()

    # 移轉 zip 到待轉目錄前先寫一筆到 flowctrl
    cn = pyodbc.connect(sys_args['ConnectStr'],autocommit=True)
    sr = cn.cursor()

    theTime = datetime.now()
    theTimeS = theTime.strftime('%Y/%m/%d %H:%M:%S')
    # 仍要先查此筆是否存在，是則用修改方式
    _sql = "select ZipFileName from FlowCtrl"
    _sql += " where ZipFileName='" + dir_name + "'"
    sr.execute(_sql)
    rows = sr.fetchall()
    # 無則新增
    if (len(rows)<=0) :
        _sql = "INSERT INTO flowctrl (objectid,zipfilename,priority,progress,status,refertime,T50YearMonth)"
        _sql += " VALUES( (select COALESCE(MAX(objectid), 0)+1 from flowctrl)"
        _sql += ",'"+dir_name+"',99999"
        _sql += ",'0:待轉','0:未處理','"+theTimeS+"','"+dir_year_month+"')"
        sr.execute(_sql)
    else:
        _sql = "update FlowCtrl set Progress='0:待轉',Status='0:未處理',ErrMsg=''"
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
       #if zip_name == '' :
       #    break

       process_dir_count = process_dir_count + 1


    return True


#////////////////////////////////////////////////
# 執行主程序

# 讀取系統參數
load_sys_param()

main()

