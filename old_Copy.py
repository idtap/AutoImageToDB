# -*- coding: utf-8 -*-
# !/usr/bin/env python

# 2021/10/07 備考：
#   1.此程式專用於檢查舊系統圖將其移動到 IDT_OLD_IMG 下供自動壓縮程式壓縮
#   2.壓縮時先壓到 IDT_WATCH_N 再 Move 到 IDT_WATCH，避免自動匯入程式提前開啟 break 掉
#   3.自動壓縮程式也要改成壓後將目錄刪除(先前是因在\\上不能刪)
#   4.自動搬移程式 list 檔案後要降冪排序，將年月近的先轉
#   5.自動搬移程式只數 IDT_WATCH 中檔案數，小於 30 就搬至 30
#   6.自動搬移程式 old_hist 初始與 zip_hist 相同，以與已轉的 95 張不會重轉                                                                                                                                                                                                       

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
syspara_path = sys_path + 'old_sysparam.csv'
sys_args = {
    'sourImage_T50' : sys_path + 'sourImage_T50/',        # 舊圖T50根路徑
    'autoZip_path'  : sys_path + 'input_zips/',           # 自動匯入路徑
    'local_T50'     : sys_path + 'local_T50/',            # 複製至根路徑
    'ConnectStr'    : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',      # flowctrl 流程LOG資料庫
    'min_pathFiles' : 30,                                  # 啟動複製待轉 zip 數
    'T50_dirs'      : [],                                  # 舊圖list
    'histlines'     : []                                   # 已處理圖 list
}

# 複製歷史資料
histfile = sys_path + 'old_history.csv'


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
    sys_args['autoZip_path']      = lines[0][1]
    sys_args['local_T50']         = lines[0][2]
    sys_args['ConnectStr']        = lines[0][3]
    sys_args['min_pathFiles']     = int(lines[0][4])

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
# 自動複製程序

# 讀取舊圖T50 dir list(降冪排序)
def loadT50_dirs() :
    
    # 搜尋T50根路徑下各圖檔路徑
    sys_args['T50_dirs'] = []
    arr = os.listdir(sys_args['sourImage_T50']+'.')            # 取到 list 中
    for f in arr:
        # 是子目錄才處理
        if not os.path.isdir(sys_args['sourImage_T50']+f):
            continue

        # 目錄名是數字才處理
        if not f.isdigit():
            continue

        # 檢查數字必須介於 200000-203000 才是
        if int(f)<200000 or int(f)>203000 :
            continue

        # 此時即是舊圖年月路徑
        # 取下此年月路徑下各子目錄
        arr_sub = os.listdir(sys_args['sourImage_T50']+f+'\\.')            # 取到 list 中
        for f_sub in arr_sub:
           # 取下此張圖檔路徑
           dir_name = sys_args['sourImage_T50']+f+'\\'+f_sub
           if not os.path.isdir( dir_name ):
               continue
           # 加到 list 中準備複製
           sys_args['T50_dirs'].append(dir_name)

    # 反向排序以得降冪
    sys_args['T50_dirs'].sort(reverse = True)

    return True       

# 計算待轉區目前檔案數
def countAutoZipPath() :   

    allfiles = os.listdir(sys_args['autoZip_path']+'.')
    arr_zip_f = [ fname for fname in allfiles if fname.lower().endswith('.zip') or fname.lower().endswith('.rar') or fname.lower().endswith('.7z')]

    return len(arr_zip_f)


# 取得下一個可複製到 local 端檔案
def getNextDirPath() :

    find_dir_info = ''
    # 對 T50_dirs 每一筆，取得第一個可複製路徑
    for dir_name in sys_args['T50_dirs'] :
         if dir_name not in sys_args['histlines']:
             find_dir_info = dir_name
             break

    return  find_dir_info



#////////////////////////////////////////////////////////////////////////////////////////////
# 主流程
def main():

    # 讀取舊圖T50 dir list(降冪排序)
    loadT50_dirs()                     # T50 資料係固定的可先載入

    # 已處理圖檔路徑可先載入
    load_history()
    
    # 無窮迴圈檢查待匯入路徑下檔案數<min_pathFiles，則開始複製
    while True:
        count_autozips = countAutoZipPath()
        if count_autozips<sys_args['min_pathFiles'] :
            # 取得下一個可複製到 local 端檔案
            dir_name = getNextDirPath()
            if dir_name == '':
                break
            # 取得年月
            yearmonth = dir_name.split('\\')[-2]
            file_path = dir_name.split('\\')[-1]
            fullpath = sys_args['local_T50'] + yearmonth + '\\' + file_path
            if os.path.isdir(fullpath) :
                shutil.rmtree(fullpath)
            # 開始複製
            print('Copy:'+dir_name)
            print('To:'+fullpath)
            shutil.copytree(dir_name,fullpath)
            # 保留至 history
            save_history(dir_name)         
            sys_args['histlines'].append(dir_name)

    return True


#////////////////////////////////////////////////
# 執行主程序

# 讀取系統參數
load_sys_param()

# 僅可一份檢查
lock = FileLock("old_ground.txt.lock")
try:
    with lock.acquire(timeout=1):
        main()
except Timeout:
    print("Another instance of this application running.")

