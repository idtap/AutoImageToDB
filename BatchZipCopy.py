# -*- coding: utf-8 -*-
# !/usr/bin/env python

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

# 複製 info
list_root_path = 'D:\\安康\\AutoImageToDB\\'
target_path    = 'D:\\安康\\Test\\'          # 此即
temp_path      = 'D:\\安康\\Temp\\'
copy_info_file = 'batch_zip_info.csv'
copy_list_file = 'batch_zip_list.txt'
copy_info = {
    'max_file_time': '2023-11-20 00:00:00',
    'last_copy_file' : '',
    'file_lists'     : []
}

# 取得目前複製 info
def loadCopyInfo():

    # 讀入檔案
    lines = []
    with open(copy_info_file, 'r',encoding='UTF-8') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            lines.append([each for each in each_arr])
    # 此參數檔應僅一行
    copy_info['max_file_time']   = lines[0][0]
    copy_info['last_copy_file']   = lines[0][1].strip('\n').strip()

    print('load info:'+copy_info['max_file_time']+','+copy_info['last_copy_file'] )

    # 讀取待複製 zip 檔案清單
    copy_info['file_lists'] = []
    with open(copy_list_file,'r',encoding='UTF-8') as f:
       for line in f:
          line = line.strip('\n').strip()
          if line != '':
              copy_info['file_lists'].append(line)
       f.close()

    print('load list count:'+str(len(copy_info['file_lists'])) )

    return True


def saveCopyInfo():
    with open(copy_info_file, 'w',encoding='UTF-8') as f:
        f.write(copy_info['max_file_time']+','+copy_info['last_copy_file'])
        f.close()
  
    print('save info:'+copy_info['max_file_time']+','+copy_info['last_copy_file'] )

    return True


def saveCopyList():
    with open(copy_list_file, 'w',encoding='UTF-8') as f:
        i = 0
        for file_path in copy_info['file_lists']:
            if i!=0:
                f.write('\n')
            f.write(file_path)
            i = i + 1
        f.close()

    print('save list count:'+str(len(copy_info['file_lists'])) )

    return True


def getIndexFromList(find_file):
    i = 0
    for file_name in copy_info['file_lists']:
        if file_name==find_file:
            return i
        i = i + 1
    return -1

# 判斷及取得新的 List
def getNewCopyList() :
    # 可能前次未完成(依最後複製成功檔案)，此時繼續
    if copy_info['last_copy_file'] != '':
        now_index = getIndexFromList(copy_info['last_copy_file'])
        if now_index>=0 and (now_index+1)<len(copy_info['file_lists']):
            return now_index+1            # 下次轉即由此值
                 
    # 否則即依 file time 大於 max_file_time 重新 list
    last_file_time = datetime.strptime(copy_info['max_file_time'], '%Y-%m-%d %H:%M:%S')
    max_file_time = last_file_time
    copy_info['file_lists'] = []
    for root, dirs, files in os.walk(list_root_path):
        for file in files:
            if file.endswith(".zip") or file.endswith(".rar") or file.endswith(".7z"):
                file_path = os.path.join(root, file)
                # 取得檔案的修改時間
                mtime = os.path.getmtime(file_path)
                mtime_datetime = datetime.fromtimestamp(mtime)
                # 大於 last_file_time
                if mtime_datetime > last_file_time:
                    copy_info['file_lists'].append(file_path)
                # 保留最大檔案時間 
                if mtime_datetime > max_file_time:
                    max_file_time = mtime_datetime
    # 最新資料存檔
    copy_info['max_file_time']   = max_file_time.strftime('%Y-%m-%d %H:%M:%S')
    copy_info['last_copy_file']  = ''
    saveCopyInfo()
    saveCopyList()

    if len(copy_info['file_lists'])>0:
        return 0
    else:
        return -1

# 主流程
def main():

    # 載入目前複製 info
    loadCopyInfo()

    # 判斷及取得新的 List
    now_copy_index = getNewCopyList()
    if now_copy_index == -1:
        return True

    # 開始逐個複製
    while now_copy_index<len(copy_info['file_lists']) :
        now_copy_file = copy_info['file_lists'][now_copy_index]
        file_name = os.path.basename(now_copy_file)
        # 先 copy 到 temp 再 move，避免大檔複製到一半被匯入 
        shutil.copy( now_copy_file, temp_path+file_name )
        shutil.move( temp_path+file_name,target_path+file_name )
        copy_info['last_copy_file']  = now_copy_file
        saveCopyInfo()           # 完成複製一筆就存 info，避免程式被中斷

        now_copy_index = now_copy_index + 1

    return True

# 執行主程序
lock = FileLock("batch_zip_copy.txt.lock")
try:
    with lock.acquire(timeout=1):
        main()
except Timeout:
    print("Another instance of this application running.")

