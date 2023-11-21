# -*- coding: utf-8 -*-
# !/usr/bin/env python

# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# 康華舊圖自動複製程序
#
# 2022/06/27 備考：
#   1.此程式仿 old_copy.py 用於將康華舊圖將其移動到 ITAS_OLD_IMG(原IDT_OLD_IMG) 下供自動壓縮程式壓縮後匯入 MD
#   2.與 old_copy.py 程式主要不同在原是 list 路徑，此處是用 postgresql t_image_2 表格複製
#   3.copy 到 local 時次路徑命名(亦zip名) satid+'_'+filmtime+'_'+imageid
#     (前者避開後續星種判定，其次參數因匯入後原始圖檔擺放有分年月，imageid供檢核用，後二者有pan時放pan的)
#   4.t_image_2 加一欄 transtate('0'/未複製,'1'/複製中,'2'/已複製)供判斷是否已複製
#   5.itas_old_sysparam(原 old_sysparam)加 t_image_2 過濾條件 satfilt，此處預計先轉 WV01 星種
#   6.WV01 星種會有 _PAN _MUL 或其一，必須同時處理(一併 Copy 及 transtate 設定)

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
syspara_path = sys_path + 'itas_old_sysparam.csv'
sys_args = {
    'satfilt'          : "(satid='WV01')",                   # 星種過濾條件
    'autoZip_path'     : sys_path + 'itas_input_zips/',      # 自動匯入路徑
    'local_T50'        : sys_path + 'itas_local_T50/',       # 複製至待壓zip根路徑
    'copy_tempPath'    : sys_path + "itas_copy_temp",        # copy NAS 到 local 暫存
    'ConnectStr'       : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',  # postgresql connect    
    'min_pathFiles'    : 30,                                 # 自動複製待轉數
    'itasImageRoot'    : 'D:\\安康\\AutoImageToDB\\source_ImageFiles\\'     # 原始圖檔根路徑
}

# 目前處理影像資訊
nowImage_info = {
    'satid'        : '',

    'imageid'      : '',         # 非 _PAN 檔的 path 置於此(可能兩者都有) 
    'filmtime'     : '',
    'path'         : '',
                                 # _PAN 檔的 path 置於此 
    'imageid_pan'  : '',
    'filmtime_pan' : '',
    'path_pan'     : ''
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
    sys_args['satfilt']           = lines[0][0]
    sys_args['autoZip_path']      = lines[0][1]
    sys_args['local_T50']         = lines[0][2]
    sys_args['copy_tempPath']     = lines[0][3]
    sys_args['ConnectStr']        = lines[0][4]
    sys_args['min_pathFiles']     = int(lines[0][5])
    sys_args['itasImageRoot']     = lines[0][6]

    return True


#////////////////////////////////////////////////////////////////////////////////////////////
# 自動複製程序

# 計算自動匯入zip目錄下目前檔案數+已複製檔案數
def countExistImages() :   

    # 先計算待匯入區 zip 數
    allfiles = os.listdir(sys_args['autoZip_path']+'.')
    arr_zip_f = [ fname for fname in allfiles if fname.lower().endswith('.zip') ]
    total_image = len(arr_zip_f)

    # 再計算待壓縮 zip 路徑數
    arr = os.listdir(sys_args['local_T50']+'.')            # 取到 list 中
    for f in arr:
        # 是子目錄才處理
        if not os.path.isdir(sys_args['local_T50']+f):
            continue
        total_image = total_image + 1

    return total_image


# 複製下一個 image 
def copyNextImage() :

    # 開啟 t_image_2 資料表，準備取得下一個可複製檔案複製
    cn = pyodbc.connect(sys_args['ConnectStr'],autocommit=True)
    sr = cn.cursor()

    _sql = "select satid,imageid,filmtime,path from t_image_2"
    _sql += " where "+sys_args['satfilt']+" and coalesce(havenew,'N')<>'Y' and haveimage='Y' and coalesce(transtate,'0')='0' "
    _sql += " order by filmtime desc"     # 越接近現在的先轉
    sr.execute(_sql)
    rows = sr.fetchall()
    if len(rows)<=0 :
        sr.close()
        cn.close()
        return False

    satid    = rows[0][0].strip(' ')
    imageid  = str(rows[0][1])
    filmtime = rows[0][2].strip(' ')
    path     = rows[0][3].strip(' ')

    # 視結尾是 _PAN _MUL 處理 
    nowImage_info['satid'] = satid
    if not path.lower().endswith('_pan') :
        nowImage_info['imageid']  = imageid
        nowImage_info['path']     = path
        nowImage_info['filmtime'] = filmtime
        # 尋找 _PAN 筆填入，以便一併複製後匯入
        # path 去頭尾
        sep = "\\" if "\\" in path else "/"
        parent, name = path.rsplit(sep, 1)
        name = name.split('_MUL')[0]+'_PAN'
        #print(name)
        _sql = "select satid,imageid,filmtime,path from t_image_2"
        _sql += " where "+sys_args['satfilt']+" and coalesce(havenew,'N')<>'Y' and haveimage='Y' and coalesce(transtate,'0')='0' "
        _sql += " and (path like '%"+name+"%')"
        _sql += " order by filmtime desc"     
        sr.execute(_sql)
        rows = sr.fetchall()
        if len(rows)>0 :
            nowImage_info['imageid_pan']  = str(rows[0][1])
            nowImage_info['filmtime_pan'] = rows[0][2].strip(' ')
            nowImage_info['path_pan']     = rows[0][3].strip(' ')
            #print(nowImage_info['path_pan'])
        else:
            nowImage_info['imageid_pan']  = ""
            nowImage_info['filmtime_pan'] = ""
            nowImage_info['path_pan']     = ""
    else:
        nowImage_info['imageid_pan']  = imageid
        nowImage_info['path_pan']     = path
        nowImage_info['filmtime_pan'] = filmtime
        # 有 _PAN 要尋找 _MUL(不一定有，WV01即無)
        # path 去頭尾
        sep = "\\" if "\\" in path else "/"
        parent, name = path.rsplit(sep, 1)
        name = name.split('_PAN')[0]+'_MUL'
        #print(name)
        _sql = "select satid,imageid,filmtime,path from t_image_2"
        _sql += " where "+sys_args['satfilt']+" and coalesce(havenew,'N')<>'Y' and haveimage='Y' and coalesce(transtate,'0')='0' "
        _sql += " and (path like '%"+name+"%')"
        _sql += " order by filmtime desc"     
        sr.execute(_sql)
        rows = sr.fetchall()
        if len(rows)>0 :
            nowImage_info['imageid']  = str(rows[0][1])
            nowImage_info['filmtime'] = rows[0][2].strip(' ')
            nowImage_info['path']     = rows[0][3].strip(' ')
            #print(nowImage_info['path'])
        else:
            nowImage_info['imageid']  = ""
            nowImage_info['filmtime'] = ""
            nowImage_info['path']     = ""

    # 複製前將資料庫此筆 transtate 改為 '1'(雖無多工)
    if nowImage_info['imageid'] != '':
        _sql = "update t_image_2 set transtate='1' "
        _sql += " where imageid="+nowImage_info['imageid']
        sr.execute(_sql)
        sr.commit() 
    if nowImage_info['imageid_pan'] != '':
        _sql = "update t_image_2 set transtate='1' "
        _sql += " where imageid="+nowImage_info['imageid_pan']
        sr.execute(_sql)
        sr.commit() 

    # 開始依 nowImage_info 內容複製檔案
    try: 
        # 先組次路徑名(亦之後匯入的zip名) satid+'_'+filmtime+'_'+imageid
        file_code = nowImage_info['satid']+'_'+nowImage_info['filmtime']+'_'+nowImage_info['imageid']
        if nowImage_info['path_pan'] != '':
            file_code = nowImage_info['satid']+'_'+nowImage_info['filmtime_pan']+'_'+nowImage_info['imageid_pan']
        targ_path = sys_args['copy_tempPath']+file_code
        if os.path.isdir(targ_path) :
            shutil.rmtree(targ_path)

        # 複製
        if nowImage_info['path'] != '':
            sep = "\\" if "\\" in path else "/"
            parent, name = nowImage_info['path'].rsplit(sep, 1)
            sour_path = sys_args['itasImageRoot']+nowImage_info['path']
            to_path = targ_path+'\\'+name
            print('Copy:'+sour_path)
            print('To:'+to_path)
            shutil.copytree(sour_path,to_path)
        if nowImage_info['path_pan'] != '':
            sep = "\\" if "\\" in path else "/"
            parent, name = nowImage_info['path_pan'].rsplit(sep, 1)
            sour_path = sys_args['itasImageRoot']+nowImage_info['path_pan']
            to_path = targ_path+'\\'+name
            print('Copy:'+sour_path)
            print('To:'+to_path)
            shutil.copytree(sour_path,to_path)

        # 複製完再 move，以避免後續自動壓縮程式只壓到部分
        to_path = sys_args['local_T50']+file_code
        if os.path.isdir(to_path) :
            shutil.rmtree(to_path)
        print('Move:'+targ_path)
        print('To:'+to_path)
        shutil.move(targ_path, to_path)
    except OSError as e: 
        print(repr(e))
        sr.close()
        cn.close()
        return False

    # 都完成後要將 transtate 改為 '2'
    if nowImage_info['imageid'] != '':
        _sql = "update t_image_2 set transtate='2' "
        _sql += " where imageid="+nowImage_info['imageid']
        sr.execute(_sql)
        sr.commit() 
    if nowImage_info['imageid_pan'] != '':
        _sql = "update t_image_2 set transtate='2' "
        _sql += " where imageid="+nowImage_info['imageid_pan']
        sr.execute(_sql)
        sr.commit() 

    sr.close()
    cn.close()

    return True

#////////////////////////////////////////////////////////////////////////////////////////////
# 主流程
def main():

    # 無窮迴圈檢查待匯入路徑下檔案數+已複製檔案 < min_pathFiles，則開始複製
    while True:
        count_exists = countExistImages()
        #print('count:'+str(count_exists))
        if count_exists<sys_args['min_pathFiles'] :
            if not copyNextImage() :
                break

    return True


#////////////////////////////////////////////////
# 執行主程序

# 讀取系統參數
load_sys_param()

# 僅可一份執行檢查
lock = FileLock("itas_old_ground.txt.lock")
try:
    with lock.acquire(timeout=1):
        main()
except Timeout:
    print("Another instance of this application running.")

