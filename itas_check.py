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
import patoolib
import xml.etree.ElementTree as ET
import json
import base64


#//////////////////////////////////////////////////
# 初始化 

# 取下目前路徑
sys_path = sys.path[0] 
sys_path += '/'

# 系統參數
sys_args = {
    'config_path'    : sys_path + 'config_postgre.csv',                 # 新圖星種參數檔
    'itasConnectStr' : 'DSN=msSQL;UID=sa;PWD=as',                       # 舊圖 SQL Server 連接字串
    'psqlConnectStr' : 'DSN=PostgreSQL35W',                             # 取原 flowctrl 亦用此
    'itasTableName'  : 't_image_2',                                     # SQL 及 Postgre 都使用此名
    'itasImageRoot'  : 'D:\\安康\\AutoImageToDB\\source_ImageFiles\\',  # 圖檔存放根路徑
    'newMinDate'     : '20150101',                                      # 找新圖時依此減少搜尋量(需至安康找各MD最小值填入)
    'nameCheckSize'  : 25                                               # 依名字判斷有無新圖字數
}

# 星種資訊(此處主要取得MD以便開啟搜尋新圖)
config_lines = []
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


#//////////////////////////////////////////////////
# 共用副程式

# 測試透過 odbc32 設定的 DSN connect 是否可行
def TestSQLConnect() :
    ret_bo = False
    cn = pyodbc.connect(sys_args['itasConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "select * FROM "+sys_args['itasTableName']
    sr.execute(_sql)
    rows = sr.fetchall()
    if (len(rows)>0) :
        print('total record:'+str(len(rows)))
        ret_bo = True    

    sr.close()
    cn.close()

    return ret_bo

# 讀取各星種參數
def load_config():
    with open(sys_args['config_path'], 'r',encoding='UTF-8') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            if i>0 :
                config_lines.append([each for each in each_arr])

    return True

# 依星種 id 取得 config 該星種參數
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



# 第一步，將 SQL T_Image_2 搬到 PostgreSQL
def step_1() :
    # 先刪除 PostgreSQL 上資料表，準備搬新的
    cn = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "delete FROM "+sys_args['itasTableName']
    sr.execute(_sql)
    sr.close()
    cn.close()

    # 開啟 SQL Server 資料表，準備逐筆搬入
    cn = pyodbc.connect(sys_args['itasConnectStr'],autocommit=True)
    sr = cn.cursor()
    cn1 = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr1 = cn1.cursor()

    _sql = "select imageid,satid,filmtime,path,sataz FROM "+sys_args['itasTableName']
    sr.execute(_sql)
    rows = sr.fetchall()
    for row in rows:
        _sql1 = "insert into "+sys_args['itasTableName']+" (imageid,satid,filmtime,path,sataz) values("
        _sql1 += str(row[0])+",'"+str(row[1])+"','"+str(row[2])+"','"+str(row[3])+"',"+str(row[4])+")"
        sr1.execute(_sql1)

    sr.close()
    cn.close()
    sr1.close()
    cn1.close()

# 第二步，判斷舊圖是否存在，填入 haveimage 欄位(一併決定ispan欄)
def step_2() :
    cn = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr = cn.cursor()

    _sql = "select path,imageid FROM "+sys_args['itasTableName']
    sr.execute(_sql)
    rows = sr.fetchall()
    for row in rows:
        # sys_args itasImageRoot+path 路徑必須存在，存在填Y，不存在填N
        image_path = (sys_args['itasImageRoot']+row[0]).strip(' ')       # SQL 取得欄位會含空白
        ispan = "N"
        if image_path.lower().find("_pan") != -1 :
            ispan = "Y"
        if os.path.exists(image_path) :
            _sql = "UPDATE "+sys_args['itasTableName']+" SET haveimage='Y'" 
        else:
            _sql = "UPDATE "+sys_args['itasTableName']+" SET haveimage='N'" 
        _sql += ",ispan='"+ispan+"' "
        _sql += " WHERE imageid="+str(row[1])
        sr.execute(_sql)
        sr.commit() 
    sr.close()
    cn.close()

# 第三步A，判斷是否有新圖填入 havenew 欄(此依 path 尾名比對 flowctrl imagename 前數碼)
def step_3A() :
    cn = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr = cn.cursor()
    cn1 = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr1 = cn1.cursor()

    _sql = "select path,imageid from "+sys_args['itasTableName']
    _sql += " where filmtime>='"+sys_args['newMinDate']+"' and coalesce(havenew,'N')<>'Y' and haveimage='Y' "
    sr.execute(_sql)
    rows = sr.fetchall()
    for row in rows:
        image_name = row[0].strip(' ').replace('\\','/').split('/')[-1][0:sys_args['nameCheckSize']]
        havenew = 'N'
        _sql = "select * from (select imagename, position('"+image_name+"' in imagename) as namepos"
        _sql += " from flowctrl) as t1 where namepos>0"
        sr1.execute(_sql)
        rows1 = sr1.fetchall()
        if (len(rows1)>0) :
            havenew = 'Y'
        _sql = "UPDATE "+sys_args['itasTableName']+" SET havenew='"+havenew+"'" 
        _sql += " WHERE imageid="+str(row[1])
        sr.execute(_sql)
        sr.commit() 

    sr1.close()
    cn1.close()
    sr.close()
    cn.close()

# 依條件搜尋 MD 
def searchRasterExist(whereStr,mosaicDataset) :
    count = 0
    with arcpy.da.SearchCursor(mosaicDataset, ['name'], where_clause=whereStr) as cursor:
        for row in cursor:
            count += 1
    if count>0:
        return True
    return False

# 依 satid filmtime 找 MD 中日期為此的
def CheckNewByMD(satid,filmtime) :
    f_filmtime = (filmtime-timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
    e_filmtime = (filmtime+timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
    # print(satid+","+f_filmtime+","+e_filmtime)
    # 依星種 satid 開啟 16bit MD 以便搜尋 MD 中日期有此筆回傳 'Y'

    # 此處寫死，到安康後再確定有哪些 satid，又對應新圖何星種 
    if satid=='WV01':
        if getConfigById('WV01') :
            if searchRasterExist(
                "(acquisitiondate>='"+f_filmtime+"' and acquisitiondate<='"+e_filmtime+"') "+ 
                " or (generationtime>='"+f_filmtime+"' and generationtime<='"+e_filmtime+"') "+ 
                " or (earliestacqtime>='"+f_filmtime+"' and earliestacqtime<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='WV02':
        if getConfigById('WV02') :
            if searchRasterExist(
                "(acquisitiondate>='"+f_filmtime+"' and acquisitiondate<='"+e_filmtime+"') "+ 
                " or (generationtime>='"+f_filmtime+"' and generationtime<='"+e_filmtime+"') "+ 
                " or (earliestacqtime>='"+f_filmtime+"' and earliestacqtime<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='WV03':
        if getConfigById('WV03') :
            if searchRasterExist(
                "(acquisitiondate>='"+f_filmtime+"' and acquisitiondate<='"+e_filmtime+"') "+ 
                " or (generationtime>='"+f_filmtime+"' and generationtime<='"+e_filmtime+"') "+ 
                " or (earliestacqtime>='"+f_filmtime+"' and earliestacqtime<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='WV04':
        if getConfigById('WV04') :
            if searchRasterExist(
                "(acquisitiondate>='"+f_filmtime+"' and acquisitiondate<='"+e_filmtime+"') "+ 
                " or (generationtime>='"+f_filmtime+"' and generationtime<='"+e_filmtime+"') "+ 
                " or (earliestacqtime>='"+f_filmtime+"' and earliestacqtime<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='GE01':
        if getConfigById('GE01') :
            if searchRasterExist(
                "(acquisitiondate>='"+f_filmtime+"' and acquisitiondate<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='BS01':
        if getConfigById('BS01') :
            if searchRasterExist(
                "(acquisitiondate>='"+f_filmtime+"' and acquisitiondate<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='PHR01':
        if getConfigById('PHR01') :
            if searchRasterExist(
                "(TIME_RANGE>='"+f_filmtime+"' and TIME_RANGE<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='SKY01':
        if getConfigById('SKY01') :
            if searchRasterExist(
                "(acquired>='"+f_filmtime+"' and acquired<='"+e_filmtime+"') "+
                " or (published>='"+f_filmtime+"' and published<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
    elif satid=='PS01':
        # 至安康查
        return 'N'
    else:
        if getConfigById('Other01') :
            if searchRasterExist(
                "(acq_time>='"+f_filmtime+"' and acq_time<='"+e_filmtime+"') "+ 
                " or (receive_time>='"+f_filmtime+"' and receive_time<='"+e_filmtime+"') "
                ,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']) :
                    return 'Y'
            
    return 'N'

# 第三步B，判斷是否有新圖填入 havenew 欄(此改用 filmtime 搜尋星種 MD 中的 acquistiondate)
def step_3B():
    cn = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr = cn.cursor()
    cn1 = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr1 = cn1.cursor()

    _sql = "select filmtime,imageid,satid from "+sys_args['itasTableName']
    _sql += " where filmtime>='"+sys_args['newMinDate']+"' and coalesce(havenew,'N')<>'Y' and haveimage='Y' "
    sr.execute(_sql)
    rows = sr.fetchall()
    for row in rows:
        filmtime = row[0].strip(' ')
        satid = row[2].strip(' ')
        # 依 satid filmtime 找 MD 中日期為此的
        havenew = CheckNewByMD( satid, datetime.strptime(filmtime,'%Y%m%d%H%M%S') )
        _sql = "UPDATE "+sys_args['itasTableName']+" SET havenew='"+havenew+"'" 
        _sql += " WHERE imageid="+str(row[1])
        sr.execute(_sql)
        sr.commit() 

    sr1.close()
    cn1.close()
    sr.close()
    cn.close()


# 第四步，統計各筆資料現況
def step_4() :
    cn = pyodbc.connect(sys_args['psqlConnectStr'],autocommit=True)
    sr = cn.cursor()

    _sql = "select satid,havenew,ispan,haveimage FROM "+sys_args['itasTableName']
    _sql += " order by satid"
    sr.execute(_sql)
    rows = sr.fetchall()
    t_count = t_new = t_pan = t_noimage = t_turn = 0
    m_count = m_new = m_pan = m_noimage = m_turn = 0
    p_satid = ''
    for row in rows:
        satid = str(row[0]).strip(' ')
        havenew = str(row[1]).strip(' ')
        ispan = str(row[2]).strip(' ')
        haveimage = str(row[3]).strip(' ')
        if p_satid=='':
            p_satid = satid
        if p_satid!=satid :
            print('\n星種:'+p_satid+',資料筆數:'+str(m_count)+',新圖已有數:'+str(m_new)+',路徑中無檔案數:'+str(m_noimage)+',PAN檔數:'+str(m_pan)+',有效待轉數:'+str(m_turn))
            m_count = m_new = m_pan = m_noimage = m_turn = 0
            p_satid=satid
        t_count = t_count + 1
        m_count = m_count + 1
        if havenew == 'Y' :
            t_new = t_new + 1
            m_new = m_new + 1
        if haveimage != 'Y':
            t_noimage = t_noimage + 1
            m_noimage = m_noimage + 1
        if haveimage == 'Y' and ispan == 'Y':
            t_pan = t_pan + 1
            m_pan = m_pan + 1
        if havenew != 'Y' and haveimage == 'Y' and ispan != 'Y':
            t_turn = t_turn + 1
            m_turn = m_turn + 1

    print('\n星種:'+p_satid+',資料筆數:'+str(m_count)+',新圖已有數:'+str(m_new)+',路徑中無檔案數:'+str(m_noimage)+',PAN檔數:'+str(m_pan)+',有效待轉數:'+str(m_turn))
    print('\n\n合計總筆數:'+str(t_count)+',新圖已有總數:'+str(t_new)+',路徑中無檔案總數:'+str(t_noimage)+',PAN檔總數:'+str(t_pan)+',有效待轉總數:'+str(t_turn))

    sr.close()
    cn.close()

#//////////////////////////////////////////////////
# 主程式
def main():

    # 讀取各星種參數後用
    load_config()

    try:

        # 第一步，將 SQL T_Image_2 搬到 PostgreSQL
        step_1()

        # 第二步，判斷舊圖是否存在，填入 haveimage 欄位(一併決定ispan欄)
        step_2()

        # 第三步A，判斷是否有新圖填入 havenew 欄(此依 path 尾名比對 flowctrl imagename 前數碼)
        step_3A()

        # 第三步B，判斷是否有新圖填入 havenew 欄(此改用 filmtime 搜尋星種 MD 中的 acquistiondate)
        step_3B()

        # 第四步統計各筆資料現況
        step_4()

    except Exception as e:
        err_msg = repr(e)
        print(err_msg)
        return False

    return True

# 執行主程序
main()

