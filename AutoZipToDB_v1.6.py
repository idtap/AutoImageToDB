# -*- coding: big5 -*-
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

#//////////////////////////////////////////////////
# 資料定義

# 取下目前路徑
sys_path = sys.path[0] 
sys_path += '/'

# zip 檔路徑
zips_path        = sys_path + 'input_zips/'
tempZip_path     = sys_path + 'tempZip/'

# zip history 資料
histfile = sys_path + 'History.csv'
histlines = []

# 星種參數檔
config_path = sys_path + 'config.csv'
config_lines = []

# 定義目前判定處理的影像內容
nowImage_args = {
    'rasterType_id'    : 'WV03',       
    'rasterType_name'  : 'WorldView-3',
    'bits'             : '16',                           # 定義此星種bits(8/16)
    'filter'           : '.IMD',                         # 定義目前處理影像的 metafile 附加名
    'pansharpen'       : 'Y',                            # 定義目前處理影像是否需做 pansharpen
    'gdbName'          : sys_path+'rasterStore.gdb',
    'fileStore_16'     : 'D:\安康\AutoImageToDB\rasterStore.gdb,D:\安康\AutoImageToDB\source_ImageFiles\WV03\16bit',
    'fileStore_8'      : 'D:\安康\AutoImageToDB\source_ImageFiles\WV03\8bit',
    'datasetName_16'   : 'WV03_16',
    'datasetName_8'    : 'WV03_8',
    'panBit_1'         : '3',
    'panBit_2'         : '2',
    'panBit_3'         : '1',
    'panBit_4'         : '4',
    'pathPAN'          : '',                             # PanSharpening 檔名(此動態搜尋後填入)
    'pathMUL'          : ''
}

#///////////////////////////////////////////////////////////////////////////////////
# 共用

#////////////////////////////////////////////////////
def fprint(w_str) :
    print( w_str ) 
    theTime_e = datetime.now()
    with open(sys_path+'LOG.txt', 'a') as f:
        f.write( '['+datetime.now().strftime('%Y/%m/%d %H:%M:%S')+'] '+w_str )

#////////////////////////////////////////////////////
# 讀取各星種參數後用
def load_config():
    with open(config_path, 'r') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            if i>0 :
                config_lines.append([each for each in each_arr])
    #for each_line in config_lines:
    #    print(each_line[1])  # 試列出各筆第二欄

#////////////////////////////////////////////////////
# 讀入 history
def read_history():
    if os.path.exists(histfile):
        with open(histfile,'r') as f:
           for line in f:
              line = line.strip('\n')
              histlines.append(line)
           f.close()

#////////////////////////////////////////////////////
# 寫出 history
def save_history():
    with open(histfile, 'w') as f:
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
            nowImage_args['fileStore_16']     = line[6]
            nowImage_args['fileStore_8']      = line[7]
            nowImage_args['datasetName_16']   = line[8]
            nowImage_args['datasetName_8']    = line[9]
            nowImage_args['panBit_1']         = line[10]
            nowImage_args['panBit_2']         = line[11]
            nowImage_args['panBit_3']         = line[12]
            nowImage_args['panBit_4']         = line[13]
    return bo

#///////////////////////////////////////////////////////
# 找出檔案中字串位置
def findFileStrPos( file_name, find_str ) :
    filea = open(file_name, "r")        
    fileaString = filea.read()               
    idFilter = find_str            
    idPosition = fileaString.find(idFilter)  
    #filea.seek(idPosition+33,0)              
    #str = filea.read(4)               
    filea.close()

    return idPosition

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

#////////////////////////////////////////////////////
# 判斷星種
def checkImageType(image_root_path):
    # 處理邏輯，判斷出 Raster Type 後從 config.csv 搜尋取得 id 及其他參數
    bo = False
    if not bo : bo = checkWorldView2( image_root_path )         # 找 WorldView2
    if not bo : bo = checkWorldView3( image_root_path )         # 找 WorldView3
    if not bo : bo = checkWorldView4( image_root_path )         # 找 WorldView4
    return bo

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
# 將 image 新增到 gdb/imageDataset
def addRasterToDataset( ras_type, file_path, file_name, dataset_name, filter ):

    time_f = datetime.now()
    fprint( '執行 addRasterToDataset:' )

    # 設定 AddRasterToMosaicDataset
    mdname  = dataset_name
    rastype = ras_type                             
    inpath  = file_path + "/" + file_name
    updatecs = "UPDATE_CELL_SIZES"
    updatebnd = "UPDATE_BOUNDARY"
    updateovr = "#"        # "UPDATE_OVERVIEWS"
    maxlevel = "#"                         # "2"
    maxcs = "0"
    maxdim = "1500"
    spatialref = "#"                       # sys_path + "w18n.prj"
    inputdatafilter = "*"+filter  
    subfolder = "SUBFOLDERS"               # "NO_SUBFOLDERS"
    duplicate = "EXCLUDE_DUPLICATES"       # 排除重複轉
    #duplicate = "ALLOW_DUPLICATES"       # 重複轉

    buildpy = "BUILD_PYRAMIDS"             # 優化運算                  -> 加優化及統計，有些圖不能轉
    #buildpy = "#"             # 優化運算

    calcstats = "CALCULATE_STATISTICS"     # 統計
    #calcstats = "#"     # 統計

    buildthumb = "BUILD_THUMBNAILS"           # "NO_THUMBNAILS"
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

#/////////////////////////////////////////////////////
# 找 MetaData 檔案
def getMetaDataPath( sour_imagePath ) :

    # 不同星種有不同找法，此處因只有 WordView，暫直接找
    # 有 PanSharpen 亦一併將 PAN 檔找出

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
        return False 

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
        return False 

    return True

#////////////////////////////////////////////////////
# pansharpen 
def pansharpenImage( targ_rasterTemp ):

    time_f = datetime.now()
    fprint( '執行 PanSharpening:' )

    # 清除暫存
    arcpy.Delete_management(targ_rasterTemp)

    # 開始做 PanSharpening
    arcpy.management.CreatePansharpenedRasterDataset(
        nowImage_args['pathMUL'], 
        int(nowImage_args['panBit_1']), int(nowImage_args['panBit_2']), int(nowImage_args['panBit_3']), int(nowImage_args['panBit_4']),
        targ_rasterTemp,
        nowImage_args['pathPAN'], 
        "Gram-Schmidt", 0.38, 0.25, 0.2, 0.16, nowImage_args['rasterType_name'])

    #CalculateStatistics(sourRaster,"","","")
    #BuildPyramids(sourRaster)

    time_e = datetime.now()
    fprint( "費時幾秒:{0}\n".format( str((time_e-time_f).seconds )) )

    return True

#////////////////////////////////////////////////////
# 16bit轉8bit
def copyRaster(sourRaster,targRaster, bits, scale, format):

    time_f = datetime.now()
    fprint( '執行 CopyRaster:' )

    arcpy.Delete_management(targRaster)
    #arcpy.CopyRaster_management( sourRaster, targRaster,
    #    "#","#","#","NONE","NONE","8 bit unsigned","NONE","NONE")
    arcpy.management.CopyRaster(
        sourRaster, 
        targRaster, '', None, '', "NONE", "NONE", 
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
# 圖名重復自動更名
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

#////////////////////////////////////////////////////////////////////////////////////////////
# 主流程
def main():

    # 讀取各星種參數後用
    load_config()

    # 讀入 zip 轉入後的 history 檔，供判斷是否轉入過依據
    read_history()

    # 搜尋路徑下的 zip 檔，逐一判斷未轉過(History.csv中無此筆)的轉
    allfiles = os.listdir(zips_path+'.')
    arr_zip = [ fname for fname in allfiles if fname.lower().endswith('.zip')]            # 取 zip 路徑下各 zip 檔到 list 中
    for zip_name in arr_zip:
       try:

           if zip_name in histlines:
               fprint( '此筆已轉過:'+zip_name+'\n' )
           else:
               fprint( '目前轉入:'+zip_name+'\n' )

               theTime_f = datetime.now()
               fprint( '此檔轉換開始時間:'+theTime_f.strftime('%Y/%m/%d %H:%M')+'\n'  )

               # 開啟 zip 檔
               zfile = zipfile.ZipFile(zips_path + zip_name,'r')

               # 解壓縮此zip到 Temp 路徑
               #        解壓縮路徑
               tempzip_dir = tempZip_path + os.path.splitext(zip_name)[0]
               #        存在則先刪除(當成重轉，此要確定 addrastertodataset 不會有誤)
               if os.path.isdir(tempzip_dir):
                  shutil.rmtree(tempzip_dir)
               #        建立路徑(檔案必需有一根路徑存放，避免客戶直接用檔案壓縮)
               os.mkdir(tempzip_dir)
               #        解壓縮到 tempzip_dir，準備判讀星種
               zfile.extractall(tempzip_dir)   # extract file to dir

               # 判斷星種
               if checkImageType(tempzip_dir):
                  fprint('判斷出此圖星種為:'+nowImage_args['rasterType_name']+'\n')
               else:
                  fprint('此 zip 圖檔無法判讀星種\n')
                  continue

               # 圖名重複改檔名沒用(可能IMD等檔內容也要改，故此系統暫為圖名唯一，轉時覆蓋，並不自動更名)
               ## 暫找 tempzip 路徑 MetaData 檔案
               #if not getMetaDataPath( tempzip_dir ) :
               #    fprint('此 zip 無 Meta 檔，無法繼續作業，請檢查:\n')
               #    continue
               #
               ## 圖名重複及自動更名檢查
               #if searchRasterExist( "Name='"+os.path.basename(nowImage_args['pathMUL']).split('.')[0]+"'",
               #                      nowImage_args['gdbName']+'/'+nowImage_args['datasetName_16']) :
               #    AutoFileName( tempzip_dir, os.path.basename(nowImage_args['pathMUL']).split('.')[0],
               #                  nowImage_args['gdbName']+'/'+nowImage_args['datasetName_16'] )

               # 此星種若是 16 bit 則需加轉 8 bit 相關處理
               if nowImage_args['bits'] == '16' :

                   # 移轉圖檔到該星種 16 bit 存放區
                   file_dir = nowImage_args['fileStore_16'] + '/' + os.path.splitext(zip_name)[0]
                   fprint('圖檔移動到各星種16 bit存放:'+file_dir+'\n')
                   if os.path.isdir(file_dir):
                      shutil.rmtree(file_dir)
                   shutil.move(tempzip_dir,nowImage_args['fileStore_16'])

                   # 找 MetaData 檔案
                   if not getMetaDataPath( file_dir ) :
                       fprint('此路徑無 Meta 檔，無法繼續作業，請檢查:\n'+file_dir+'\n')
                       continue

                   # 轉此星種到 16bit mosaic dataset，此無論有無 8 bit 都需原檔轉入
                   fprint( '轉入16bit Mosaic Dataset\n' )
                   addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                       nowImage_args['gdbName'] + '/' + nowImage_args['datasetName_16'],
                                       nowImage_args['filter'] )

                   # 需 pansharpen 星種則執行 pan-sharpen
                   tempRaster_16 = nowImage_args['gdbName']+'/tempPan_16'
                   if (nowImage_args['pansharpen'] == 'Y'):
                       pansharpenImage(tempRaster_16)

                   # 轉16bit到8bit
                   tempRaster_8 = nowImage_args['gdbName']+'/tempPan_8'
                   fprint( '16bit轉8bit:'+tempRaster_8+'\n' )
                   if (nowImage_args['pansharpen'] == 'Y'):
                       copyRaster(tempRaster_16, tempRaster_8, "8_BIT_UNSIGNED", "ScalePixelValue", "GRID")
                   else:                        # 讀取此筆16bit Raster 轉存至如上pansharpen結果
                       copyRaster(nowImage_args['pathMUL'], tempRaster_8, "8_BIT_UNSIGNED", "NONE", "TIFF")

                   # 轉存到 8 bit 存放路徑
                   dir_8bit = nowImage_args['fileStore_8'] + '/' + os.path.splitext(zip_name)[0]
                   if os.path.isdir(dir_8bit):
                      shutil.rmtree(dir_8bit)
                   os.mkdir(dir_8bit)
                   file_8bit = dir_8bit + '/' + os.path.basename(nowImage_args['pathMUL']).split('.')[0] + '.TIF'
                   copyRaster(tempRaster_8, file_8bit, "8_BIT_UNSIGNED", "NONE", "TIFF")

                   # 轉入 8 bit mosaic dataset
                   fprint( '轉入8bit Mosaic Dataset:'+nowImage_args['gdbName']+'/'+nowImage_args['datasetName_8']+'\n' )
                   addRasterToDataset( "Raster Dataset", dir_8bit, '',
                                       nowImage_args['gdbName']+'/'+nowImage_args['datasetName_8'], ".TIF" )

                   # 轉完在 zips_path/history.csv 寫入一筆
                   zfile.close()
                   histlines.append(zip_name)

                   # 將 zip 檔移動到各星種 16 bit 存放區
                   fprint( '轉完，將 zip 檔移動到各星種:'+nowImage_args['fileStore_16']+'/'+zip_name+'\n' )
                   shutil.move( zips_path+zip_name, nowImage_args['fileStore_16']+'/'+zip_name )


               # 否則處理 8 bit
               else :

                   # 移轉圖檔到該星種 8 bit 存放區
                   file_dir = nowImage_args['fileStore_8'] + '/' + os.path.splitext(zip_name)[0]
                   fprint('圖檔移動到各星種8 bit存放:'+file_dir+'\n')
                   if os.path.isdir(file_dir):
                      shutil.rmtree(file_dir)
                   shutil.move(tempzip_dir,nowImage_args['fileStore_8'])

                   # 轉此星種到 8bit mosaic dataset
                   fprint( '轉入8bit Mosaic Dataset\n' )
                   addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                       nowImage_args['gdbName'] + '/' + nowImage_args['datasetName_8'],
                                       nowImage_args['filter'] )

                   # 轉完在 zips_path/history.csv 寫入一筆
                   zfile.close()
                   histlines.append(zip_name)

                   # 將 zip 檔移動到各星種 8 bit 存放區
                   fprint( '轉完，將 zip 檔移動到各星種:'+nowImage_args['fileStore_8']+'/'+zip_name+'\n' )
                   shutil.move( zips_path+zip_name, nowImage_args['fileStore_8']+'/'+zip_name )

               theTime_e = datetime.now()
               fprint( '此檔轉換結束時間:'+theTime_e.strftime('%Y/%m/%d %H:%M')+'\n'  )
               fprint( "轉此ZIP費時幾秒:{0}\n".format( str((theTime_e-theTime_f).seconds )) )

       except Exception as e:
           fprint('\n發生錯誤:')
           fprint(e)

    # 最後將 history 存回
    save_history()

    return True


# 執行主程序

runCheckFile = sys_path+'isrun.txt'
if not os.path.exists(runCheckFile):
    open(runCheckFile,"w+").close()
    main()
    os.remove(runCheckFile)

