# -*- coding: big5 -*-
# !/usr/bin/env python

# ���ұҰ�
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


#//////////////////////////////////////////////////
# ��Ʃw�q

# ���U�ثe���|
sys_path = sys.path[0] 
sys_path += '/'

# �t�ΰѼ���
syspara_path = sys_path + 'sysparam.csv'
sys_args = {
    'is_MultiRun'    : 'Y',                         # �h�u����
    'is_sdeDB'       : 'Y',                         # �ϥ� sde ��Ʈw
    'limit_doFiles'  : 3,                           # �C������B�z�X����(�t�X�C5��������@���A�H����P�ɰ����)
    'zips_path'      : sys_path + 'input_zips/',    # zip �ɦs����|
                                                    # �y�{������ connect string( MS-Access .mdb)
    'flowConnectStr' : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',
    'config_path'    : sys_path + 'config_sde.csv',     # �P�ذѼ���
    'defaultPriority': '9999'                             # �w�]�u����
}

# �t�μȦs�θ��|
tempZip_path     = sys_path + 'tempZip/'

# zip history ���
histfile = sys_path + 'History.csv'
histlines = []

# �P�ظ�Ʀ�
config_lines = []

# �t�Υ� gdb
sys_gdb = sys_path + 'sysStore.gdb'

# �w�q�ثe�P�w�B�z���v�����e
nowImage_args = {
    'rasterType_id'    : 'WV03',       
    'rasterType_name'  : 'WorldView-3',
    'bits'             : '16',                           # �w�q���P��bits(8/16)
    'filter'           : '.IMD',                         # �w�q�ثe�B�z�v���� metafile ���[�W
    'pansharpen'       : 'Y',                            # �w�q�ثe�B�z�v���O�_�ݰ� pansharpen
    'gdbName'          : sys_path+'rasterStore.gdb',
    'fileStore_16'     : 'D:\�w�d\AutoImageToDB\source_ImageFiles\WV03\16bit',
    'fileStore_8'      : 'D:\�w�d\AutoImageToDB\source_ImageFiles\WV03\8bit',
    'datasetName_16'   : 'WV03_16',
    'datasetName_8'    : 'WV03_8',
    'panBit_1'         : '3',
    'panBit_2'         : '2',
    'panBit_3'         : '1',
    'panBit_4'         : '4',
    'pathPAN'          : '',                             # PanSharpening �ɦW(���ʺA�j�M���J)
    'pathMUL'          : ''
}

#///////////////////////////////////////////////////////////////////////////////////
# �@��

#////////////////////////////////////////////////////
def fprint(w_str) :
    print( w_str ) 
    theTime_e = datetime.now()
    with open(sys_path+'LOG.txt', 'a') as f:
        f.write( '['+datetime.now().strftime('%Y/%m/%d %H:%M:%S')+'] '+w_str )

#////////////////////////////////////////////////////
# Ū���t�ΰѼ�
def load_sys_param():
    # Ū�J�ɮ�
    lines = []
    with open(syspara_path, 'r') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            if i>0 :    # ���欰���Ѹ��L
                lines.append([each for each in each_arr])
    # ��Ķ�� sys_args
    sys_args['is_MultiRun']     = lines[0][0]
    sys_args['is_sdeDB']        = lines[0][1]
    sys_args['limit_doFiles']   = int(lines[0][2])
    sys_args['zips_path']       = lines[0][3]
    sys_args['flowConnectStr']  = lines[0][4]
    sys_args['config_path']     = lines[0][5]
    sys_args['defaultPriority'] = lines[0][6]

    return True

#////////////////////////////////////////////////////
# Ū���U�P�ذѼƫ��
def load_config():
    with open(sys_args['config_path'], 'r') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            if i>0 :
                config_lines.append([each for each in each_arr])

    return True

#////////////////////////////////////////////////////
# Ū�J history
def read_history():
    if os.path.exists(histfile):
        with open(histfile,'r') as f:
           for line in f:
              line = line.strip('\n')
              histlines.append(line)
           f.close()

#////////////////////////////////////////////////////
# �g�X history
def save_history():
    with open(histfile, 'w') as f:
        for line in histlines:
           f.write(line+'\n')
        f.close()

#////////////////////////////////////////////////////
# �� id ���o config �ӬP�ذѼ�
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
# ��X�ɮפ��r���m
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
# �P�_�O�_�O WordView2 �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
def checkWorldView2( check_path ):
    # �� .IMD meta ��
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # �����h�}�l�ˬd
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "WV02"')
    # pos>-1 �h���
    bo = False
    if pos>-1 :
        bo = getConfigById('WV02')
    return bo

#/////////////////////////////////////////////////////////
# �P�_�O�_�O WordView3 �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
def checkWorldView3( check_path ):
    # �� .IMD meta ��
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # �����h�}�l�ˬd
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "WV03"')
    # pos>-1 �h���
    bo = False
    if pos>-1 :
        bo = getConfigById('WV03')
    return bo

#////////////////////////////////////////////////////////
# �P�_�O�_�O WordView4 �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
def checkWorldView4( check_path ):
    # �� .IMD meta ��
    imd_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.IMD'):
                imd_name = fullpath
                #fprint(fullpath)
    # �����h�}�l�ˬd
    if imd_name == '' :
        return False
    pos = findFileStrPos( imd_name, 'satId = "WV04"')
    # pos>-1 �h���
    bo = False
    if pos>-1 :
        bo = getConfigById('WV04')
    return bo

#////////////////////////////////////////////////////////
# �P�_�O�_�O GeoEye-1 �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
def checkGeoEye( check_path ):
    # �� xxxx_metadata.txt ��
    meta_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('_metadata.txt'):                 # GeoEye-1 �H�ɦW��X�� metadata ��
                meta_file_name = fullpath
                #fprint(fullpath)

    # �����h�}�l�ˬd
    if meta_file_name == '' :
        return False
    pos = findFileStrPos( meta_file_name, 'Sensor Name: GeoEye-1')   # ���ɤ��ݦ����~�O GeoEye-1

    # pos>-1 �h���A����� id �� config Ū���������
    bo = False
    if pos>-1 :
        bo = getConfigById('GE01')

    return bo

#////////////////////////////////////////////////////
# �P�_�P��
def checkImageType(image_root_path):
    # �B�z�޿�A�P�_�X Raster Type ��q config.csv �j�M���o id �Ψ�L�Ѽ�
    bo = False
    if not bo : bo = checkWorldView2( image_root_path )         # �� WorldView2
    if not bo : bo = checkWorldView3( image_root_path )         # �� WorldView3
    if not bo : bo = checkWorldView4( image_root_path )         # �� WorldView4
    if not bo : bo = checkGeoEye( image_root_path )             # �� GeoEye-1
    return bo

#////////////////////////////////////////////////////
# Statistics �B��
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
# �N image �s�W�� gdb/imageDataset
def addRasterToDataset( ras_type, file_path, file_name, dataset_name, filter ):

    time_f = datetime.now()
    fprint( '���� addRasterToDataset:' )

    # �]�w AddRasterToMosaicDataset
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
    #duplicate = "EXCLUDE_DUPLICATES"      # �ư�������
    #duplicate = "ALLOW_DUPLICATES"        # ������
    duplicate = "OVERWRITE_DUPLICATES"     # �Ƽg

    buildpy = "BUILD_PYRAMIDS"             # �u�ƹB��                  -> �[�u�Ƥβέp�A���ǹϤ�����
    #buildpy = "#"             # �u�ƹB��

    calcstats = "CALCULATE_STATISTICS"     # �έp
    #calcstats = "#"     # �έp

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
    fprint( "�O�ɴX��:{0}\n".format( str((time_e-time_f).seconds )) )

    return True

#/////////////////////////////////////////////////////
# �� MetaData �ɮ�
# (ps:���^�ǹϦW)
def getMetaDataPath( sour_imagePath ) :

    # ���P�P�ئ����P��k
    # (ps:�� PanSharpen ��@�ֱN PAN �ɧ�X)

    # WorldView �t�C��k
    if nowImage_args['rasterType_name'] in ['WorldView-2','WorldView-3','WorldView-4'] :

        # ���� PAN ��
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
            fprint('�䤣����� PAN �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        # �A�� MUL ��
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
            fprint('�䤣����� MUL �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]

    # GeoEye-1
    if nowImage_args['rasterType_name'] in ['GeoEye-1'] :
        # ���� MUL metadata ��(endswith filter)
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
            fprint('�䤣�즹�P�إ��� MetaDate �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        # �A�� PAN ��
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.ntf') and f.find('_pan_'):     # ntf �ɥB������ pan �r��
                    nowImage_args['pathPAN'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('�䤣�즹�P�إ��� PAN �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        # �ϦW�� pan �ɥh�� pan �r��
        sArr = os.path.basename(nowImage_args['pathPAN']).split('.')[0].split('_')

        return sArr[0]+'_'+sArr[1]+'_'+sArr[3]

    return ''


#////////////////////////////////////////////////////
# pansharpen 
def pansharpenImage( targ_rasterTemp ):

    time_f = datetime.now()
    fprint( '���� PanSharpening:' )

    # �M���Ȧs
    arcpy.Delete_management(targ_rasterTemp)

    # �}�l�� PanSharpening
    arcpy.management.CreatePansharpenedRasterDataset(
        nowImage_args['pathMUL'], 
        int(nowImage_args['panBit_1']), int(nowImage_args['panBit_2']), int(nowImage_args['panBit_3']), int(nowImage_args['panBit_4']),
        targ_rasterTemp,
        nowImage_args['pathPAN'], 
        "Gram-Schmidt", 0.38, 0.25, 0.2, 0.16, nowImage_args['rasterType_name'])

    #CalculateStatistics(sourRaster,"","","")
    #BuildPyramids(sourRaster)

    time_e = datetime.now()
    fprint( "�O�ɴX��:{0}\n".format( str((time_e-time_f).seconds )) )

    return True

#////////////////////////////////////////////////////
# 16bit��8bit
def copyRaster(sourRaster,targRaster, bits, scale, format):

    time_f = datetime.now()
    fprint( '���� CopyRaster:' )

    arcpy.Delete_management(targRaster)
    #arcpy.CopyRaster_management( sourRaster, targRaster,
    #    "#","#","#","NONE","NONE","8 bit unsigned","NONE","NONE")
    arcpy.management.CopyRaster(
        sourRaster, 
        targRaster, '', None, '', "NONE", "NONE", 
        bits, scale, "NONE", format, "NONE", "CURRENT_SLICE", "NO_TRANSPOSE")

    time_e = datetime.now()
    fprint( "�O�ɴX��:{0}\n".format( str((time_e-time_f).seconds )) )

    return True

#////////////////////////////////////////////////////
# �j�M���ϦW�O�_�s�b
def searchRasterExist(whereStr,mosaicDataset) :
    count = 0
    with arcpy.da.SearchCursor(mosaicDataset, ['name'], where_clause=whereStr) as cursor:
        for row in cursor:
            count += 1
    if count>0:
        return True
    return False

#////////////////////////////////////////////////////
# �ϦW���_�۰ʧ�W(�d���ȶȧ�W���D�A���Ȥ��� )
def AutoFileName( file_dir, old_name, dataset ) :
    # ����X�ߤ@�s�W
    i = 1
    new_name = old_name + '_' + str(i)
    while searchRasterExist("name='"+new_name+"'", dataset) :
       i = i + 1
       new_name = old_name + '_' + str(i)

    fprint('�ϦW:'+old_name+',���Ʀ۰ʧ�W��:'+new_name)

    # �A�N file_dir ���|�U�����W�ɮק�W
    for root, dirs, files in walk(file_dir):
        for f in files:
            file_name = os.path.basename(f).split('.')[0]
            if file_name == old_name :
                old_fullpath = join(root, f)
                new_fullpath = old_fullpath.replace(old_name, new_name)
                os.rename(old_fullpath, new_fullpath)
    return True

#////////////////////////////////////////////////////////
# ��FlowCtrl�ޱ���θ��|��ƨM�w�U�@�ӫ����ɮסA�M���ഫ

def getNextZip() :

    # �����o zip ���|�U�U zip
    allfiles = os.listdir(sys_args['zips_path']+'.')
    # ���� array �� 
    arr_zip = [ fname for fname in allfiles if fname.lower().endswith('.zip') or fname.lower().endswith('.rar') or fname.lower().endswith('.7z')]

    # zip ���|���L�ɮ׫h�h�X
    if len(arr_zip) == 0 :
        return ''

    # �����ɮ׹w�]����
    choice_zip = arr_zip[0]

    # �}�� FlowCtrl�A�v���d FlowCtrl �L�����h�s�W
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()

    # Dir ���|��۰ʷs�W�ި���
    # �@�֨��U�ثe���b�઺�P��
    rasterTypeList = []

    # �קK�h�u�U��{���P�� insert�A�@�~�e�� lock
    lock_dir = FileLock("check_dir.txt.lock")
    try:
        with lock_dir.acquire(timeout=30):
            for zip_name in arr_zip:
                _sql = "select ZipFileName,Progress,Status,RasterTypeID from FlowCtrl"
                _sql += " where ZipFileName='" + zip_name + "'"
                sr.execute(_sql)
                rows = sr.fetchall()
                # �L�� zip �h LOG �s�W�@��
                if (len(rows)<=0) :
                    _sql = "INSERT INTO FlowCtrl VALUES('"+zip_name+"','',"+sys_args['defaultPriority']+",'0:�ݿ�','0:���B�z','','','','',16,'','','','','')"
                    sr.execute(_sql)
                    if (sr.rowcount <= 0) :
                        fprint('�L�k�s�W������ FlowCtrl�A���ˬd:'+zip_name)
                    sr.commit()
                else:
                    # progress �D 0�Astatus �D 3 ���P�ثO�d�A��ܦ��P�إ��ഫ��
                    if sys_args['is_sdeDB'] != 'Y' and sys_args['is_MultiRun'] == 'Y' :
                        if (rows[0][1].find('0:') == -1 and rows[0][2].find('3:') == -1 and rows[0][3] not in rasterTypeList):
                            rasterTypeList.append( rows[0][3] )
            lock_dir.release()
    except Timeout:
        return ''

    # ���u���Ǩ��o FlowCtrl 0:�ݿ� ��ƫ�������u����
    _sql = "select ZipFileName,RasterTypeID from FlowCtrl"
    _sql += " where Progress like '%0:%' order by Priority,ZipFileName"
    sr.execute(_sql)
    rows = sr.fetchall()

    # �P�_���O����P�إB�ɮצs�b�Y�i�� 
    choice_zip = ''
    for row in rows:
        if row[0] in arr_zip and not (row[1]!='' and row[1] in rasterTypeList):
            choice_zip = row[0]         # �Ĥ@�ӲŦX���Y�O�u���n�઺
            break

    sr.close()
    cn.close()

    return choice_zip


#///////////////////////////////////////////////////////////////////
# ��P�� zip �ˬd

def haveSameZipProcess(zip_name) :
    ret_bo = False
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "select ZipFileName from FlowCtrl"
    _sql += " where ZipFileName='" + zip_name + "'"
    _sql += " and Progress not like '%0:%'"            # �}�l��g�J1:���u���e�ˬd���A�G���O 0:�A��ܦ��H���m 
    sr.execute(_sql)
    rows = sr.fetchall()
    if (len(rows)>0) :
        ret_bo = True    

    return ret_bo

#///////////////////////////////////////////////////////////////////
# fgdb �P�P�إu�঳�@�Ӧ��@�Ӱ���

def haveSameRasterTypeRun(zip_name,raster_type_id) :
    ret_bo = False
    cn = pyodbc.connect(sys_args['flowConnectStr'])
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

    return ret_bo

#///////////////////////////////////////////////////////////////
# �H�U���y�{�ޱ������U�B�J

# �^�_���A 0:�ݿ�
def FlowCtrl_Step_0(zip_name, rasterTypeID, rasterTypeName) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='0:����',Status='0:���B�z',StartTime='',EndTime='',ErrMsg=''"
    _sql += " ,ImageName='',FileStore_16='',FileStore_8='',MosaicDataset_16='',MosaicDataset_8=''" 
    _sql += " ,RasterType='"+rasterTypeName+"',RasterTypeID='"+rasterTypeID+"'" 
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# ���� FlowCtrl Step1 (���o�u���A�}�l��)
def FlowCtrl_Step_1(zip_name, start_time) :
    # �}�� FlowCtrl�A�ץ� Progress Status StartTime
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='1:���u���}�l��',Status='2:���\',StartTime='"+start_time+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# ���� FlowCtrl Step2 (�P�اP�_)
def FlowCtrl_Step_2(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='2:�P�اPŪ',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
# �����P����
def FlowCtrl_Step_2_RasterType(zip_name, raster_type_id, raster_type, bits) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET RasterTypeId='"+raster_type_id+"',RasterType='"+raster_type+"',ImageBits="+bits
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# �q�� Status ErrMsg �]�w
def FlowCtrl_StatusMsg(zip_name, status, add_msg) :

    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()

    if add_msg != '' :
        _sql = "select ErrMsg from FlowCtrl"
        _sql += " where ZipFileName='" + zip_name + "'"
        sr.execute(_sql)
        rows = sr.fetchall()
        msg = rows[0][0] + ';' + add_msg
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

# Step 3(�ϦW�ˬd)
def FlowCtrl_Step_3(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='3:�ϦW�ˬd',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_3_ImageName(zip_name,image_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET ImageName='"+image_name+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 4(��16bit�s��)
def FlowCtrl_Step_4(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='4:��16bit�s��',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_4_Store(zip_name,file_store) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET FileStore_16='"+file_store+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 5 ��J 16 bit MosaicDataset
def FlowCtrl_Step_5(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='5:�[�J16bitMosaicDataset',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_5_MosaicDataset(zip_name,mosaic_dataset) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET MosaicDataset_16='"+mosaic_dataset+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 6 Pan-Sharpen
def FlowCtrl_Step_6(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='6:Pen-Sharpening',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 7 16bit �� 8bit
def FlowCtrl_Step_7(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='7:16bit��8bit',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 8 �� 8bit �s��
def FlowCtrl_Step_8(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='8:��8bit�s��',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_8_Store(zip_name,file_store) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET FileStore_8='"+file_store+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 9 �� 8bit MosaicDataset
def FlowCtrl_Step_9(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='9:�[�J8bitMosaicDataset',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_9_MosaicDataset(zip_name,mosaic_dataset) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET MosaicDataset_8='"+mosaic_dataset+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 99 ����
def FlowCtrl_Step_99(zip_name, end_time, raster_type_id) :
    cn = pyodbc.connect(sys_args['flowConnectStr'])
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='99:����',Status='2:���\',EndTime='"+end_time+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

#////////////////////////////////////////////////////////////////////////////////////////////
# �D�y�{
def main():

    # Ū���U�P�ذѼƫ��
    load_config()

    # Ū�J zip ��J�᪺ history �ɡA�ѧP�_�O�_��J�L�̾�
    read_history()       # ��� FlowCtrl�ެy�{�A�� History.csv �������d��ѦҡA�]�\����

    process_zip_count = 0
    while process_zip_count < sys_args['limit_doFiles'] :

       # �ѥؿ��� FlowCtrl�u���Ǩ��o�U�@�ӫ��� zip
       zip_name = getNextZip()
       if zip_name == '':
           break

       # �}�l�ഫ�� zip ��
       try:

           fprint('\n���ɶ}�l�ഫ:'+zip_name+'\n')
           theTime_f = datetime.now()

           # �קK��{�Ƿm��P�@�ɡA���o�u���n�� lock
           lock_zip = FileLock("same_zip.txt.lock")
           try:
               with lock_zip.acquire(timeout=30):
                  if haveSameZipProcess(zip_name) :
                     fprint('�o�{�m��ۦP zip �B�z\n')
                     lock_zip.release()
                     continue
                  else:
                     # ���� FlowCtrl Step1 (���o�u���A�}�l��)
                     FlowCtrl_Step_1(zip_name, theTime_f.strftime('%Y/%m/%d %H:%M:%S'))
                     FlowCtrl_StatusMsg(zip_name, '2:���\','')
                     lock_zip.release()
           except Timeout:
               continue

           fprint( '�}�l�ɶ�:'+theTime_f.strftime('%Y/%m/%d %H:%M')+'\n'  )

           # �����Y��zip�� Temp ���|
           tempzip_dir = tempZip_path + os.path.splitext(zip_name)[0]
           if os.path.isdir(tempzip_dir):
               shutil.rmtree(tempzip_dir)
           os.mkdir(tempzip_dir)

           # ����
           if zip_name.endswith(".zip"):      # .zip �μзǤ覡
               # �}�� zip ��
               zfile = zipfile.ZipFile(sys_args['zips_path'] + zip_name,'r')
               zfile.extractall(tempzip_dir)   
               zfile.close()
           else:                              # ��L�� patool
               #���patool
               patoolib.extract_archive(sys_args['zips_path'] + zip_name, outdir=tempzip_dir)

           # ���� Step2 (�P�اP�_)
           FlowCtrl_Step_2(zip_name)

           # �P�_�P��
           if checkImageType(tempzip_dir):
              fprint('�P�_�X���ϬP�ج�:'+nowImage_args['rasterType_name']+'\n')

              # fgdb ���b�g�J�P������e�n�ˬd�P�P�إu�঳�@��
              if sys_args['is_sdeDB'] != 'Y' and sys_args['is_MultiRun'] == 'Y' :
                 lock_check = FileLock("check_rastertype.txt.lock")
                 try:
                     with lock_check.acquire(timeout=30):
                        if haveSameRasterTypeRun(zip_name,nowImage_args['rasterType_id']) :
                           fprint('�o�{�P�P�عϥ���J���A�����ഫ���L\n')
                           FlowCtrl_Step_0(zip_name,nowImage_args['rasterType_id'],nowImage_args['rasterType_name'])
                           lock_check.release()
                           process_zip_count = process_zip_count + 1
                           continue
                        else:
                           # �g�J�P�_���P��
                           FlowCtrl_Step_2_RasterType(zip_name, nowImage_args['rasterType_id'], nowImage_args['rasterType_name'], nowImage_args['bits'])
                           FlowCtrl_StatusMsg(zip_name, '2:���\','')
                           lock_check.release()
                 except Timeout:
                     FlowCtrl_Step_0(zip_name,nowImage_args['rasterType_id'],nowImage_args['rasterType_name'])
                     process_zip_count = process_zip_count + 1
                     continue
              else:
                 # �g�J�P�_���P��
                 FlowCtrl_Step_2_RasterType(zip_name, nowImage_args['rasterType_id'], nowImage_args['rasterType_name'], nowImage_args['bits'])
                 FlowCtrl_StatusMsg(zip_name, '2:���\','')
           else:
              fprint('�� zip ���ɵL�k�PŪ�P��\n')
              FlowCtrl_StatusMsg(zip_name, '3:����','�����P��')
              continue

           # ���� Step3 (�ϦW�B�z)
           FlowCtrl_Step_3(zip_name)

           # ��tempzip�ˬd�Ψ��o�ϦW
           zip_ImageName = getMetaDataPath( tempzip_dir )
           if zip_ImageName == '' :
               fprint('�� zip �L Meta �ɡA�L�k�~��@�~�A���ˬd:\n')
               FlowCtrl_StatusMsg(zip_name, '3:����', '�䤣��ϦW')
               continue

           fprint('���o�ϦW:'+zip_ImageName+'\n')

           # �����ϦW
           FlowCtrl_Step_3_ImageName(zip_name,zip_ImageName)
           FlowCtrl_StatusMsg(zip_name, '2:���\', '')
           
           # ���P�حY�O 16 bit �h�ݥ[�� 8 bit �����B�z
           if nowImage_args['bits'] == '16' :

               # ��16bit�s��
               FlowCtrl_Step_4(zip_name)

               # ������ɨ�ӬP�� 16 bit �s���
               file_dir = nowImage_args['fileStore_16'] + '/' + os.path.splitext(zip_name)[0]
               fprint('���ɲ��ʨ�U�P��16 bit�s��:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_16'])

               FlowCtrl_Step_4_Store(zip_name,file_dir)
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # ��16bit���| MetaData �� Pan �ɮ׫��
               zip_ImageName = getMetaDataPath( file_dir )
               if zip_ImageName == '' :
                   fprint('�����|�L���P�� Meta �ɡA�L�k�~��@�~�A���ˬd:\n'+file_dir+'\n')
                   continue

               # ��J 16 bit MosaicDataset
               FlowCtrl_Step_5(zip_name)

               # �হ�P�ب� 16bit mosaic dataset�A���L�צ��L 8 bit ���ݭ�����J
               fprint( '��J16bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']+'\n' )
               addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                   nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                   nowImage_args['filter'] )

               FlowCtrl_Step_5_MosaicDataset(zip_name,nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'])
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # Pan-Sharpen
               FlowCtrl_Step_6(zip_name)

               # �� pansharpen �P�ثh���� pan-sharpen
               tempPansharpen = sys_gdb+'/R'+datetime.now().strftime("%y%m%d_%H%M%S")
               if (nowImage_args['pansharpen'] == 'Y'):
                   pansharpenImage(tempPansharpen)

               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # 16 �� 8(���L step 7)
               FlowCtrl_Step_8(zip_name)

               # ��16bit��8bit
               dir_8bit = nowImage_args['fileStore_8'] + '/' + os.path.splitext(zip_name)[0]
               if os.path.isdir(dir_8bit):
                  shutil.rmtree(dir_8bit)
               os.mkdir(dir_8bit)
               file_8bit = dir_8bit + '/' + zip_ImageName + '.TIF'
               if (nowImage_args['pansharpen'] == 'Y'):
                   copyRaster(tempPansharpen, file_8bit, "8_BIT_UNSIGNED", "ScalePixelValue", "TIFF")
                   arcpy.Delete_management(tempPansharpen)
               else:                        # Ū������16bit Raster ��s�ܦp�Wpansharpen���G
                   copyRaster(nowImage_args['pathMUL'], file_8bit, "8_BIT_UNSIGNED", "ScalePixelValue", "TIFF")

               FlowCtrl_Step_8_Store(zip_name,dir_8bit)
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # �� 8 bit MosaicDataset
               FlowCtrl_Step_9(zip_name)

               # ��J 8 bit mosaic dataset
               fprint( '��J8bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8']+'\n' )
               addRasterToDataset( "Raster Dataset", dir_8bit, '',
                                   nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'], 
                                   ".TIF" )

               FlowCtrl_Step_9_MosaicDataset(zip_name,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'])
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # �৹�b history.csv �g�J�@��
               histlines.append(zip_name)

               # �N zip �ɲ��ʨ�U�P�� 16 bit �s���
               fprint( '�৹�A�N zip �ɲ��ʨ�U�P��:'+nowImage_args['fileStore_16']+'/'+zip_name+'\n' )
               shutil.move( sys_args['zips_path']+zip_name, nowImage_args['fileStore_16']+'/'+zip_name )

           # �_�h�B�z 8 bit
           else :

               # �� 8bit �s��
               FlowCtrl_Step_8(zip_name)

               # ������ɨ�ӬP�� 8 bit �s���
               file_dir = nowImage_args['fileStore_8'] + '/' + os.path.splitext(zip_name)[0]
               fprint('���ɲ��ʨ�U�P��8 bit�s��:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_8'])

               FlowCtrl_Step_8_Store(zip_name,nowImage_args['fileStore_8'])
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # �� 8 bit MosaicDataset
               FlowCtrl_Step_9(zip_name)

               # �হ�P�ب� 8bit mosaic dataset
               fprint( '��J8bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8']+'\n' )
               addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                   nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_8'],
                                   nowImage_args['filter'] )

               FlowCtrl_Step_9_MosaicDataset(zip_name,nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_8'])
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # �৹�b history.csv �g�J�@��
               zfile.close()
               histlines.append(zip_name)

               # �N zip �ɲ��ʨ�U�P�� 8 bit �s���
               fprint( '�৹�A�N zip �ɲ��ʨ�U�P��:'+nowImage_args['fileStore_8']+'/'+zip_name+'\n' )
               shutil.move( sys_args['zips_path']+zip_name, nowImage_args['fileStore_8']+'/'+zip_name )

           theTime_e = datetime.now()
           fprint( '�����ഫ�����ɶ�:'+theTime_e.strftime('%Y/%m/%d %H:%M')+'\n'  )
           fprint( "�হZIP�O�ɴX��:{0}\n".format( str((theTime_e-theTime_f).seconds )) )

           # ����
           FlowCtrl_Step_99(zip_name,theTime_e.strftime('%Y/%m/%d %H:%M:%S'),nowImage_args['rasterType_id'])

           # �w�B�z�ɮ�+1
           process_zip_count = process_zip_count + 1
           
       except Exception as e:
           fprint('\n�o�Ϳ��~:')
           fprint(str(e))
           FlowCtrl_StatusMsg(zip_name, '3:����', '���~:'+str(e))
           break

    # �̫�N history �s�^
    save_history()         

    return True


# ����D�{��

# ���d license �]�]
#if arcpy.CheckProduct("ArcInfo") != "Available":

# Ū���t�ΰѼ�
load_sys_param()
if sys_args['is_MultiRun'] != 'Y' :         # �W���Ҧ��n����w
    lock = FileLock("high_ground.txt.lock")
    try:
        with lock.acquire(timeout=1):
            main()
    except Timeout:
        print("Another instance of this application currently holds the lock.")
else:
    main()

