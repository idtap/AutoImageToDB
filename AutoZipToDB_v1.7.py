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


#//////////////////////////////////////////////////
# ��Ʃw�q

# ���U�ثe���|
sys_path = sys.path[0] 
sys_path += '/'

# zip �ɸ��|
zips_path        = sys_path + 'input_zips/'
tempZip_path     = sys_path + 'tempZip/'

# zip history ���
histfile = sys_path + 'History.csv'
histlines = []

# �P�ذѼ���
config_path = sys_path + 'config.csv'
config_lines = []

# �w�q�ثe�P�w�B�z���v�����e
nowImage_args = {
    'rasterType_id'    : 'WV03',       
    'rasterType_name'  : 'WorldView-3',
    'bits'             : '16',                           # �w�q���P��bits(8/16)
    'filter'           : '.IMD',                         # �w�q�ثe�B�z�v���� metafile ���[�W
    'pansharpen'       : 'Y',                            # �w�q�ثe�B�z�v���O�_�ݰ� pansharpen
    'gdbName'          : sys_path+'rasterStore.gdb',
    'fileStore_16'     : 'D:\�w�d\AutoImageToDB\rasterStore.gdb,D:\�w�d\AutoImageToDB\source_ImageFiles\WV03\16bit',
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

# �w�q�y�{���� dbLOG connect string( MS-Access .mdb)
logConnectStr = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\LOG.mdb'

#///////////////////////////////////////////////////////////////////////////////////
# �@��

#////////////////////////////////////////////////////
def fprint(w_str) :
    print( w_str ) 
    theTime_e = datetime.now()
    with open(sys_path+'LOG.txt', 'a') as f:
        f.write( '['+datetime.now().strftime('%Y/%m/%d %H:%M:%S')+'] '+w_str )

#////////////////////////////////////////////////////
# Ū���U�P�ذѼƫ��
def load_config():
    with open(config_path, 'r') as record_read:
        reader = csv.reader(record_read)
        for i, each_arr in enumerate(reader):
            if i>0 :
                config_lines.append([each for each in each_arr])
    #for each_line in config_lines:
    #    print(each_line[1])  # �զC�X�U���ĤG��

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

#////////////////////////////////////////////////////
# �P�_�P��
def checkImageType(image_root_path):
    # �B�z�޿�A�P�_�X Raster Type ��q config.csv �j�M���o id �Ψ�L�Ѽ�
    bo = False
    if not bo : bo = checkWorldView2( image_root_path )         # �� WorldView2
    if not bo : bo = checkWorldView3( image_root_path )         # �� WorldView3
    if not bo : bo = checkWorldView4( image_root_path )         # �� WorldView4
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
    duplicate = "EXCLUDE_DUPLICATES"       # �ư�������
    #duplicate = "ALLOW_DUPLICATES"       # ������

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
def getMetaDataPath( sour_imagePath ) :

    # ���P�P�ئ����P��k�A���B�]�u�� WordView�A�Ȫ�����
    # �� PanSharpen ��@�ֱN PAN �ɧ�X

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
        return False 

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
        return False 

    return True

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

#/////////////////////////////////////////////////////
# �� dbLOG �θ��|��ƨM�w�U�@�ӫ����ɮסA�M���ഫ

def getNextZip() :

    # �����o zip ���|�U�U zip
    allfiles = os.listdir(zips_path+'.')
    # ���� array �� 
    arr_zip = [ fname for fname in allfiles if fname.lower().endswith('.zip')]

    # zip ���|���L�ɮ׫h�h�X
    if len(arr_zip) == 0 :
        return ''

    # �����ɮ׹w�]����
    choice_zip = arr_zip[0]

    # �}�� dbLOG�A�v���d LOG �L�����h�s�W
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    for zip_name in arr_zip:
        _sql = "select ZipFileName,Progress from LOG_rasterIO"
        _sql += " where ZipFileName='" + zip_name + "'"
        sr.execute(_sql)
        rows = sr.fetchall()
        # �L�� zip �h LOG �s�W�@��
        if (len(rows)<=0) :
            _sql = "INSERT INTO LOG_rasterIO VALUES('"+zip_name+"','',0,0,0,'','','','',16)"
            sr.execute(_sql)
            if (sr.rowcount <= 0) :
                fprint('�L�k�s�W������LOG�A���ˬd:'+zip_name)
            sr.commit()
        else :     # �������h�ˬd Progress ���� 0 ��ܤw��L�A�o������
            if rows[0][1] != 0 :
                choice_zip = ''


    # ���B�n�@�֤���u���A�M�w���� zip ���B�z

    sr.close()
    cn.close()

    return choice_zip

#///////////////////////////////////////////////////////////////
# �H�U�� LOG �y�{�ޱ������U�B�J

# ���� dbLOG Step1 (���o�u���A�}�l��)
def dbLOG_Step_1(zip_name, start_time) :
    # �}�� dbLOG�A�ץ� Progress Status StartTime
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=1,Status=2,StartTime='"+start_time+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# ���� dbLOG Step2 (�P�اP�_)
def dbLOG_Step_2(zip_name) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=2,Status=1"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# �q�� Status NoteMsg �]�w
def dbLOG_StatusMsg(zip_name, status, add_msg) :

    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()

    if add_msg != '' :
        _sql = "select NoteMsg from LOG_rasterIO"
        _sql += " where ZipFileName='" + zip_name + "'"
        sr.execute(_sql)
        rows = sr.fetchall()
        msg = rows[0][0] + ';' + add_msg
        _sql = "UPDATE LOG_rasterIO SET Status="+status+",NoteMsg='"+msg+"'"
        _sql += " WHERE ZipFileName='"+zip_name+"'"
        sr.execute(_sql)
    else:
        _sql = "UPDATE LOG_rasterIO SET Status="+status
        _sql += " WHERE ZipFileName='"+zip_name+"'"
        sr.execute(_sql)

    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 3(�ϦW�B�z)
def dbLOG_Step_3(zip_name,image_name) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=3,Status=1,ImageName='"+image_name+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 4(��16bit�s��)
def dbLOG_Step_4(zip_name) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=4,Status=1"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 5 ��J 16 bit MosaicDataset
def dbLOG_Step_5(zip_name) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=5,Status=1"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 6 Pan-Sharpen
def dbLOG_Step_6(zip_name) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=6,Status=1"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 7 16bit �� 8bit
def dbLOG_Step_7(zip_name) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=7,Status=1"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 8 �� 8bit MosaicDataset
def dbLOG_Step_8(zip_name) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=8,Status=1"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# Step 9 ����
def dbLOG_Step_9(zip_name, end_time) :
    cn = pyodbc.connect(logConnectStr)
    sr = cn.cursor()
    _sql = "UPDATE LOG_rasterIO SET Progress=99,Status=2,EndTime='"+end_time+"'"
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
    #read_history()       # ��� dbLOG�A�� History.csv ���ϥ�

    while 1 :

       # �� dbLOG ���u���Ǩ��o�U�@�ӫ��� zip
       zip_name = getNextZip()
       if zip_name == '':
           break

       # �}�l�ഫ�� zip ��
       try:
           theTime_f = datetime.now()
           fprint( '�����ഫ�}�l�ɶ�:'+theTime_f.strftime('%Y/%m/%d %H:%M')+'\n'  )

           # ���� dbLOG Step1 (���o�u���A�}�l��)
           dbLOG_Step_1(zip_name, theTime_f.strftime('%Y/%m/%d %H:%M:%S'))

           # �}�� zip ��
           zfile = zipfile.ZipFile(zips_path + zip_name,'r')

           # �����Y��zip�� Temp ���|
           #        �����Y���|
           tempzip_dir = tempZip_path + os.path.splitext(zip_name)[0]
           #        �s�b�h���R��(������A���n�T�w addrastertodataset ���|���~)
           if os.path.isdir(tempzip_dir):
              shutil.rmtree(tempzip_dir)
           #        �إ߸��|(�ɮץ��ݦ��@�ڸ��|�s��A�קK�Ȥ᪽�����ɮ����Y)
           os.mkdir(tempzip_dir)
           #        �����Y�� tempzip_dir�A�ǳƧPŪ�P��
           zfile.extractall(tempzip_dir)   # extract file to dir

           # ���� dbLOG Step2 (�P�اP�_)
           dbLOG_Step_2(zip_name)

           # �P�_�P��
           if checkImageType(tempzip_dir):
              fprint('�P�_�X���ϬP�ج�:'+nowImage_args['rasterType_name']+'\n')
              dbLOG_StatusMsg(zip_name, '2', '�P��'+nowImage_args['rasterType_name'])
           else:
              fprint('�� zip ���ɵL�k�PŪ�P��\n')
              dbLOG_StatusMsg(zip_name, '3','�����P��')
              continue

           # �ϦW���Ƨ��ɦW�S��(�i��IMD���ɤ��e�]�n��A�G���t�μȬ��ϦW�ߤ@�A����л\�A�ä��۰ʧ�W)
           ## �ȧ� tempzip ���| MetaData �ɮ�
           #if not getMetaDataPath( tempzip_dir ) :
           #    fprint('�� zip �L Meta �ɡA�L�k�~��@�~�A���ˬd:\n')
           #    continue
           #
           ## �ϦW���ƤΦ۰ʧ�W�ˬd
           #if searchRasterExist( "Name='"+os.path.basename(nowImage_args['pathMUL']).split('.')[0]+"'",
           #                      nowImage_args['gdbName']+'/'+nowImage_args['datasetName_16']) :
           #    AutoFileName( tempzip_dir, os.path.basename(nowImage_args['pathMUL']).split('.')[0],
           #                  nowImage_args['gdbName']+'/'+nowImage_args['datasetName_16'] )

           # ���P�حY�O 16 bit �h�ݥ[�� 8 bit �����B�z
           if nowImage_args['bits'] == '16' :

               # ��16bit�s��
               dbLOG_Step_4(zip_name)

               # ������ɨ�ӬP�� 16 bit �s���
               file_dir = nowImage_args['fileStore_16'] + '/' + os.path.splitext(zip_name)[0]
               fprint('���ɲ��ʨ�U�P��16 bit�s��:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_16'])

               dbLOG_StatusMsg(zip_name, '2', '')

               # �� MetaData �ɮ�
               if not getMetaDataPath( file_dir ) :
                   fprint('�����|�L Meta �ɡA�L�k�~��@�~�A���ˬd:\n'+file_dir+'\n')
                   dbLOG_StatusMsg(zip_name, '3', '�L�ϦW')
                   continue

               # ���� dbLOG Step3 (�ϦW�B�z)
               dbLOG_Step_3(zip_name,os.path.basename(nowImage_args['pathMUL']).split('.')[0])
               dbLOG_StatusMsg(zip_name, '2', '�ϦW'+os.path.basename(nowImage_args['pathMUL']).split('.')[0])

               # ��J 16 bit MosaicDataset
               dbLOG_Step_5(zip_name)

               # �হ�P�ب� 16bit mosaic dataset�A���L�צ��L 8 bit ���ݭ�����J
               fprint( '��J16bit Mosaic Dataset\n' )
               addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                   nowImage_args['gdbName'] + '/' + nowImage_args['datasetName_16'],
                                   nowImage_args['filter'] )

               dbLOG_StatusMsg(zip_name, '2', '���J'+nowImage_args['datasetName_16'])

               # Pan-Sharpen
               dbLOG_Step_6(zip_name)

               # �� pansharpen �P�ثh���� pan-sharpen
               tempRaster_16 = nowImage_args['gdbName']+'/tempPan_16'
               if (nowImage_args['pansharpen'] == 'Y'):
                   pansharpenImage(tempRaster_16)

               dbLOG_StatusMsg(zip_name, '2', 'Pan-Sharpen:tempPan_16')

               # 16 �� 8
               dbLOG_Step_7(zip_name)

               # ��16bit��8bit
               tempRaster_8 = nowImage_args['gdbName']+'/tempPan_8'
               fprint( '16bit��8bit:'+tempRaster_8+'\n' )
               if (nowImage_args['pansharpen'] == 'Y'):
                   copyRaster(tempRaster_16, tempRaster_8, "8_BIT_UNSIGNED", "ScalePixelValue", "GRID")
               else:                        # Ū������16bit Raster ��s�ܦp�Wpansharpen���G
                   copyRaster(nowImage_args['pathMUL'], tempRaster_8, "8_BIT_UNSIGNED", "NONE", "TIFF")

               # ��s�� 8 bit �s����|
               dir_8bit = nowImage_args['fileStore_8'] + '/' + os.path.splitext(zip_name)[0]
               if os.path.isdir(dir_8bit):
                  shutil.rmtree(dir_8bit)
               os.mkdir(dir_8bit)
               file_8bit = dir_8bit + '/' + os.path.basename(nowImage_args['pathMUL']).split('.')[0] + '.TIF'
               copyRaster(tempRaster_8, file_8bit, "8_BIT_UNSIGNED", "NONE", "TIFF")

               # �� 8 bit MosaicDataset
               dbLOG_Step_8(zip_name)

               # ��J 8 bit mosaic dataset
               fprint( '��J8bit Mosaic Dataset:'+nowImage_args['gdbName']+'/'+nowImage_args['datasetName_8']+'\n' )
               addRasterToDataset( "Raster Dataset", dir_8bit, '',
                                   nowImage_args['gdbName']+'/'+nowImage_args['datasetName_8'], ".TIF" )

               dbLOG_StatusMsg(zip_name, '2', '��J:'+nowImage_args['datasetName_8'])

               # �৹�b zips_path/history.csv �g�J�@��
               zfile.close()
               histlines.append(zip_name)

               # �N zip �ɲ��ʨ�U�P�� 16 bit �s���
               fprint( '�৹�A�N zip �ɲ��ʨ�U�P��:'+nowImage_args['fileStore_16']+'/'+zip_name+'\n' )
               shutil.move( zips_path+zip_name, nowImage_args['fileStore_16']+'/'+zip_name )


           # �_�h�B�z 8 bit
           else :

               # ������ɨ�ӬP�� 8 bit �s���
               file_dir = nowImage_args['fileStore_8'] + '/' + os.path.splitext(zip_name)[0]
               fprint('���ɲ��ʨ�U�P��8 bit�s��:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_8'])

               # �হ�P�ب� 8bit mosaic dataset
               fprint( '��J8bit Mosaic Dataset\n' )
               addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                   nowImage_args['gdbName'] + '/' + nowImage_args['datasetName_8'],
                                   nowImage_args['filter'] )

               # �৹�b zips_path/history.csv �g�J�@��
               zfile.close()
               histlines.append(zip_name)

               # �N zip �ɲ��ʨ�U�P�� 8 bit �s���
               fprint( '�৹�A�N zip �ɲ��ʨ�U�P��:'+nowImage_args['fileStore_8']+'/'+zip_name+'\n' )
               shutil.move( zips_path+zip_name, nowImage_args['fileStore_8']+'/'+zip_name )

           theTime_e = datetime.now()
           fprint( '�����ഫ�����ɶ�:'+theTime_e.strftime('%Y/%m/%d %H:%M')+'\n'  )
           fprint( "�হZIP�O�ɴX��:{0}\n".format( str((theTime_e-theTime_f).seconds )) )

           # ����
           dbLOG_Step_9(zip_name,theTime_e.strftime('%Y/%m/%d %H:%M:%S'))

       except Exception as e:
           fprint('\n�o�Ϳ��~:')
           fprint(e)
           dbLOG_StatusMsg(zip_name, '3', '���~:'+e)

    # �̫�N history �s�^
    #save_history()              # ��� dbLOG

    return True


# ����D�{��

#runCheckFile = sys_path+'isrun.txt'
#if not os.path.exists(runCheckFile):
#    open(runCheckFile,"w+").close()
#    main()
#    os.remove(runCheckFile)

lock = FileLock("high_ground.txt.lock")

#with lock:
#    open("high_ground.txt", "w").write("You were the chosen one.")   
#    main()
try:
    with lock.acquire(timeout=1):
        main()
except Timeout:
    print("Another instance of this application currently holds the lock.")

