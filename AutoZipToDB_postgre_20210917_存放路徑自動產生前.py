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
import xml.etree.ElementTree as ET

import json

import base64

#//////////////////////////////////////////////////
# ��Ʃw�q

# ���U�ثe���|
sys_path = sys.path[0] 
sys_path += '/'

# ����p����
run_times_file = sys_path + 'runtimes.txt'

# �t�ΰѼ���
syspara_path = sys_path + 'sysparam_postgre.csv'
sys_args = {
    'logdb_mode'     : '1',                         # �ʱ�LOG��Ʈw����(1/mdb,2/postgre,3/SQL)
    'is_MultiRun'    : 'Y',                         # �h�u����
    'is_sdeDB'       : 'Y',                         # �ϥ� sde ��Ʈw
    'limit_doFiles'  : 3,                           # �C������B�z�X����(�t�X�C5��������@���A�H����P�ɰ����)
    'zips_path'      : sys_path + 'input_zips/',    # zip �ɦs����|
                                                    # �y�{������ connect string( MS-Access .mdb)
    'flowConnectStr' : r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=.\flowControl.mdb',
    'config_path'    : sys_path + 'config_sde.csv',     # �P�ذѼ���
    'defaultPriority': '9999',                          # �w�]�u����
    'zips_stub'      : sys_path + 'zips_stub/',         # zip ���d�s���|
    'sys_store'      : sys_path + 'sysStore.gdb',       # �t�μȦs��
    'store_root_path': '',                              # ���ɦs��ڸ��|
    'zip_broken_path': ''                               # zip ���~�ɲ��ܸ��|
}

# �t�μȦs�θ��|
tempZip_path     = sys_path + 'tempZip/'

# zip history ���
histfile = sys_path + 'History.csv'
histlines = []

# �P�ظ�Ʀ�
config_lines = []

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
    'pathMUL'          : '',
    'raster_id'        : '',                             # �~���+type_id+�y��
    'self_pansharpen'  : 'N'                             # �ۦ� pansharpen ���� RGB MD add raster
}

# �s�ɥΦ۩w���~�T��
step_errmsg = ''

# �ثe�B�z�v�� metadata 
# (ps:���B�Τ@�H�r��O�s�AMD ���h�Υ��T���A�s��)
image_MetaData = {
    'img_id'           : '',                            # Text �v���s��
    'acq_time'         : '',                            # Date ������
    'sun_elev'         : '',                            # Num  �Ӷ�����
    'sun_azu'          : '',                            # Num  �Ӷ�����
    'cloud_rate'       : '',                            # Num  ���л\�v,�̤p0,�̤j1
    'band'             : '',                            # Int  �i�q,����3��4
    'cen_x'            : '',                            # Num  ���߸g��, WGS84 
    'cen_y'            : '',                            # Num  ���߽n��, WGS84 
    'ul_x'             : '',                            # Num  ���W�g��, WGS84 
    'ul_y'             : '',                            # Num  ���W�n��, WGS84 
    'ur_x'             : '',                            # Num  �k�W�g��, WGS84 
    'ur_y'             : '',                            # Num  �k�W�n��, WGS84 
    'll_x'             : '',                            # Num  ���U�g��, WGS84 
    'll_y'             : '',                            # Num  ���U�n��, WGS84 
    'lr_x'             : '',                            # Num  �k�U�g��, WGS84 
    'lr_y'             : '',                            # Num  �k�U�n��, WGS84 
    'ak_num'           : '',                            # Text AK Number, i.e. ... 
    'geomWKT'          : '',                            # Text �v���d�� Polygon ��ܦ�
    'sat_type'         : '',                            # Text �P��
    'receive_time'     : '',                            # Date �v����f�ɶ�
    'image_desc'       : '',                            # Text ���~����
    'row_gsd'          : '',                            # Num  meanCollectedRowGSD 
    'col_gsd'          : '',                            # Num  mean Collected GSD
    'sat_az'           : '',                            # Num  meanSatAz
    'sat_el'           : '',                            # Num  meanSatel
    'metadata'         : '',                            # Text ��IMD������r�s�J
    'gen_time'         : '',                            # Date generation Time
    'path'             : '',                            # Text �s���ɮת�������|
    'band_id'          : '',                            # Text IMD's bandid
    'catalog_id'       : '',                            # Text 12/23/16 Added
    'target_id'        : '',                            # Text mapping targets if not mapping with orders

    # �H�U�¸��PostgreSQL ���õL�A������ img_metadata ���
    'shoot_type'       : '',                            # Text ���ӼҦ�
    'issendmail'       : '',                            # Text �O�_�w�H�H�q��
    'thumbnail'        : '',                            # Text �Y��
    'note'             : '',                            # 
    'rimgid'           : '',                            # �u�W�s���v�� ID 
    'source_type'      : '',     
    'img_ovr'          : '',                            # �O�_�s�@���r��
    'img'              : ''                             # �v�������Y��Ƨ����|
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
    sys_args['logdb_mode']      = lines[0][0]
    sys_args['is_MultiRun']     = lines[0][1]
    sys_args['is_sdeDB']        = lines[0][2]
    sys_args['limit_doFiles']   = int(lines[0][3])
    sys_args['zips_path']       = lines[0][4]
    sys_args['flowConnectStr']  = lines[0][5]
    sys_args['config_path']     = lines[0][6]
    sys_args['defaultPriority'] = lines[0][7]
    sys_args['zips_stub']       = lines[0][8]
    sys_args['sys_store']       = lines[0][9]
    sys_args['store_root_path'] = lines[0][10]
    sys_args['zip_broken_path'] = lines[0][11]

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
            nowImage_args['self_pansharpen']  = line[14]
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

#//////////////////////////////////////////////////////////////////////////////////////////////////////////
# �H�U���U�P�اP�_

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

#///////////////////////////////////////////////////////
# �P�_�O�_�O WordView1 �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
def checkWorldView1( check_path ):
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
    pos = findFileStrPos( imd_name, 'satId = "WV01"')
    # pos>-1 �h���
    bo = False
    if pos>-1 :
        bo = getConfigById('WV01')
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
# �P�_�O�_�O�ª� GeoEye-1 �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
def checkOldGeoEye( check_path ):
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
        bo = getConfigById('oldGE01')

    return bo

#////////////////////////////////////////////////////////
# �P�_�O�_�O�s�� GeoEye-1 �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
# �s���P WorldView �ۦP
def checkNewGeoEye( check_path ):
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
    pos = findFileStrPos( imd_name, 'satId = "GE01"')
    # pos>-1 �h���
    bo = False
    if pos>-1 :
        bo = getConfigById('GE01')
    return bo

#////////////////////////////////////////////////////////
# �P�_�O�_�O BlackSky �P�ءA�O�h�@�֨��o�P�ذѼƦ^��
def checkBlackSky( check_path ):
    # �� xxxx_metadata.json ��
    meta_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('_metadata.json'):                 
                meta_file_name = fullpath
                #fprint(fullpath)

    # �����h�}�l�ˬd
    if meta_file_name == '' :
        return False

    # �}�� json ������r(���B�i�Ȥ� json parser�A��@�� txt find �Y�i)
    pos = findFileStrPos( meta_file_name, '"sensorName" : "Global-8"')   # ���ɤ��ݦ����~�O

    # pos>-1 �h���A����� id �� config Ū���������
    bo = False
    if pos>-1 :
        bo = getConfigById('BS01')            # �^�ǬP�� id

    return bo

#////////////////////////////////////////////////////////
# �P�_�O�_�O PlanetScope �P��
# ps: ���P�_�覡�O���� .tif �ɡA�A���ɦW.xml �O�_�s�b?
#     �O/RGB(�হxml metadata)�A�_/�Y PlanetScope(�u��rasterid path)
def checkPlanetScope( check_path ):
    # �� .tif ��
    tif_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.tif'):                 
                tif_file_name = fullpath

    # �����h�}�l�ˬd
    if tif_file_name == '' :
        return False

    # tif.xml �ɤ��s�b�Y�O
    if not os.path.exists(tif_file_name.split('.')[0]+'.xml') :
        return getConfigById('PS01')            # Ū���P��

    return False

#//////////////////////////////////////////////////////////
# �P�_�O�_�O Pleiades �P��
# ps: ���P�_�覡�O���� .dim �ɡA�A���ɮ׬O�_�� _MS_ _P_ ��
def checkPleiades( check_path ):
    # �� .dim ��
    dim_file_name = ''
    for root, dirs, files in walk(check_path):
        for f in files:
            fullpath = join(root, f)
            if f.endswith('.dim'):                 
                dim_file_name = fullpath

    # �����h�}�l�ˬd
    if dim_file_name == '' :
        return False

    # �ɮצC���� _MS_ _P_ ���Y�O
    bo = False
    ok_n = 0
    for root, dirs, files in walk(check_path):
        for f in files:
            if f.endswith('.tif') and '_MS_' in f:
                ok_n = ok_n + 1                
            if f.endswith('.tif') and '_P_' in f:
                ok_n = ok_n + 1 
    if ok_n >= 2 :                           
        return getConfigById('PHR01')            # Ū���P�ظ��

    return bo


#////////////////////////////////////////////////////
# �P�_�P��
def checkImageType(image_root_path):
    # �B�z�޿�A�P�_�X Raster Type ��q config.csv �j�M���o id �Ψ�L�Ѽ�
    bo = False
    if not bo : bo = checkWorldView1( image_root_path )         # �� WorldView1
    if not bo : bo = checkWorldView2( image_root_path )         # �� WorldView2
    if not bo : bo = checkWorldView3( image_root_path )         # �� WorldView3
    if not bo : bo = checkWorldView4( image_root_path )         # �� WorldView4
    if not bo : bo = checkOldGeoEye( image_root_path )          # ���ª� GeoEye-1
    if not bo : bo = checkNewGeoEye( image_root_path )          # ��s�� GeoEye-1
    if not bo : bo = checkPleiades( image_root_path )           # �� Pleiades
    if not bo : bo = checkBlackSky( image_root_path )           # �� BlackSky
    if not bo : bo = checkPlanetScope( image_root_path )        # �� PlanetScope
    return bo


#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# metadata ����

#//////////////////////////////////////////////
# �� MUL PAN 
# ps:�^�ǹ����ɦW
def getMetaDataName( sour_imagePath ) :

    # ���P�P�ئ����P��k
    # (ps:�� PanSharpen ��@�ֱN PAN �ɧ�X)

    # WorldView-1 �] ArcGIS �L�� type�A��� xml �H RGB �B�z
    if nowImage_args['rasterType_name'] in ['WorldView-1'] :
        # ���� PAN ��
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith( nowImage_args['filter'] ):
                    pos = findFileStrPos( fullpath, 'bandId = "P"')
                    if pos>-1 :
                        # ����n��� .tif �H�K��ۦ� pansharpen
                        tif_name = fullpath.split('.')[0] + '.tif'
                        if os.path.exists(tif_name) :
                            nowImage_args['pathPAN'] = tif_name
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
                if fullpath.split('.')[0] != nowImage_args['pathPAN'].split('.')[0] and f.endswith( nowImage_args['filter'] ):
                    # ����n��� .tif �H�K��ۦ� pansharpen
                    tif_name = fullpath.split('.')[0] + '.tif'
                    if os.path.exists(tif_name) :
                        nowImage_args['pathMUL'] = tif_name
                        bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('�䤣����� MUL �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]


    # WorldView �t�C��k
    if nowImage_args['rasterType_name'] in ['WorldView-2','WorldView-3','WorldView-4','GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01' :

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

    # �ª� GeoEye-1
    if nowImage_args['rasterType_id'] == 'oldGE01' :
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
            fprint('�䤣�즹�P�إ��� MUL �ɡA�ɮצ��ʺ|�A���ˬd\n')
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

        # �]�� MD �ӵ��u��̾a metadata �ɦW�A�G�ۦ�w�q�ϦW�覡�@�}
        #return sArr[0]+'_'+sArr[1]+'_'+sArr[3]

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]

    # BlackSky
    if nowImage_args['rasterType_name'] in ['BlackSky'] :
        # ���� MUL �D����
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and not f.endswith('-pan.tif'):     # tif �ɥB�����L pan �r��
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('�䤣�즹�P�إ��� MUL �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''
        # �A�� PAN ��
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and f.find('-pan'):     # tif �ɥB������ pan �r��
                    nowImage_args['pathPAN'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('�䤣�즹�P�إ��� PAN �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        # �^�ǹ��ɦW�i��( �]�ۦ� pansharpen addraster )
        return os.path.basename(nowImage_args['pathMUL']).split('.')[0].split('_')[0]

    # PlanetScope
    if nowImage_args['rasterType_name'] in ['PlanetScope'] :
        # ���� MUL �D����
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') :
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('�䤣�즹�P�إ��� MUL �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]

    # Pleiades
    if nowImage_args['rasterType_name'] in ['Pleiades-1'] :
        # ���� MUL �D����
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and f.find('_MS_'):     # tif �ɥB������ _MS_
                    nowImage_args['pathMUL'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('�䤣�즹�P�إ��� MUL �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''
        # �A�� PAN ��
        bo = False
        for root, dirs, files in walk(sour_imagePath):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.tif') and f.find('_P_'):     # tif �ɥB������ _P_ �r��
                    nowImage_args['pathPAN'] = fullpath
                    bo = True
                    break
            if bo :
                break
        if not bo :
            fprint('�䤣�즹�P�إ��� PAN �ɡA�ɮצ��ʺ|�A���ˬd\n')
            return ''

        # �^�ǹ��ɦW�� MUL
        return os.path.basename(nowImage_args['pathMUL']).split('.')[0]


    return ''

# �� xml �Y�`�I
def getXmlTagValue(tree_node, tag_path) :
    for el in tree_node.findall(tag_path) :
        return el.text
    return ''

# �p��xml �`�I�ơA�p�i�q�ƱĥߧY�p��
def countXmlTag(root_node,tag_root_path,tag_head) :
    tree_node = None
    for el in root_node.findall(tag_root_path) :
        tree_node = el
        break
    if tree_node is None:
        return 0
    count = 0
    for child in tree_node :
        if tag_head in child.tag :
            count = count + 1
    return count

#/////////////////////////////////////////
# paraser World-View XML �ݩ�
def parserWorldViewXML( xml_file_name ) :

    # �� ET xml Ū�� xml ��
    tree = ET.parse(xml_file_name)
    root = tree.getroot()

    # Ū�� img_id
    image_MetaData['img_id'] = getXmlTagValue(root,'./IMD/PRODUCTORDERID')
    print( 'img_id:'+image_MetaData['img_id'] )
    image_MetaData['acq_time'] = getXmlTagValue(root,'./IMD/MAP_PROJECTED_PRODUCT/EARLIESTACQTIME').replace('T',' ')
    print( 'acq_time:'+image_MetaData['acq_time'] )
    image_MetaData['sun_evev'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSUNEL')
    print( 'sun_evev:'+image_MetaData['sun_evev'] )
    image_MetaData['sun_azu'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSUNAZ')
    print( 'sun_azu:'+image_MetaData['sun_azu'] )
    image_MetaData['cloud_rate'] = getXmlTagValue(root,'./IMD/IMAGE/CLOUDCOVER')
    print( 'cloud_rate:'+image_MetaData['cloud_rate'] )

    # �i�q�ƱĥߧY�p��
    image_MetaData['band'] = str(countXmlTag(root,'./IMD','BAND_'))
    print( 'band:'+image_MetaData['band'] )

    # �d��
    image_MetaData['ul_x'] = getXmlTagValue(root,'./IMD/BAND_B/ULLON')
    print( 'ul_x:'+image_MetaData['ul_x'] )
    image_MetaData['ul_y'] = getXmlTagValue(root,'./IMD/BAND_B/ULLAT')
    print( 'ul_y:'+image_MetaData['ul_y'] )
    image_MetaData['ur_x'] = getXmlTagValue(root,'./IMD/BAND_B/URLON')
    print( 'ur_x:'+image_MetaData['ur_x'] )
    image_MetaData['ur_y'] = getXmlTagValue(root,'./IMD/BAND_B/URLAT')
    print( 'ur_y:'+image_MetaData['ur_y'] )
    image_MetaData['ll_x'] = getXmlTagValue(root,'./IMD/BAND_B/LLLON')
    print( 'll_x:'+image_MetaData['ll_x'] )
    image_MetaData['ll_y'] = getXmlTagValue(root,'./IMD/BAND_B/LLLAT')
    print( 'll_y:'+image_MetaData['ll_y'] )
    image_MetaData['lr_x'] = getXmlTagValue(root,'./IMD/BAND_B/LRLON')
    print( 'lr_x:'+image_MetaData['lr_x'] )
    image_MetaData['lr_y'] = getXmlTagValue(root,'./IMD/BAND_B/LRLAT')
    print( 'lr_y:'+image_MetaData['lr_y'] )
    image_MetaData['sat_type'] = getXmlTagValue(root,'./IMD/IMAGE/SATID')

    print( 'sat_type:'+image_MetaData['sat_type'] )
    image_MetaData['row_gsd'] = getXmlTagValue(root,'./IMD/IMAGE/MEANCOLLECTEDROWGSD')
    print( 'row_gsd:'+image_MetaData['row_gsd'] )
    image_MetaData['col_gsd'] = getXmlTagValue(root,'./IMD/IMAGE/MEANCOLLECTEDCOLGSD')
    print( 'col_gsd:'+image_MetaData['col_gsd'] )
    image_MetaData['sat_az'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSATAZ')
    print( 'sat_az:'+image_MetaData['sat_az'] )
    image_MetaData['sat_el'] = getXmlTagValue(root,'./IMD/IMAGE/MEANSATEL')
    print( 'sat_el:'+image_MetaData['sat_el'] )
    image_MetaData['gen_time'] = getXmlTagValue(root,'./IMD/GENERATIONTIME').replace('T',' ')
    print( 'gen_time:'+image_MetaData['gen_time'] )
    image_MetaData['band_id'] = getXmlTagValue(root,'./IMD/BANDID')
    print( 'band_id:'+image_MetaData['band_id'] )
    image_MetaData['catalog_id'] = getXmlTagValue(root,'./IMD/PRODUCTCATALOGID')
    print( 'catalog_id:'+image_MetaData['catalog_id'] )
    image_MetaData['shoot_type'] = getXmlTagValue(root,'./IMD/IMAGE/MODE')
    print( 'shoot_type:'+image_MetaData['shoot_type'] )

    return 

#/////////////////////////////////////////
# paraser BlackSky JSON ���ݩ�
def parserBlackSkyJSON( json_file_name ) :
    # �}�� json �ɡA�ѪR��s image_MetaData
    input_file = open( json_file_name )
    json_data = json.load(input_file)

    # �Ȩ��@�����
    image_MetaData['img_id'] = json_data['id']
    print( 'img_id:'+image_MetaData['img_id'] )
    image_MetaData['acq_time'] = json_data['acquisitionDate'].replace('T',' ')
    print( 'acq_time:'+image_MetaData['acq_time'] )
    image_MetaData['sun_evev'] = str(json_data['sunElevation'])
    print( 'sun_evev:'+image_MetaData['sun_evev'] )
    image_MetaData['sun_azu'] = str(json_data['sunAzimuth'])
    print( 'sun_azu:'+image_MetaData['sun_azu'] )
    image_MetaData['cloud_rate'] = str(json_data['cloudCoverPercent'])
    print( 'cloud_rate:'+image_MetaData['cloud_rate'] )
    
    # �d�����έp��
    xmin = 0.0
    ymin = 0.0
    xmax = 0.0
    ymax = 0.0
    coord_arr = json_data['geometry']['coordinates'][0]
    for coord in coord_arr:
        if xmin==0 or coord[0]<xmin :
           xmin = coord[0]
        if ymin==0 or coord[1]<ymin :
           ymin = coord[1]
        if xmax==0 or coord[0]>xmax :
           xmax = coord[0]
        if ymax==0 or coord[1]>ymax :
           ymax = coord[1]
    image_MetaData['cen_x'] = str((xmin+xmax)/2)
    print( 'cen_x:'+image_MetaData['cen_x'] )
    image_MetaData['cen_y'] = str((ymin+ymax)/2)
    print( 'cen_y:'+image_MetaData['cen_y'] )
    image_MetaData['ul_x'] = str(xmin)
    print( 'ul_x:'+image_MetaData['ul_x'] )
    image_MetaData['ul_y'] = str(ymax)
    print( 'ul_y:'+image_MetaData['ul_y'] )
    image_MetaData['ur_x'] = str(xmax)
    print( 'ur_x:'+image_MetaData['ur_x'] )
    image_MetaData['ur_y'] = str(ymax)
    print( 'ur_y:'+image_MetaData['ur_y'] )
    image_MetaData['ll_x'] = str(xmin)
    print( 'll_x:'+image_MetaData['ll_x'] )
    image_MetaData['ll_y'] = str(ymin)
    print( 'll_y:'+image_MetaData['ll_y'] )
    image_MetaData['lr_x'] = str(xmax)
    print( 'lr_x:'+image_MetaData['lr_x'] )
    image_MetaData['lr_y'] = str(ymin)
    print( 'lr_y:'+image_MetaData['lr_y'] )

    image_MetaData['sat_type'] = json_data['sensorName']
    print( 'sat_type:'+image_MetaData['sat_type'] )
    image_MetaData['sat_az'] = str(json_data['satelliteAzimuth'])
    print( 'sat_az:'+image_MetaData['sat_az'] )
    image_MetaData['sat_el'] = str(json_data['satelliteElevation'])
    print( 'sat_el:'+image_MetaData['sat_el'] )
    image_MetaData['catalog_id'] = json_data['gemini']['catalogImageId']
    print( 'catalog_id:'+image_MetaData['catalog_id'] )

    return


#/////////////////////////////////////////
# paraser Pleiades-1 �ݩ�
def parserPleiades( dim_file_name ) :

    # �� ET xml Ū�� dim ��(���Pxml�P�榡)
    tree = ET.parse(dim_file_name)
    root = tree.getroot()

    # Ū�� img_id
    image_MetaData['img_id'] = getXmlTagValue(root,'./Dataset_Identification/DATASET_NAME')
    print( 'img_id:'+image_MetaData['img_id'] )
    image_MetaData['acq_time'] = getXmlTagValue(root,'./Geometric_Data/Use_Area/Located_Geometric_Values/TIME').replace('T',' ')
    print( 'TIME_RANGE:'+image_MetaData['acq_time'] )
    image_MetaData['sun_evev'] = getXmlTagValue(root,'./Geometric_Data/Use_Area/Located_Geometric_Values/Solar_Incidences/SUN_ELEVATION')
    print( 'SUN_ELEVATION:'+image_MetaData['sun_evev'] )
    image_MetaData['sun_azu'] = getXmlTagValue(root,'./Geometric_Data/Use_Area/Located_Geometric_Values/Solar_Incidences/SUN_AZIMUTH')
    print( 'SUN_AZIMUTH:'+image_MetaData['sun_azu'] )
    image_MetaData['cloud_rate'] = getXmlTagValue(root,'./Dataset_Content/CLOUD_COVERAGE')
    print( 'CLOUD_COVERAGE:'+image_MetaData['cloud_rate'] )

    # �i�q�ƩT�w��4
    image_MetaData['band'] = "4"
    print( 'band:'+image_MetaData['band'] )

    # �d��A����� ./Dataset_Content/Dataset_Extent �U�U Vertex
    vertexs = root.findall('./Dataset_Content/Dataset_Extent/Vertex')
    image_MetaData['ul_x'] = vertexs[0].find('LON').text
    print( 'ul_x:'+image_MetaData['ul_x'] )
    image_MetaData['ul_y'] = vertexs[0].find('LAT').text
    print( 'ul_y:'+image_MetaData['ul_y'] )
    image_MetaData['ur_x'] = vertexs[1].find('LON').text
    print( 'ur_x:'+image_MetaData['ur_x'] )
    image_MetaData['ur_y'] = vertexs[1].find('LAT').text
    print( 'ur_y:'+image_MetaData['ur_y'] )
    image_MetaData['ll_x'] = vertexs[2].find('LON').text
    print( 'll_x:'+image_MetaData['ll_x'] )
    image_MetaData['ll_y'] = vertexs[2].find('LAT').text
    print( 'll_y:'+image_MetaData['ll_y'] )
    image_MetaData['lr_x'] = vertexs[3].find('LON').text
    print( 'lr_x:'+image_MetaData['lr_x'] )
    image_MetaData['lr_y'] = vertexs[3].find('LAT').text
    print( 'lr_y:'+image_MetaData['lr_y'] )

    # �����I������
    image_MetaData['cen_x'] = getXmlTagValue(root,'./Dataset_Content/Dataset_Extent/Center/LON')
    print( 'cen_x:'+image_MetaData['cen_x'] )
    image_MetaData['cen_y'] = getXmlTagValue(root,'./Dataset_Content/Dataset_Extent/Center/LAT')
    print( 'cen_y:'+image_MetaData['cen_y'] )
    # satId �T�w�� PHR01
    image_MetaData['sat_type'] = 'PHR01'
    print( 'sat_type:'+image_MetaData['sat_type'] )

    image_MetaData['catalog_id'] = getXmlTagValue(root,'./Product_Information/Delivery_Identification/Order_Identification/COMMERCIAL_REFERENCE')
    print( 'catalog_id:'+image_MetaData['catalog_id'] )

    return 


#/////////////////////////////////////////
# MUL.imd metadata �ɵѨ��ݩ�
# (ps:���ѤW getMetaDataName ���o nowImage_args['pathMUL'] metadata �ɸ�ƫ��J image_MetaData ������s MD �ϥ�)
def parserMetaData( check_path ) :  
    # �̦U�P�� parser

    # World-View/�sGeoEye-1 �ثe���ۦP
    if nowImage_args['rasterType_name'] in ['WorldView-1','WorldView-2','WorldView-3','WorldView-4', 'GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01' :
        imd_name = nowImage_args['pathMUL']
        xml_name = os.path.splitext(imd_name)[0] + ".xml"
        # xml �u��
        if os.path.exists(xml_name) :
            parserWorldViewXML(xml_name)
        else:
            # �Ȥ��B�z .imd
            step_errmsg = 'XML �ɤ��s�b�A�L�k�Ѩ� MetaData'
            return False
    
    # BlackSky
    if nowImage_args['rasterType_name'] in ['BlackSky'] :
        # BlackSky �n���s�A�� json �ɦW
        json_name = ''
        for root, dirs, files in walk(check_path):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('_metadata.json'):                 
                    json_name = fullpath
        if os.path.exists(json_name) :
            parserBlackSkyJSON(json_name)
        else:
            step_errmsg = 'xxx_metadata.json �ɤ��s�b�A�L�k�Ѩ� MetaData'
            return False
    
    # PlanetScope �L metadata �ɡA���ݭn parser

    # Pleiades-1
    if nowImage_args['rasterType_name'] in ['Pleiades-1'] :

        # ��X .dim �ɡA�̦��� parser 
        dim_name = ''
        for root, dirs, files in walk(check_path):
            for f in files:
                fullpath = join(root, f)
                if f.endswith('.dim'):                 
                    dim_name = fullpath
        if os.path.exists(dim_name) :
            parserPleiades(dim_name)
        else:
            step_errmsg = '.dim metadata �ɤ��s�b�A�L�k�Ѩ� MetaData'
            return False
    
    return True

#////////////////////////////////////////////////////////////////////////////////////////////
# arcpy raster �B�z����

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
# �N image �s�W�� MD
def addRasterToDataset( ras_type, file_path, file_name, dataset_name, filter ):

    time_f = datetime.now()
    fprint( '���� addRasterToDataset:' )

    # �]�w AddRasterToMosaicDataset
    mdname  = dataset_name
    rastype = ras_type                             
    inpath  = file_path + "/" + file_name
    updatecs = "UPDATE_CELL_SIZES"
    updatebnd = "UPDATE_BOUNDARY"
    updateovr = "#"        
    maxlevel = "#"                         
    maxcs = "0"
    maxdim = "1500"
    spatialref = "#"                       
    inputdatafilter = "*"+filter  
    subfolder = "SUBFOLDERS"               
    duplicate = "OVERWRITE_DUPLICATES"    
    buildpy = "BUILD_PYRAMIDS"             # �ؼv�����r��
    calcstats = "CALCULATE_STATISTICS"     # �زέp���
    buildthumb = "BUILD_THUMBNAILS"        # �����Y��
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


#////////////////////////////////////////////////////
# pansharpen 
def pansharpenImage( targ_rasterTemp, rasterType_name ):

    time_f = datetime.now()
    fprint( '���� PanSharpening:' )

    # �M���Ȧs
    arcpy.Delete_management(targ_rasterTemp)

    #Compute Pan Sharpen Weights  
    #bit_plane = nowImage_args['panBit_1']+' '+nowImage_args['panBit_2']+' '+nowImage_args['panBit_3']+' '+nowImage_args['panBit_4']
    #out_pan_weight = arcpy.ComputePansharpenWeights_management(
    #    nowImage_args['pathMUL'], nowImage_args['pathPAN'], bit_plane)
    #Get results 
    #pansharpen_weights = out_pan_weight.getOutput(0)
    #Split the results string for weights of each band
    #pansplit = pansharpen_weights.split(";")
    #print("Weight,R:"+pansplit[0].split(" ")[1]+",G:"+pansplit[1].split(" ")[1]+",B:"+pansplit[2].split(" ")[1]+",I:"+pansplit[3].split(" ")[1])

    # �}�l�� PanSharpening
    arcpy.management.CreatePansharpenedRasterDataset(
        nowImage_args['pathMUL'], 
        int(nowImage_args['panBit_1']), int(nowImage_args['panBit_2']), int(nowImage_args['panBit_3']), int(nowImage_args['panBit_4']),
        targ_rasterTemp,
        nowImage_args['pathPAN'], 
        #"Gram-Schmidt", 0.38, 0.25, 0.2, 0.16, rasterType_name)
        "Gram-Schmidt", 0.166, 0.167, 0.167, 0.5, rasterType_name)
        #"Gram-Schmidt",pansplit[0].split(" ")[1],pansplit[1].split(" ")[1], pansplit[2].split(" ")[1],pansplit[3].split(" ")[1])

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
        targRaster, '', None, '65535', "NONE", "NONE", 
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

#//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# �y�{�ޱ�����

# ��FlowCtrl�ޱ���θ��|��ƨM�w�U�@�ӫ����ɮסA�M���ഫ
def getNextZip() :

    # �����o zip ���|�U�U zip
    allfiles = os.listdir(sys_args['zips_path']+'.')

    # ���� array �� 
    arr_zip = [ os.path.splitext(fname)[0] for fname in allfiles if fname.lower().endswith('.zip') or fname.lower().endswith('.rar') or fname.lower().endswith('.7z')]
    arr_zip_f = [ fname for fname in allfiles if fname.lower().endswith('.zip') or fname.lower().endswith('.rar') or fname.lower().endswith('.7z')]

    # zip ���|���L�ɮ׫h�h�X
    if len(arr_zip) == 0 :
        return ''

    # �����ɮ׹w�]����
    choice_zip = arr_zip[0]

    # �}�� FlowCtrl�A�v���d FlowCtrl �L�����h�s�W
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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
                    theTime = datetime.now()
                    theTimeS = theTime.strftime('%Y/%m/%d %H:%M:%S')
                    # �Ϥ� mdb/postgresql/sql �M�w�s�W�覡
                    if sys_args['logdb_mode'] == '1':           # mdb
                        _sql = "INSERT INTO FlowCtrl VALUES('"+zip_name+"','',"+sys_args['defaultPriority']
                        _sql += ",'0:����','0:���B�z','','','','',16,'','','','','','"+theTimeS+"','')"
                        sr.execute(_sql)
                    else:
                        # postgresql �L isnull
                        if sys_args['logdb_mode'] == '2':       # postgreSQL
                            _sql = "INSERT INTO flowctrl (objectid,zipfilename,priority,progress,status,imagebits,refertime)"
                            _sql += " VALUES( (select COALESCE(MAX(objectid), 0)+1 from flowctrl)"
                            _sql += ",'"+zip_name+"',"+sys_args['defaultPriority']
                            _sql += ",'0:����','0:���B�z',16,'"+theTimeS+"')"
                            sr.execute(_sql)
                        # �_�h�Y�O ms sql server
                        else:
                            _sql = "INSERT INTO flowctrl (objectid,zipfilename,priority,progress,status,imagebits,refertime)"
                            _sql += " VALUES( (select isnull(MAX(objectid), 0)+1 from flowctrl)"
                            _sql += ",'"+zip_name+"',"+sys_args['defaultPriority']
                            _sql += ",'0:����','0:���B�z',16,'"+theTimeS+"')"
                            sr.execute(_sql)
                    if (sr.rowcount <= 0) :
                        fprint('�L�k�s�W������ FlowCtrl�A���ˬd:'+zip_name)
                    sr.commit()
                else:    
                    # progress 99: �w��L�� status 3:����(��L�h�O�ഫ��)�h�۰ʭ���
                    if rows[0][1].find('99:') != -1 or rows[0][2].find('3:') != -1 :
                        rows[0][1] = '0:����'
                        rows[0][2] = '0:���B�z'
                        _sql = "update FlowCtrl set Progress='"+rows[0][1]+"',Status='"+rows[0][2]+"',ErrMsg=''"
                        _sql += " where ZipFileName='" + zip_name + "'"
                        sr.execute(_sql)
                         
                    # �B�z fgdb �P�P�ؤ��i�P����
                    if sys_args['is_sdeDB'] != 'Y' and sys_args['is_MultiRun'] == 'Y' :
                        # progress �D 0 ��ܦ��P�إ��ഫ���A���i��
                        if (rows[0][1].find('0:') == -1  and rows[0][3] not in rasterTypeList):
                            rasterTypeList.append( rows[0][3] )
            # ����
            lock_dir.release()
    except Timeout:
        fprint('dir lock timeout')
        return ''

    # ���u���Ǩ��o FlowCtrl 0:���� ��ƫ�������u����
    _sql = "select ZipFileName,RasterTypeID from FlowCtrl"
    _sql += " where Progress like '%0:%' order by FromOldSystem,Priority,ZipFileName"
    sr.execute(_sql)
    rows = sr.fetchall()

    # �P�_���O����P�إB�ɮצs�b�Y�i�� 
    choice_zip = ''
    for row in rows:
        if row[0] in arr_zip and not (row[1]!='' and row[1] in rasterTypeList):
            idx = arr_zip.index(row[0])
            choice_zip = arr_zip_f[idx]

            # �Ĥ@�ӲŦX���Y�O�u���n�઺
            break

    sr.close()
    cn.close()

    return choice_zip


# ��P�� zip �ˬd
def haveSameZipProcess(zip_name) :
    ret_bo = False
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "select ZipFileName from FlowCtrl"
    _sql += " where ZipFileName='" + zip_name + "'"
    _sql += " and Progress not like '%0:%'"            # �}�l��g�J1:���u���e�ˬd���A�G���O 0:�A��ܦ��H���m 
    sr.execute(_sql)
    rows = sr.fetchall()
    if (len(rows)>0) :
        ret_bo = True    

    return ret_bo

# fgdb �P�P�إu�঳�@�Ӧ��@�Ӱ���
def haveSameRasterTypeRun(zip_name,raster_type_id) :
    ret_bo = False
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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

# �^�_���A 0:����
def FlowCtrl_Step_0(zip_name, rasterTypeID, rasterTypeName) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='1:���u���}�l��',Status='1:�B�z��',StartTime='"+start_time+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# ���� FlowCtrl Step2 (�P�اP�_)
def FlowCtrl_Step_2(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='2:�P�اPŪ',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
# �����P����
# (ps:���n�[�gRasterID)
def FlowCtrl_Step_2_RasterType(zip_name, raster_type_id, raster_type, bits) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()

    # ����type_id ��X rasterid
    time_s = datetime.now().strftime('%Y%m%d')
    id_head = time_s+raster_type_id
    _sql = "select RasterID from FlowCtrl"
    _sql += " where RasterID like '%"+id_head+"%'"
    _sql += " order by RasterID desc"
    sr.execute(_sql)
    rows = sr.fetchall()
    serialN = "0001"
    if (len(rows)>0) :
        serialN = str(int(rows[0][0][-4:])+1).zfill(4)
    raster_id = id_head+serialN

    # raster_id ����J       
    nowImage_args['raster_id'] = raster_id

    # ��s flowctrl
    _sql = "UPDATE FlowCtrl SET RasterTypeId='"+raster_type_id+"',RasterType='"+raster_type+"',ImageBits="+bits
    _sql += ",RasterID='"+raster_id+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 

    sr.close()
    cn.close()

    return True

# �q�� Status ErrMsg �]�w
def FlowCtrl_StatusMsg(zip_name, status, add_msg) :

    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()

    if add_msg != '' :
        _sql = "select ErrMsg from FlowCtrl"
        _sql += " where ZipFileName='" + zip_name + "'"
        sr.execute(_sql)
        rows = sr.fetchall()
        if len(rows)>0 :
            msg = ''
            if str(rows[0][0]) != 'None' :
                msg += str(rows[0][0]) + ';'
            #if len(msg)+len(add_msg)>255 :
            msg = add_msg
            #else:
            #    msg += add_msg
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
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='3:�ݩʵѨ�',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_3_ImageName(zip_name,image_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='4:��16bit�s��',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_4_Store(zip_name,file_store) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='5:�[�J16bitMosaicDataset',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# ������s FlowCtrl �~��@�֧�s mosaic dataset �� RasterID ��  
def FlowCtrl_Step_5_MosaicDataset(zip_name,mosaic_dataset,image_name,raster_id,image_path) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET MosaicDataset_16='"+mosaic_dataset+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()

    # �A�}�� mosaic dataset ��s���� rasterid(image_name��w�۰ʩ�Jmosaic dataset-name)
    where_clause = "Name like '%"+image_name+"%'"
    rows = arcpy.UpdateCursor(mosaic_dataset,where_clause)
    # ���ɥ��`�Ȥ@���A�h����@�֧�s
    for row in rows:
        row.setValue("RasterID", raster_id)
        # ���ɸ��|�ά۹���|��J
        cmp_path = [image_path, sys_args['store_root_path']]
        root = os.path.commonprefix(cmp_path)
        row.setValue("path",image_path.replace(root,''))

        # ��L���̬P�بM�w

        # Wordld-View �t�C
        if nowImage_args['rasterType_name'] in ['WorldView-1','WorldView-2','WorldView-3','WorldView-4','GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01':
            row.setValue("productorderId",      image_MetaData['img_id']     )
            row.setValue("earliestAcqTime",     image_MetaData['acq_time']   )
            row.setValue("meanSunEl",           image_MetaData['sun_evev']   )
            row.setValue("meanSunAz",           image_MetaData['sun_azu']    )
            row.setValue("cloudcover",          image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']       )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("meanCollectedRowGSD", image_MetaData['row_gsd']    )
            row.setValue("meanCollectedColGSD", image_MetaData['col_gsd']    )
            row.setValue("meanSatAz",           image_MetaData['sat_az']     )
            row.setValue("meanSatel",           image_MetaData['sat_el']     )
            row.setValue("generationTime",      image_MetaData['gen_time']   )
            row.setValue("bandid",              image_MetaData['band_id']    )
            row.setValue("productCatalogId",    image_MetaData['catalog_id'] )
            row.setValue("mode",                image_MetaData['shoot_type'] )

        # BlackSky
        if nowImage_args['rasterType_name'] in ['BlackSky'] :
            row.setValue("id",                  image_MetaData['img_id']     )
            row.setValue("acquisitionDate",     image_MetaData['acq_time']   )
            row.setValue("sunElevation",        image_MetaData['sun_evev']   )
            row.setValue("sunAzimuth",          image_MetaData['sun_azu']    )
            row.setValue("cloudCoverPercent",   image_MetaData['cloud_rate'] )
            row.setValue("CENLon",              image_MetaData['cen_x']      )
            row.setValue("CENLat",              image_MetaData['cen_y']      )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("sensorName",          image_MetaData['sat_type']   )
            row.setValue("satelliteAzimuth",    image_MetaData['sat_az']     )
            row.setValue("satelliteElevation",  image_MetaData['sat_el']     )
            row.setValue("catalogImageId",      image_MetaData['catalog_id'] )

        # Pleiades-1
        if nowImage_args['rasterType_name'] in ['BlackSky'] :
            row.setValue("img_id",              image_MetaData['img_id']     )
            row.setValue("TIME_RANGE",          image_MetaData['acq_time']   )
            row.setValue("SUN_ELEVATION",       image_MetaData['sun_evev']   )
            row.setValue("SUN_AZIMUTH",         image_MetaData['sun_azu']    )
            row.setValue("CLOUD_COVERAGE",      image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']      )
            row.setValue("cen_x",               image_MetaData['cen_x']      )
            row.setValue("cen_y",               image_MetaData['cen_y']      )
            row.setValue("ul_x",                image_MetaData['ul_x']       )
            row.setValue("ul_y",                image_MetaData['ul_y']       )
            row.setValue("ur_x",                image_MetaData['ur_x']       )
            row.setValue("ur_y",                image_MetaData['ur_y']       )
            row.setValue("ll_x",                image_MetaData['ll_x']       )
            row.setValue("ll_y",                image_MetaData['ll_y']       )
            row.setValue("lr_x",                image_MetaData['lr_x']       )
            row.setValue("lr_y",                image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("catalog_id",          image_MetaData['catalog_id'] )

        rows.updateRow(row)
        del row
    # cursor �n�R���_�h�| lock 
    del rows
    return True

# ���U 16 bit thumbnail �ǳƼg�� 8 bit thumbnail
def getThumbnail(mosaic_dataset, raster_id) :
    whereStr = "rasterid= '"+raster_id+"'"
    with arcpy.da.SearchCursor(mosaic_dataset, ['thumbnail'], where_clause=whereStr) as cursor:
        for row in cursor:
           # �g�� C:\Temp �U
           with open("C:\\Temp\\imageToSave.png", "wb") as fh:
               fh.write(base64.decodebytes(row[0]))
           return row[0]
    return None

# �s�J 8 bit thumbnail ��    
def saveThumbNail(thumbnail, mosaic_dataset, raster_id) :
    where_clause = "rasterid='"+raster_id+"'"
    with arcpy.da.UpdateCursor(mosaic_dataset,['thumbnail'],where_clause) as cursor :
        for row in cursor: 
            row[0] = thumbnail
            cursor.updateRow(row)
    del cursor
    return True


# Step 6 Pan-Sharpen
def FlowCtrl_Step_6(zip_name) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='8:��8bit�s��',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
def FlowCtrl_Step_8_Store(zip_name,file_store) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
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
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET Progress='9:�[�J8bitMosaicDataset',Status='1:�B�z��'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True
# ���P 16bit mosaic dataset �ۦP�A�n�[��s raster_id �浥 metadata �ݩ�
def FlowCtrl_Step_9_MosaicDataset(zip_name,mosaic_dataset,image_name,raster_id,image_path) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    _sql = "UPDATE FlowCtrl SET MosaicDataset_8='"+mosaic_dataset+"'"
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()

    # �A�}�� mosaic dataset ��s���� rasterid(image_name��w�۰ʩ�Jmosaic dataset-name)
    where_clause = "Name like '%"+image_name+"%'"
    rows = arcpy.UpdateCursor(mosaic_dataset,where_clause)
    # ���ɥ��`�Ȥ@���A�h����@�֧�s
    for row in rows:
        row.setValue("RasterID", raster_id)
        # ���ɸ��|�ά۹���|��J
        cmp_path = [image_path, sys_args['store_root_path']]
        root = os.path.commonprefix(cmp_path)
        row.setValue("path",image_path.replace(root,''))

        # ��L���̬P�بM�w
        if nowImage_args['rasterType_name'] in ['WorldView-1','WorldView-2','WorldView-3','WorldView-4','GeoEye-1'] and nowImage_args['rasterType_id'] != 'oldGE01' :
            row.setValue("productorderId",      image_MetaData['img_id']     )
            row.setValue("earliestAcqTime",     image_MetaData['acq_time']   )
            row.setValue("meanSunEl",           image_MetaData['sun_evev']   )
            row.setValue("meanSunAz",           image_MetaData['sun_azu']    )
            row.setValue("cloudcover",          image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']       )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("meanCollectedRowGSD", image_MetaData['row_gsd']    )
            row.setValue("meanCollectedColGSD", image_MetaData['col_gsd']    )
            row.setValue("meanSatAz",           image_MetaData['sat_az']     )
            row.setValue("meanSatel",           image_MetaData['sat_el']     )
            row.setValue("generationTime",      image_MetaData['gen_time']   )
            row.setValue("bandid",              image_MetaData['band_id']    )
            row.setValue("productCatalogId",    image_MetaData['catalog_id'] )
            row.setValue("mode",                image_MetaData['shoot_type'] )

        # BlackSky
        if nowImage_args['rasterType_name'] in ['BlackSky'] :
            row.setValue("id",                  image_MetaData['img_id']     )
            row.setValue("acquisitionDate",     image_MetaData['acq_time']   )
            row.setValue("sunElevation",        image_MetaData['sun_evev']   )
            row.setValue("sunAzimuth",          image_MetaData['sun_azu']    )
            row.setValue("cloudCoverPercent",   image_MetaData['cloud_rate'] )
            row.setValue("CENLon",              image_MetaData['cen_x']      )
            row.setValue("CENLat",              image_MetaData['cen_y']      )
            row.setValue("ULLON",               image_MetaData['ul_x']       )
            row.setValue("ULLat",               image_MetaData['ul_y']       )
            row.setValue("URLon",               image_MetaData['ur_x']       )
            row.setValue("URLat",               image_MetaData['ur_y']       )
            row.setValue("LLLon",               image_MetaData['ll_x']       )
            row.setValue("LLLat",               image_MetaData['ll_y']       )
            row.setValue("LRLon",               image_MetaData['lr_x']       )
            row.setValue("LRLat",               image_MetaData['lr_y']       )
            row.setValue("sensorName",          image_MetaData['sat_type']   )
            row.setValue("satelliteAzimuth",    image_MetaData['sat_az']     )
            row.setValue("satelliteElevation",  image_MetaData['sat_el']     )
            row.setValue("catalogImageId",      image_MetaData['catalog_id'] )

        # Pleiades-1
        if nowImage_args['rasterType_name'] in ['BlackSky'] :
            row.setValue("img_id",              image_MetaData['img_id']     )
            row.setValue("TIME_RANGE",          image_MetaData['acq_time']   )
            row.setValue("SUN_ELEVATION",       image_MetaData['sun_evev']   )
            row.setValue("SUN_AZIMUTH",         image_MetaData['sun_azu']    )
            row.setValue("CLOUD_COVERAGE",      image_MetaData['cloud_rate'] )
            row.setValue("band",                image_MetaData['band']      )
            row.setValue("cen_x",               image_MetaData['cen_x']      )
            row.setValue("cen_y",               image_MetaData['cen_y']      )
            row.setValue("ul_x",                image_MetaData['ul_x']       )
            row.setValue("ul_y",                image_MetaData['ul_y']       )
            row.setValue("ur_x",                image_MetaData['ur_x']       )
            row.setValue("ur_y",                image_MetaData['ur_y']       )
            row.setValue("ll_x",                image_MetaData['ll_x']       )
            row.setValue("ll_y",                image_MetaData['ll_y']       )
            row.setValue("lr_x",                image_MetaData['lr_x']       )
            row.setValue("lr_y",                image_MetaData['lr_y']       )
            row.setValue("satId",               image_MetaData['sat_type']   )
            row.setValue("catalog_id",          image_MetaData['catalog_id'] )

        rows.updateRow(row)
        del row
    # cursor �n�R���_�h�| lock 
    del rows

    return True

# Step 99 ����
def FlowCtrl_Step_99(zip_name, end_time, raster_type_id) :
    cn = pyodbc.connect(sys_args['flowConnectStr'],autocommit=True)
    sr = cn.cursor()
    # �����n�ץ� Priority �H�Ϻ�����N���B�z���ƫe��
    _sql = "UPDATE FlowCtrl SET Progress='99:����',Status='2:���\',EndTime='"+end_time+"',Priority=" + sys_args['defaultPriority']
    _sql += " WHERE ZipFileName='"+zip_name+"'"
    sr.execute(_sql)
    sr.commit() 
    sr.close()
    cn.close()
    return True

# �N zip �ɲ��ʨ� zip �O�d��
def move_zip_to_stub(sour_zip, zip_file_name) :

    # ���U�~���A�ǳƻs�@�l���|�N zip �����B
    theDate = datetime.now()
    theDateS = theDate.strftime('%Y-%m-%d')
    move_path = sys_args['zips_stub'] + theDateS
    if not os.path.exists(move_path) :
        os.mkdir(move_path)
    # �� zip
    fprint( '�N zip �ɲ��ʨ�:'+move_path+'/'+zip_file_name+'\n' )
    shutil.move( sour_zip, move_path+'/'+zip_file_name )

    return

# �N zip �ɲ��ʨ� broken�A�ä@�ּg�X .err �T��
def move_zip_to_broken(sour_zip, zip_file_name, errmsg) :
    # zip �ɤw�s�b���R��
    move_path = sys_args['zip_broken_path']
    if os.path.exists(move_path+'/'+zip_file_name):
        os.remove(move_path+'/'+zip_file_name)
    # �� zip
    fprint( '�פJ���~�A�N zip �ɲ��ʨ�:'+move_path+'/'+zip_file_name+'\n' )
    shutil.move( sour_zip, move_path+'/'+zip_file_name )

    # �g�X���~�T���� .err ��
    errfile = move_path+'/'+zip_file_name.split('.')[0]+'.err'
    with open(errfile, 'w') as f:
        f.write(errmsg)
        f.close()

    return

# �h���h�l���|���o���T tempzip_dir
def checkZipRoot(zip_dir,sub_dir) :
    ret_dir = zip_dir

    # ���P�_�O�_��U�٦� sub_dir�A���h���V sub_dir
    if os.path.isdir(ret_dir+'/'+sub_dir):
        ret_dir += '/'+sub_dir
    else:
        # �P�_ zip_dir �U�Ȧ��@���ɮסA�B�����|�ɧ�H��
        arr = os.listdir(ret_dir+'/.')
        count = 0       
        new_sub_dir = ''    
        for f in arr:
            count = count + 1
            ff = ret_dir+'/'+f
            if os.path.isdir(ff):
                new_sub_dir = f
        if count==1 and new_sub_dir != '' :
            # ���|�W�אּ zip(sub_dir) �W��(zip �W�|�������R�W)
            os.rename(ret_dir+'/'+new_sub_dir, ret_dir+'/'+sub_dir)
            ret_dir += '/'+sub_dir

    return ret_dir

#////////////////////////////////////////////////////////////////////////////////////////////
# �D�y�{
def main():

    # Ū���U�P�ذѼƫ��
    load_config()

    # Ū�J zip ��J�᪺ history �ɡA�ѧP�_�O�_��J�L�̾�
    #read_history()       # ��� FlowCtrl�ެy�{�A�� History.csv �������d��ѦҡA�]�\����

    process_zip_count = 0
    #while process_zip_count < sys_args['limit_doFiles'] :
    while True :

       step_errmsg = ''
       # �ѥؿ��� FlowCtrl�u���Ǩ��o�U�@�ӫ��� zip
       zip_file_name = getNextZip()
       if zip_file_name == '':
           break

       # �}�l�ഫ�� zip ��
       try:

           fprint('\n���ɶ}�l�ഫ:'+zip_file_name+'\n')
           theTime_f = datetime.now()

           zip_name = os.path.splitext(zip_file_name)[0]

           step_errmsg = '���u���ǿ��~'
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

           step_errmsg = '�����Y���~'
           # �����Y��zip�� Temp ���|
           tempzip_dir_root = tempZip_path + zip_name
           if os.path.isdir(tempzip_dir_root):
               shutil.rmtree(tempzip_dir_root)
           os.mkdir(tempzip_dir_root)

           # ����
           if zip_file_name.endswith(".zip"):      # .zip �μзǤ覡
               # �}�� zip ��
               zfile = zipfile.ZipFile(sys_args['zips_path'] + zip_file_name,'r')
               zfile.extractall(tempzip_dir_root)   
               zfile.close()
           else:                              # ��L�� patool
               #���patool
               patoolib.extract_archive(sys_args['zips_path'] + zip_file_name, outdir=tempzip_dir_root)

           # �h���h�l���|���o���T tempzip_dir
           tempzip_dir = checkZipRoot(tempzip_dir_root,zip_name)

           step_errmsg = '�L�k�PŪ�P��'
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
                           # �g�J�P�_���P��(�g�J�P�ؤ]�n�[�gRasterID)
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
              FlowCtrl_StatusMsg(zip_name, '3:����',step_errmsg)
              continue

           # ���� Step3 (�ݩʵѨ�)
           step_errmsg = 'MetaData�ɤ��s�b'
           FlowCtrl_Step_3(zip_name)

           # �� tempzip �ˬd�Ψ��o metadata �ɦW
           # (ps: �� MD �ӵ��u��̾a�� )
           zip_ImageName = getMetaDataName( tempzip_dir )                       # �^�� metadata �ɦW
           if zip_ImageName == '' :
               fprint('�����Y�ɯʤ��ɮ�:\n')
               FlowCtrl_StatusMsg(zip_name, '3:����', step_errmsg)
               continue
           fprint('���o���ɦW:'+zip_ImageName+'\n')
           # ���� metadata �ɦW
           FlowCtrl_Step_3_ImageName(zip_name,zip_ImageName)

           # ���ۦP�˥� temp MUL.imd metadata �ɵѨ��ݩ�
           step_errmsg = 'MetaData���ݩʵѨ�����'
           if not parserMetaData(tempzip_dir) :
               fprint(step_errmsg+'�A���ˬd:\n')
               FlowCtrl_StatusMsg(zip_name, '3:����', step_errmsg)
               continue
                 
           FlowCtrl_StatusMsg(zip_name, '2:���\', '')
           
           # ���P�حY�O 16 bit �h�ݥ[�� 8 bit �����B�z
           if nowImage_args['bits'] == '16' :

               step_errmsg = '��16bit�s����~'
               # ��16bit�s��
               FlowCtrl_Step_4(zip_name)

               # ������ɨ�ӬP�� 16 bit �s���
               file_dir = nowImage_args['fileStore_16'] + '/' + zip_name
               fprint('���ɲ��ʨ�U�P��16 bit�s��:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_16'])

               FlowCtrl_Step_4_Store(zip_name,file_dir)
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # ��16bit���| MetaData MUL �� Pan �ɦW���
               zip_ImageName = getMetaDataName( file_dir )
               if zip_ImageName == '' :
                   fprint('�䤣�즹�P�� MetaData �ɡA�L�k�~��@�~�A���ˬd:\n'+file_dir+'\n')
                   continue

               fprint('raster id:'+nowImage_args['raster_id'])
               
               # pan-sharpen ���e����A�H�]���L addraster autopansharpen �ݦۦ� pansharpen ����
               step_errmsg = 'pan-sharping ���~'
               # Pan-Sharpen
               FlowCtrl_Step_6(zip_name)

               # �� pansharpen �P�ثh���� pan-sharpen
               tempPansharpen = sys_args['sys_store']+'/R'+datetime.now().strftime("%y%m%d_%H%M%S")
               if (nowImage_args['pansharpen'] == 'Y'):
                   # �̬O�_�ۦ� pansharpen �B�z(�B�z BlackSky ���L���P��)
                   if nowImage_args['self_pansharpen'] == 'Y' :
                       # �אּ file ���H�K addraster
                       os.mkdir(file_dir + '\\AutoPan')
                       tempPansharpen = file_dir + '\\AutoPan\\' + zip_ImageName + '.tif'
                       out_type = 'UNKNOWN'
                       #if nowImage_args['rasterType_name'] == 'WorldView-1' :
                       #    out_type = 'WorldView-3'
                       pansharpenImage(tempPansharpen, out_type)
                   else:
                       pansharpenImage(tempPansharpen, nowImage_args['rasterType_name'])

               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               step_errmsg = 'add raster 16bit MD ���~'
               # ��J 16 bit MosaicDataset
               FlowCtrl_Step_5(zip_name)

               # �হ�P�ب� 16bit mosaic dataset�A���L�צ��L 8 bit ���ݭ�����J
               fprint( '��J16bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_16']+'\n' )
               # �̬O�_�ۦ� pansharpen �B�z(�B�z BlackSky ���L���P��)
               if nowImage_args['self_pansharpen'] == 'Y' :
                   if nowImage_args['pansharpen'] == 'Y':
                       addRasterToDataset( 'Raster Dataset', file_dir+'\\AutoPan', '',
                                           nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                           '.tif' )
                   else:
                       addRasterToDataset( 'Raster Dataset', file_dir, '',
                                           nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                           nowImage_args['filter'] )
               else:
                   addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                       nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                       nowImage_args['filter'] )

               # mosaic dataset �n�[�g�ݩ�(�� RasterID �~ parser metadata �ɦU�ݩʩ�J)
               step_errmsg = '�g�J 16bit MD metadata ���~'
               FlowCtrl_Step_5_MosaicDataset(zip_name,nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'],
                                             zip_ImageName, nowImage_args['raster_id'], file_dir)
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # ���U 16 bit thumbnail �ǳƼg�� 8 bit thumbnail
               # ���L�ļȯd(ps:���G add raster �õL���� thumbnail�Aportal�ݪ��O portal �ۤv����)
               #thumbnail_16 = getThumbnail(nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_16'], nowImage_args['raster_id'])

               step_errmsg = 'Copy 16bit �� 8bit ���~'
               # 16 �� 8(���L step 7)
               FlowCtrl_Step_8(zip_name)

               # ��16bit��8bit
               dir_8bit = nowImage_args['fileStore_8'] + '/' + zip_name
               if os.path.isdir(dir_8bit):
                  shutil.rmtree(dir_8bit)
               os.mkdir(dir_8bit)
               file_8bit = dir_8bit + '/' + zip_ImageName + '.TIF'
               if (nowImage_args['pansharpen'] == 'Y'):
                   copyRaster(tempPansharpen, file_8bit, "8_BIT_UNSIGNED", "ScalePixelValue", "TIFF")
                   # �Y�O�۲դ���R temp
                   if nowImage_args['self_pansharpen'] != 'Y' :
                       arcpy.Delete_management(tempPansharpen)
               else:                        # Ū������16bit Raster ��s�ܦp�Wpansharpen���G
                   copyRaster(nowImage_args['pathMUL'], file_8bit, "8_BIT_UNSIGNED", "ScalePixelValue", "TIFF")

               FlowCtrl_Step_8_Store(zip_name,dir_8bit)
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               step_errmsg = 'add raster��8bit MD���~'
               # �� 8 bit MosaicDataset
               FlowCtrl_Step_9(zip_name)

               # ��J 8 bit mosaic dataset
               fprint( '��J8bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8']+'\n' )
               addRasterToDataset( "Raster Dataset", dir_8bit, '',
                                   nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'], 
                                   ".TIF" )
               # �s�J rasterid �� metadata �ݩ�
               FlowCtrl_Step_9_MosaicDataset(zip_name,nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'],
                                             zip_ImageName, nowImage_args['raster_id'], dir_8bit)
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # ���L�ļȯd(�] thumbnail �O�D�Ŧ��Ljpgpng���)
               #if thumbnail_16 != None :
               #   saveThumbNail(thumbnail_16, nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8'],nowImage_args['raster_id'])

               # �৹�b history.csv �g�J�@��
               #histlines.append(zip_name)

               step_errmsg = 'zip ���O�d�Ͽ��~'
               # ������N zip �ɲ��ʨ�zip�O�d��
               move_zip_to_stub(sys_args['zips_path']+zip_file_name,zip_file_name)

           # �_�h�B�z 8 bit
           else :

               step_errmsg = '���ɲ�8bit�s��Ͽ��~'
               # �� 8bit �s��
               FlowCtrl_Step_8(zip_name)

               # ������ɨ�ӬP�� 8 bit �s���
               file_dir = nowImage_args['fileStore_8'] + '/' + zip_name
               fprint('���ɲ��ʨ�U�P��8 bit�s��:'+file_dir+'\n')
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_8'])

               FlowCtrl_Step_8_Store(zip_name,nowImage_args['fileStore_8'])
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               step_errmsg = 'add raster��8bit MD���~'
               # �� 8 bit MosaicDataset
               FlowCtrl_Step_9(zip_name)

               # �হ�P�ب� 8bit mosaic dataset
               fprint( '��J8bit Mosaic Dataset:'+nowImage_args['gdbName']+'\\'+nowImage_args['datasetName_8']+'\n' )
               addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                   nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_8'],
                                   nowImage_args['filter'] )

               FlowCtrl_Step_9_MosaicDataset(zip_name,nowImage_args['gdbName'] + '\\' + nowImage_args['datasetName_8'],
                                             zip_ImageName, nowImage_args['raster_id'])
               FlowCtrl_StatusMsg(zip_name, '2:���\', '')

               # �৹�b history.csv �g�J�@��
               #histlines.append(zip_name)

               step_errmsg = 'zip���O�d�Ͽ��~'
               # ������N zip �ɲ��ʨ�zip�O�d��
               move_zip_to_stub(sys_args['zips_path']+zip_file_name,zip_file_name)

           theTime_e = datetime.now()
           fprint( '�����ഫ�����ɶ�:'+theTime_e.strftime('%Y/%m/%d %H:%M')+'\n'  )
           fprint( "�হZIP�O�ɴX��:{0}\n".format( str((theTime_e-theTime_f).seconds )) )

           # ����
           FlowCtrl_Step_99(zip_name,theTime_e.strftime('%Y/%m/%d %H:%M:%S'),nowImage_args['rasterType_id'])

           # �w�B�z�ɮ�+1
           process_zip_count = process_zip_count + 1
           
           # �৹ temp ���������|�ɲ���
           if os.path.isdir(tempzip_dir_root):
               shutil.rmtree(tempzip_dir_root)

       except Exception as e:
           fprint('\n�o�Ϳ��~:')
           err_msg = repr(e)
           fprint(err_msg)

           # �N zip �ɲ��ʨ� broken�A�ä@�ּg�X .err �T��
           move_zip_to_broken(sys_args['zips_path']+zip_file_name, zip_file_name, str(e))

           # �������ɡA�A�g���A�A�_�h�t�ӵ{�� dir ��|���Ѧ۰ʭ���
           FlowCtrl_StatusMsg(zip_name, '3:����', step_errmsg)
           break

    # �̫�N history �s�^
    #save_history()         

    return True


#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

# �ˬd�O�_�i����(�̦h10��{��)
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

# �N�p��+1
def addRunTimes() :
    # ����w�A�g�J�A�קK�P�ɼg�J
    lock_times = FileLock("lock_times.txt.lock")
    try:
        with lock_times.acquire(timeout=30):
            # Ū���ɮ�+1�g�J
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

# �N�p��-1
def minusRunTimes() :
    # ����w�A�g�J�A�קK�P�ɼg�J
    lock_times = FileLock("lock_times.txt.lock")
    try:
        with lock_times.acquire(timeout=30):
            # Ū���ɮ�+1�g�J
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

#////////////////////////////////////////////////////////
# ����D�{��
# ���d license �]�\�i�[�ֳt��
#if arcpy.CheckProduct("ArcInfo") != "Available":

# �ˬd�αN�p��+1�A�H�O���̦h10��{���P����
if haveFullRun() :
    print("�w�������")
else:
    # ����e�N�p��+1
    addRunTimes()
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
    # ���槹�N�p��-1
    minusRunTimes()

