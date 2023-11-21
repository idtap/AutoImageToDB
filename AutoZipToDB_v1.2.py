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

#//////////////////////////////////////////////////
# ��Ʃw�q

# ���U�ثe���|
sys_path = sys.path[0] 
sys_path += '/'

# zip �ɸ��|
zips_path        = sys_path + 'input_zips/'
sourceImage_path = sys_path + 'source_ImageFiles/'
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
    'filter'           : '.IMD',                         # �w�q�ثe�B�z�v���� metafile ���[�W
    'pansharpen'       : 'Y',                            # �w�q�ثe�B�z�v���O�_�ݰ� pansharpen
    'gdbName'          : sys_path+'rasterStore.gdb',
    'fileStore_16'     : 'D:\�w�d\AutoImageToDB\rasterStore.gdb,D:\�w�d\AutoImageToDB\source_ImageFiles\WV03\16bit',
    'fileStore_8'      : 'D:\�w�d\AutoImageToDB\source_ImageFiles\WV03\8bit',
    'datasetName_16'   : 'WV03_16',
    'datasetName_8'    : 'WV03_8',
    'bits'             : '16',                           # �ثe�B�z���� bits(���n�ѧP�w�D�q config ��)
    'pathPAN'          : '',                             # PanSharpening �ɦW(���ʺA�j�M���J)
    'pathMUL'          : ''
}


#///////////////////////////////////////////////////////////////////////////////////
# �@��

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
            #print('find:'+''.join(line))
            bo = True
            nowImage_args['rasterType_id']    = line[0]
            nowImage_args['rasterType_name']  = line[1]
            nowImage_args['filter']           = line[2]
            nowImage_args['pansharpen']       = line[3]
            nowImage_args['gdbName']          = line[4]
            nowImage_args['fileStore_16']     = line[5]
            nowImage_args['fileStore_8']      = line[6]
            nowImage_args['datasetName_16']   = line[7]
            nowImage_args['datasetName_8']    = line[8]
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
                #print(fullpath)
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
                #print(fullpath)
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
                #print(fullpath)
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
        print('�䤣����� PAN �ɡA�ɮצ��ʺ|�A���ˬd')
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
        print('�䤣����� MUL �ɡA�ɮצ��ʺ|�A���ˬd')
        return False 

    return True

#////////////////////////////////////////////////////
# pansharpen 
def pansharpenImage( targ_rasterTemp ):
    print( '\n���� PanSharpening:' )

    # �M���Ȧs
    arcpy.Delete_management(targ_rasterTemp)

    # �}�l�� PanSharpening
    arcpy.management.CreatePansharpenedRasterDataset(
        nowImage_args['pathMUL'], 
        3, 2, 1, 4, 
        targ_rasterTemp,
        nowImage_args['pathPAN'], 
        "Gram-Schmidt", 0.38, 0.25, 0.2, 0.16, nowImage_args['rasterType_name'])

    #CalculateStatistics(sourRaster,"","","")
    #BuildPyramids(sourRaster)

    return True

#////////////////////////////////////////////////////
# 16bit��8bit
def copyRaster(sourRaster,targRaster, bits, scale, format):
    arcpy.Delete_management(targRaster)
    #arcpy.CopyRaster_management( sourRaster, targRaster,
    #    "#","#","#","NONE","NONE","8 bit unsigned","NONE","NONE")
    arcpy.management.CopyRaster(
        sourRaster, 
        targRaster, '', None, '', "NONE", "NONE", 
        bits, scale, "NONE", format, "NONE", "CURRENT_SLICE", "NO_TRANSPOSE")


#////////////////////////////////////////////////////////////////////////////////////////////
# �D�y�{
def main():

    theTime_f = datetime.now()
    print( '\n�ഫ�}�l�ɶ�:'+theTime_f.strftime('%Y/%m/%d %H:%M')  )

    # Ū���U�P�ذѼƫ��
    load_config()

    # Ū�J zip ��J�᪺ history �ɡA�ѧP�_�O�_��J�L�̾�
    read_history()

    # �j�M���|�U�� zip �ɡA�v�@�P�_����L(History.csv���L����)����
    allfiles = os.listdir(zips_path+'.')
    arr_zip = [ fname for fname in allfiles if fname.lower().endswith('.zip')]            # �� zip ���|�U�U zip �ɨ� list ��
    for zip_name in arr_zip:
       try:

           if zip_name in histlines:
               print( '\n�����w��L:'+zip_name )
           else:
               print( '\n�ثe��J:'+zip_name )

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

               # �P�_�P��
               if checkImageType(tempzip_dir):
                  print('\n�P�_�X���ϬP�ج�:'+nowImage_args['rasterType_name'])
               else:
                  print('\n�� zip ���ɵL�k�PŪ�P��')
                  continue

               # ������ɨ�ӬP�� 16 bit �s���
               file_dir = nowImage_args['fileStore_16'] + '/' + os.path.splitext(zip_name)[0]
               print('\n���ɲ��ʨ�U�P��16 bit�s��:'+file_dir)
               if os.path.isdir(file_dir):
                  shutil.rmtree(file_dir)
               shutil.move(tempzip_dir,nowImage_args['fileStore_16'])

               # �� MetaData �ɮ�
               if not getMetaDataPath( file_dir ) :
                   print('\n�����|�L Meta �ɡA�L�k�~��@�~�A���ˬd:\n'+file_dir)
                   continue

               # �হ�P�ب� 16bit mosaic dataset�A���L�צ��L 8 bit ���ݭ�����J
               print( '\n��J16bit Mosaic Dataset' )
               addRasterToDataset( nowImage_args['rasterType_name'], file_dir, '',
                                   nowImage_args['gdbName'] + '/' + nowImage_args['datasetName_16'],
                                   nowImage_args['filter'] )

               # �� pansharpen �P�ثh���� pan-sharpen
               tempRaster_16 = nowImage_args['gdbName']+'/tempPan_16'
               if (nowImage_args['pansharpen'] == 'Y'):
                   pansharpenImage(tempRaster_16)

               # ��16bit��8bit
               tempRaster_8 = nowImage_args['gdbName']+'/tempPan_8'
               print( '\n16bit��8bit:\n'+tempRaster_8 )
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

               # ��J 8 bit mosaic dataset
               print( '\n��J8bit Mosaic Dataset:\n'+nowImage_args['gdbName']+'/'+nowImage_args['datasetName_8'] )
               addRasterToDataset( "#", dir_8bit, '',
                                   nowImage_args['gdbName']+'/'+nowImage_args['datasetName_8'], ".TIF" )

               # �৹�b zips_path/history.csv �g�J�@��
               zfile.close()
               histlines.append(zip_name)

               # �N zip �ɲ��ʨ�U�P�� 16 bit �s���
               print( '\n�৹�A�N zip �ɲ��ʨ�U�P��:\n'+nowImage_args['fileStore_16']+'/'+zip_name )
               shutil.move( zips_path+zip_name, nowImage_args['fileStore_16']+'/'+zip_name )

       except Exception as e:
           print('\n�o�Ϳ��~:')
           print(e)

    # �̫�N history �s�^
    save_history()

    theTime_e = datetime.now()
    print( '\n�����ɶ�:'+theTime_e.strftime('%Y/%m/%d %H:%M')+'\n'  )
    print( "\n�O�ɴX��:{0}\n".format( str((theTime_e-theTime_f).seconds )) )

    return True


# ����D�{��
print( main() )

