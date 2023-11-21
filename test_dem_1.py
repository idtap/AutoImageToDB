# -*- coding: utf-8 -*-

import os
from osgeo import gdal, osr
import warnings
warnings.filterwarnings('ignore')

gdal.UseExceptions()

# 正射校正
def ortho(file_name, dem_name, res, out_file_name):
    
    dataset = gdal.Open(file_name, gdal.GA_ReadOnly)
    
    #dstSRS = 'EPSG:4326'
    dstSRS = osr.SpatialReference()
    dstSRS.ImportFromEPSG(9001)
    
    tmp_ds = gdal.Warp(out_file_name, dataset, format = 'GTiff', 
                       #xRes = res, yRes = res, dstSRS = dstSRS, 
                       rpc = True, resampleAlg=gdal.GRIORA_Bilinear,
                       transformerOptions=["RPC_DEM="+dem_name])
    dataset = tds = None

if __name__ == '__main__':

    file_name = r"D:\安康\AutoImageToDB\source_ImageFiles\WV03\16bit\202208\Tripoli_View-Ready_8_Band_Bundle_30cm\055675519040\055675519040_01_P001_MUL\16MAR08101213-M2AS-055675519040_01_P001.TIF"
    #dem_path = r"D:\安康\AutoImageToDB\doc\GDAL校正與pan\SRTM\srtm_39_06.tif"
    dem_path = r"D:\安康\AutoImageToDB\doc\GDAL校正與pan\GDEM-10km-colorized.tif"
    out_file = r"D:\安康\AutoImageToDB\doc\GDAL校正與pan\out_5.tif"
    #ortho(file_name, dem_path, 0.8, out_file)
    ortho(file_name, dem_path, 2.5, out_file)
