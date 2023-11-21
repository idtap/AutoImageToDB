from osgeo import gdal
import numpy as np
import os

'''
path
'''
#path = "I:\\Folder\\"
##name_l = "landsat.tif"
#name_in = "lst5_30_129043_20010302Copy.tif"
#name_out = "test1.tif"

'''
deal with
'''

def dealfunc(path, name_in, name_out):
    '''
    Read tif
    '''
    name_l = name_in
    #np.set_printoptions(threshold=np.inf)#Make print a large amount of data without symbols... instead of displaying all
    dataset = gdal.Open(path + name_l)
    print(dataset.GetDescription())#Data description
    print(dataset.RasterCount)#Number of bands
    im_bands = dataset.RasterCount #number of bands
    print(dataset.GetGeoTransform())#Get affine matrix information
    im_geotrans = dataset.GetGeoTransform()#Get affine matrix information
    print(dataset.GetProjection())#Get projection information
    im_proj = dataset.GetProjection()#Get projection information
    cols=dataset.RasterXSize#Image width
    im_width = dataset.RasterXSize #The number of columns of the raster matrix
    rows=(dataset.RasterYSize)#Image length
    im_height = dataset.RasterYSize #The number of rows of the raster matrix
    
    xoffset=cols/2
    yoffset=rows/2
    
    band = dataset.GetRasterBand(1)#Get the first band
    #r = band.ReadAsArray(xoffset,yoffset,10,10)#From the center of the data, take 10 rows and 10 columns of data
    #im_data = dataset.ReadAsArray(0,0,cols,rows)#Get data
    g = band.ReadAsArray(0,0,cols,rows)#Get data
    #im_blueBand = im_data[0,0:cols,0:rows]#Get the blue band
    
    '''
    display
    '''
    import cv2
    import matplotlib.pyplot as plt
    
    ##img2=cv2.merge([g,g,g])
    #plt.imshow(g)
    #plt.xticks([]),plt.yticks([]) # Do not display the axis
    #plt.show()
    
    '''
    Replace nan
    '''
    import pandas as pd
    
    df = pd.DataFrame(g)
    df = df.fillna(0)
    
    a = df.values
    
    '''
    Output the replaced tif
    '''
    # path = path + "bbb.tif"
    path = path + name_out
    
    im_data = a
    
    if 'int8' in im_data.dtype.name:
        datatype = gdal.GDT_Byte
    elif 'int16' in im_data.dtype.name:
        datatype = gdal.GDT_UInt16
    else:
        datatype = gdal.GDT_Float32
    
    if len(im_data.shape) == 3:
        im_bands, im_height, im_width = im_data.shape
    elif len(im_data.shape) == 2:
        im_data = np.array([im_data])
    else:
        im_bands, (im_height, im_width) = 1, im_data.shape
    #Create a file
    driver = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(path, im_width, im_height, im_bands, datatype)
    if(dataset!= None):
        dataset.SetGeoTransform(im_geotrans) #Write affine transformation parameters
        dataset.SetProjection(im_proj) #Write projection
    for i in range(im_bands):
        dataset.GetRasterBand(i+1).WriteArray(im_data[i])
    del dataset
    print("\n Finished!!!/n")

'''
Main function
'''
path = "D:/安康/AutoImageToDB/doc/"

#单景图
name_in = "4band_WGS84_Taiwan_13/4band_WGS84_Taiwan_13.tif"
name_out = "remove_black_1.tif"
dealfunc(path, name_in, name_out)

##多景图
##Get all file names under the folder
#names = []
#files = os.listdir(path)
#for i in files:
#    if os.path.splitext(i)[1] =='.tif':
#        names.append(i)
#
#for j in names:
#    name_in = j
#    name_out = "z_" + j
#    dealfunc(path, name_in, name_out)
#    print("\n"+ j +"\n")
#    print("\n Finished!!!/n")
