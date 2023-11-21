# -*- coding: utf-8 -*-
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
import time


print('測試一些python碼')

print('測試字串含 9999')
if "9999" in "error message:99999，this is must found":
    print("Found")
else:
    print("Not Found")

print('測試檔名尾二碼')
file_path = "D:/安康/AutoImageToDB/source_ImageFiles/WV03/16bit/202208/Tripoli_View-Ready_8_Band_Bundle_30cm/055675519040/055675519040_01_P001_MUL/16MAR08101213-M2AS-055675519040_01_P001-1.IMD"
file_name = os.path.basename(file_path).split('.')[0]
print( '檔名:'+file_name )
print( '倒數第二碼:'+file_name[-2:-1] )
print( '尾碼+1:'+str(int(file_name[-1:])+1))

