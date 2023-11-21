# -*- coding: utf-8 -*-
# !/usr/bin/env python

import sys, getopt
import os
import math
import psutil

# 檔案大小(GB)
def fileSize_G(file_path):
    file_size = os.path.getsize(file_path)
    file_size_gb = file_size / (1024 * 1024 * 1024)
    return file_size_gb

# 磁碟大小(TB)
def diskSize_T(disk):
    disk_space = psutil.disk_usage(disk)
    total_space_T = disk_space.total / (1024 ** 3) / 1000
    return total_space_T

# 磁碟剩餘大小(TB)
def diskNotUseSize_T(disk):
    disk_space = psutil.disk_usage(disk)
    total_space_T = disk_space.total / (1024 ** 3) / 1000
    used_space_T = disk_space.used / (1024 ** 3) / 1000
    return total_space_T-used_space_T

print( "判斷此檔大小 D:/Tool/WINDOWS.X64_193000_db_home.zip" )
print( "=>"+str(fileSize_G("D:/Tool/WINDOWS.X64_193000_db_home.zip"))+" GB" )

print( "\n判斷 D: 磁碟大小" )
print("=>"+str(diskSize_T("D:"))+" TB")

print( "\nD: 磁碟剩餘空間" )
print("=>"+str(diskNotUseSize_T("D:"))+" TB")

