# -*- coding: utf-8 -*-
# !/usr/bin/env python

# 環境啟動
import sys, getopt
import os
import math

def cal_az_angle(along,cross) :
    if math.tan(along)>=0 and math.tan(cross)<0 :
        return math.degrees(math.radians(450)-math.atan2(math.tan(along),math.tan(cross))) % 360
    else:
        return math.degrees(math.radians(90)-math.atan2(math.tan(along),math.tan(cross))) % 360

def cal_az_angle2(along,cross) :
    if math.sin(along)>=0 and (math.cos(along)*math.sin(cross))<0 :
        return math.degrees(math.radians(450)-math.atan2(math.sin(along),math.cos(along)*math.sin(cross))) % 360
    else:
        return math.degrees(math.radians(90)-math.atan2(math.sin(along),math.cos(along)*math.sin(cross))) % 360

print('測試 az 計算 1:')
B_y = -11
B_x = 15
print('B(y):'+str(B_y) )
print('B(x):'+str(B_x) )
azi = 180
print('計算結果(已倫):'+str(cal_az_angle2(math.radians(B_y),math.radians(B_x))))
print('計算結果(cathy):'+str(cal_az_angle(math.radians(B_y),math.radians(B_x))))
print('-------------------------')
print('測試 az 計算 2:')
B_y = (-14.9367)
B_x = 29.8
print('B(y):'+str(B_y) )
print('B(x):'+str(B_x) )
azi = 180
print('計算結果(已倫):'+str(cal_az_angle2(math.radians(B_y),math.radians(B_x))))
print('計算結果(cathy):'+str(cal_az_angle(math.radians(B_y),math.radians(B_x))))
print('-------------------------')
print('測試 az 計算 3:')
B_y = (20.673)
B_x = 8.708
print('B(y):'+str(B_y) )
print('B(x):'+str(B_x) )
azi = 180
print('計算結果(已倫):'+str(cal_az_angle2(math.radians(B_y),math.radians(B_x))))
print('計算結果(cathy):'+str(cal_az_angle(math.radians(B_y),math.radians(B_x))))
print('-------------------------')
print('測試 az 計算 4:')
B_y = (-25.27035)
B_x = 6.64248
print('B(y):'+str(B_y) )
print('B(x):'+str(B_x) )
azi = 180
print('計算結果(已倫):'+str(cal_az_angle2(math.radians(B_y),math.radians(B_x))))
print('計算結果(cathy):'+str(cal_az_angle(math.radians(B_y),math.radians(B_x))))

