# -*- coding: utf-8 -*-

import arcpy
import math
import os,sys
from datetime import datetime, timedelta
import time
import csv 

if __name__ == '__main__':

    # 取下參數
    sourFile = arcpy.GetParameterAsText(0)
    maskFile = arcpy.GetParameterAsText(1)
    targFile = arcpy.GetParameterAsText(1)

    arcpy.sa.CloudDetection(sourFile, maskFile, "Band_3", "Band_4", "Band_5", 20, 200, 15, 20)

