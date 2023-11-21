# �s�P�� BlackSky ArcGIS �˪O

import os
import arcpy
from functools import lru_cache


try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


class DataSourceType():
    Unknown = 0
    File = 1
    Folder = 2


class RasterTypeFactory():

    def getRasterTypesInfo(self):

        self.acquisitionDate_auxField = arcpy.Field()
        self.acquisitionDate_auxField.name = 'acquisitionDate'
        self.acquisitionDate_auxField.aliasName = 'Acquisition Date'
        self.acquisitionDate_auxField.type = 'Date'
        self.acquisitionDate_auxField.length = 50

        self.sensorName_auxField = arcpy.Field()
        self.sensorName_auxField.name = 'sensorName'
        self.sensorName_auxField.aliasName = 'Sensor Name'
        self.sensorName_auxField.type = 'String'
        self.sensorName_auxField.length = 50

        self.sunAzimuth_auxField = arcpy.Field()
        self.sunAzimuth_auxField.name = 'sunAzimuth'
        self.sunAzimuth_auxField.aliasName = 'Sun Azimuth'
        self.sunAzimuth_auxField.type = 'Double'
        self.sunAzimuth_auxField.precision = 5

        self.sunElevation_auxField = arcpy.Field()
        self.sunElevation_auxField.name = 'sunElevation'
        self.sunElevation_auxField.aliasName = 'Sun Elevation'
        self.sunElevation_auxField.type = 'Double'
        self.sunElevation_auxField.precision = 5

        self.cloudCover_auxField = arcpy.Field()
        self.cloudCover_auxField.name = 'cloudCoverPercent'
        self.cloudCover_auxField.aliasName = 'Cloud Cover'
        self.cloudCover_auxField.type = 'Double'
        self.cloudCover_auxField.precision = 5

        return [
                {
                    'rasterTypeName': 'BlackSky',
                    'builderName': 'BlackSkyBuilder',
                    'description': ("Supports reading of BlackSky "
                                    "products metadata files"),
                    'supportsOrthorectification': True,
                    'enableClipToFootprint': True,
                    'isRasterProduct': True,
                    'dataSourceType': (DataSourceType.File | DataSourceType.Folder),
                    'dataSourceFilter': '*_metadata.json',
                    'processingTemplates': [
                                            {
                                                'name': 'Panchromatic',
                                                'enabled': True,
                                                'outputDatasetTag': 'Pan',
                                                'primaryInputDatasetTag': 'Pan',
                                                'isProductTemplate': True,
                                                'functionTemplate': 'blacksky_stretch_pan.rft.xml'
                                            }
                                           ],
                    'fields': [self.sensorName_auxField,
                               self.productName_auxField,
                               self.acquisitionDate_auxField,
                               self.sunAzimuth_auxField,
                               self.sunElevation_auxField,
                               self.cloudCover_auxField]
                }
               ]


# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# Utility functions used by the Builder and Crawler classes
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class Utilities():

    def isTeleos1(self, path):

        isT1 = False
        tree = cacheElementTree(path)
        if tree is not None:
            element = tree.find('Metadata_Id/METADATA_PROFILE')
            if element is not None:
                if 'TELEOS' in element.text:
                    isT1 = True
        return isT1


    def getProductName(self, path):
        #Get the product type using the string "L1" and "L2" from the txt file name
        directory = os.path.dirname(path)
        for root, dirs, files in (os.walk(directory)):
            for file in (files):
                if file.endswith(".txt"):
                    if 'L1' in file:
                        return 'L1'
                    elif 'L2' in file:
                        return 'L2'

        return None

# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##
# TeLEOS builder class
# ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ## ----- ##


class TeleosBuilder():

    def __init__(self, **kwargs):
        self.SensorName = 'TeLEOS-1'
        self.utilities = Utilities()

    def canOpen(self, datasetPath):
        # Open the datasetPath and check if the metadata file contains the string TELEOS
        return self.utilities.isTeleos1(datasetPath)

    def build(self, itemURI):

        # Make sure that the itemURI dictionary contains items
        if len(itemURI) <= 0:
            return None

        try:

            # ItemURI dictionary passed from crawler containing
            # path, tag, display name, group name, product type
            path = None
            if 'path' in itemURI:
                path = itemURI['path']
            else:
                return None

            # The metadata file is a XML file
            tree = cacheElementTree(path)
            # Horizontal CS (can also be a arcpy.SpatialReference object,
            # EPSG code, path to a PRJ file or a WKT string)
            srsEPSG = 0
            #Here, using the epsg code to build srs
            projectionNode = tree.find('Coordinate_Reference_System/Horizontal_CS/HORIZONTAL_CS_CODE')

            if projectionNode is not None:
                srsEPSG = int((projectionNode.text).split(":")[1]) #to get EPSG code


            # Dataset frame - footprint; this is a list of Vertex coordinates
            vertex_array = arcpy.Array()
            vertex_list = []
            all_vertex = tree.find('Dataset_Frame')
            if all_vertex is not None:
                for vertex in all_vertex:
                    x_vertex = vertex.find('FRAME_LON')
                    y_vertex = vertex.find('FRAME_LAT')
                    if x_vertex is not None and y_vertex is not None:
                        frame_x = float(x_vertex.text)
                        frame_y = float(y_vertex.text)
                        vertex_list.append(arcpy.Point(frame_x, frame_y))
            #the order of vertices must be ul, ur, lr, ll
            vertex_array.add(vertex_list[3])
            vertex_array.add(vertex_list[2])
            vertex_array.add(vertex_list[1])
            vertex_array.add(vertex_list[0])

            # Get geometry object for the footprint; the SRS of the
            # footprint can also be passed if it is different to the
            # SRS read from the metadata; by default, the footprint
            # geometry is assumed to be in the SRS of the metadata
            footprint_geometry = arcpy.Polygon(vertex_array)

            # Metadata Information
            bandProperties = list()

            # Band info(part of metadata) - gain, bias etc
            img_interpretation = tree.find('Image_Interpretation')
            if img_interpretation is not None:
                for band_info in img_interpretation:
                    bandProperty = {}

                    bandProperty['bandName'] = 'Panchromatic'

                    band_num = 0
                    band_index = band_info.find('BAND_INDEX')
                    if band_index is not None:
                        band_num = int(band_index.text)

                    gain = band_info.find('PHYSICAL_GAIN')
                    if gain is not None:
                        bandProperty['RadianceGain'] = float(gain.text)

                    bias = band_info.find('PHYSICAL_BIAS')
                    if bias is not None:
                        bandProperty['RadianceBias'] = float(bias.text)

                    unit = band_info.find('PHYSICAL_UNIT')
                    if unit is not None:
                        bandProperty['unit'] = unit.text

                    bandProperties.append(bandProperty)

            # Other metadata information (Sun elevation, azimuth etc)
            metadata = {}

            acquisitionDate = None
            acquisitionTime = None

            scene_source = 'Dataset_Sources/Source_Information/Scene_Source'
            img_metadata = tree.find(scene_source)
            if img_metadata is not None:
                # Get the Sun Elevation
                sunElevation = img_metadata.find('SUN_ELEVATION')
                if sunElevation is not None:
                    metadata['SunElevation'] = float(sunElevation.text)

                # Get the acquisition date of the scene
                acquisitionDate = img_metadata.find('IMAGING_DATE')
                if acquisitionDate is not None:
                    metadata['AcquisitionDate'] = acquisitionDate.text

                # Get the acquisition time of the scene
                acquisitionTime = img_metadata.find('IMAGING_TIME')
                if acquisitionTime is not None:
                    metadata['AcquisitionDate'] = metadata['AcquisitionDate'] + ' ' + acquisitionTime.text

                # Get the Sun Azimuth
                sunAzimuth = img_metadata.find('SUN_AZIMUTH')
                if sunAzimuth is not None:
                    metadata['SunAzimuth'] = float(sunAzimuth.text)

            metadata['SensorName'] = self.SensorName
            metadata['bandProperties'] = bandProperties
            metadata['ProductType'] = self.utilities.getProductName(path)

            #Set the Product Name
            if metadata['ProductType'] == 'L1':
                metadata['ProductName'] = "Standard"
            elif metadata['ProductType'] == 'L2':
                metadata['ProductName'] = "OrthoReady"

            # Dataset Path - getting the path using the xml filename and appending the required string to the name
            head, tail = os.path.split(path)
            fullPath = os.path.join(head,tail[0:tail.index('_')]+ '_' + metadata['ProductType']+'.tif')

            # define a dictionary of variables
            variables = {}
            variables['DefaultMaximumInput'] = 32767 #15 bits per pixel at product level
            variables['DefaultGamma'] = 1

            # Assemble everything into an outgoing dictionary
            builtItem = {}
            builtItem['spatialReference'] = srsEPSG
            builtItem['raster'] = {'uri': fullPath}
            builtItem['footprint'] = footprint_geometry
            builtItem['keyProperties'] = metadata
            builtItem['variables'] = variables
            builtItem['itemUri'] = itemURI

            builtItemsList = list()
            builtItemsList.append(builtItem)
            return builtItemsList

        except:
            raise

@lru_cache(maxsize=128)
def cacheElementTree(path):
        try:
            tree = ET.parse(path)
        except ET.ParseError as e:
            print("Exception while parsing {0}\n{1}".format(path,e))
            return None

        return tree

#Using the default crawler as there is only Panchromatic band


