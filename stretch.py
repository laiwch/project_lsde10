import numpy as np  
import gdal,gdalconst
import os


def linearstretching(img, x, y):
# 2% linear
   percent=2
   pLow, pHigh = np.percentile(img[img > 0], (percent,100-percent))

   for i in range(x):
   		for j in range(y):
   			img[i,j] = (img[i,j]-pLow)/(pHigh - pLow) *255;

   # img = (img - pLow) / (pHigh - pLow) * 255


def preprocessed_data(filename):

	dataset=gdal.Open(filename, gdal.GA_ReadOnly)
	if dataset is None:
		print ('Could not open' + filename)
		sys.exit(1) 


	cols =  dataset.RasterXSize
	rows =  dataset.RasterYSize
	bands = dataset.RasterCount

	# print (cols, rows, bands)

	band = dataset.GetRasterBand(1)
	data = band.ReadAsArray(0, 0, None, None, cols, rows)

	# uint16 to uint8
	print( data.dtype.name)
	linearstretching(data, rows, cols)
	np.uint8(data)
	print( data.dtype.name)

	return data, cols, rows


# register
gdal.AllRegister()

filename1 = 'LC08_L1TP_187014_20170710_20170710_01_RT_B2.TIF'
filename2 = 'LC08_L1TP_187014_20170710_20170710_01_RT_B3.TIF'
filename3 = 'LC08_L1TP_187014_20170710_20170710_01_RT_B4.TIF'

# data1, cols, rows = preprocessed_data(filename1)
# data2 , cols, rows = preprocessed_data(filename2)
# data3 , cols, rows = preprocessed_data(filename3)

# output driver
outfilename = 'band2.tif'
driver = gdal.GetDriverByName('GTiff');
driver.Register()
data1, cols, rows = preprocessed_data(filename1)
# 3 band output dataset
outDataset = driver.Create(outfilename, cols, rows, 3, gdal.GDT_Byte)
outDataset.GetRasterBand(1).WriteArray(data1);
del data1

data2 , cols, rows = preprocessed_data(filename2)
outDataset.GetRasterBand(2).WriteArray(data2);
del data2

data3 , cols, rows = preprocessed_data(filename3)
outDataset.GetRasterBand(3).WriteArray(data3);
del data3
# outBand1.WriteArray(data1)

dataset = None;
outDataset = None;

# tif to jpg
os.system("gdal_translate -of JPEG  " + outfilename + " band_translate.jpg")

 