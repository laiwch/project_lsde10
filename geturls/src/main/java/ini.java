import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.GCP;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import org.gdal.gdalconst.gdalconstConstants;

import java.io.Console;    

public class ini{

	public static void main(String[] args) {
		
		gdal.AllRegister();

		//String filename =  "/home/laiwch/lsde/data/187/014/LC08_L1TP_187014_20170710_20170710_01_RT/LC08_L1TP_187014_20170710_20170710_01_RT_B1.TIF";
		String filename = "LC08_L1TP_187014_20170710_20170710_01_RT_B1.TIF";
		Dataset hDataset = gdal.Open(filename, gdalconstConstants.GA_ReadOnly);
		if (hDataset == null)
		{
			System.err.println("GDALOpen failed - " + gdal.GetLastErrorNo());
			System.err.println(gdal.GetLastErrorMsg());

			System.exit(1);
		}
		Driver hDriver = hDataset.GetDriver();
		System.out.println("hello world!");

		System.out.println("Driver: " + hDriver.getShortName() + "/" + hDriver.getLongName());

		System.out.println("Size is " + hDataset.getRasterXSize() + ", "
				+ hDataset.getRasterYSize());

		hDataset.delete();

		gdal.GDALDestroyDriverManager();
	}

}
