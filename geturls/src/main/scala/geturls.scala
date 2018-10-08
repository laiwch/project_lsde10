import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
//import my.lai.java._;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext,SparkConf}
import java.io.{File,BufferedOutputStream,OutputStream,InputStream}
import java.net.{URL,URLConnection,HttpURLConnection}
import scala.io.Source

// object geturls{
//   def copy(in: InputStream, out: OutputStream) : Long = {
//     val buf = new Array[Byte](1 << 20) // 1 MB
//     var cnt = 0 
//     while (true) {
//       val read = in.read(buf)
//       if (read == -1) {
//         out.flush
//         return cnt;
//       }
//       cnt += read
//       out.write(buf, 0, read)
//     }
//     return cnt;
//   }

//   def downloadFile(u: String, p: String, f: String) :String = {
//     try {
//       val in = new URL(u).openConnection.getInputStream
//       val fs = FileSystem.get(new Configuration)
//       fs.mkdirs(new Path(p)) // recursively make all directories
//       val out = new BufferedOutputStream(fs.create(new Path(p + f)))
//       val cnt = copy(in, out) 
//       in.close
//       out.close
//       return "copied " + u + " into " + p + f + " (" + cnt + " bytes)";
//     } catch {
//       case e:Exception => return e + " opening url " + u + " for directory " + p +  " and file " + f;
//     }
//   }
  
//   def main(args: Array[String]) {  
//     val conf = new SparkConf().setAppName("geturls")
//     val sc = new SparkContext(conf)   
//     val input = sc.textFile("landsat_small.txt").collect 
  
//     def fileDownloader(url: String, dirname: String, filename: String) = {
//       val pathname = "hdfs://hathi-surfsara/user/pboncz/landsat/"+dirname
//       downloadFile(url+"/"+dirname+"/"+filename, pathname+"/", filename)
//     }
//     val par = sc.parallelize(input) // explicit parallelize
  
//     // collect any errors and debug output on master stdout (good old mapreduce pattern) 
//     val res = par.map(x => fileDownloader(x.split(",")(0), x.split(",")(1), x.split(",")(2)))
//     println(res.reduce((s1, s2) => s1 + "\n" + s2))
//   }
// }


object geturls {
  def main(args: Array[String]){
  	//val p = new Employee("Bill Ryan");
  	//gdal.AllRegister();
  	//Dataset hDataset;
    println("Hi!" )
  }
}