import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext,SparkConf}
import java.io.{File,BufferedOutputStream,OutputStream,InputStream}

import scala.io.Source
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._

import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._

import scala.io.StdIn
import java.io.File
import java.net.URI
import scala.math.BigDecimal.RoundingMode


object BandCombination {

def main(args: Array[String]): Unit = {
    // Setup Spark to use Kryo serializer.
    val conf =
        new SparkConf()
        // .setMaster("local[*]")
        .setAppName("BandCombination")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .set("spark.kryoserializer.buffer.max.mb", "1024")
        // .set("spark.executor.memory", "4g")
        // .set("park.driver.memory", "4g")
    // org.apache.spark.SparkException: Task not serializable
    conf.registerKryoClasses(Array(classOf[geotrellis.raster.io.geotiff.Striped]))
    conf.registerKryoClasses(Array(classOf[geotrellis.raster.io.geotiff.GeoTiffOptions]))


    val sc = new SparkContext(conf)
    try {
        run(sc)
    } finally {
    sc.stop()
    }
}



def run(implicit sc: SparkContext) = {

    val dir = "hdfs://hathi-surfsara/user/pboncz/landsat-history/1985/"
    // val dir = "/home/laiwch/lsde/data/"//187/014"
    // val outdir = "/home/laiwch/lsde/data/"
    var outdir = "hdfs://hathi-surfsara/user/lsde10/data/history/1985/"
    // def path(b: String) = s"187/014/LC08_L1TP_187014_20170710_20170710_01_RT/LC08_L1TP_187014_20170710_20170710_01_RT_${b}.TIF"

    val defaultTiffExtensions: Seq[String] = Seq("B2.TIF", "B3.TIF", "B5.TIF")//, "BQA.TIF")
    val files = HdfsUtils.listFiles(new Path(dir), sc.hadoopConfiguration)
        .filter { s =>
            if (defaultTiffExtensions.exists(s.toString.endsWith)) {
                true
            }
            else
                false
        }

    // change "hdfsPath" to "String"  because of  [error: java.io.NotSerializableException: org.apache.hadoop.fs.Path]
    val filesString = files.map{s=>s.toString}


  
   
   //Traversal List  because of [error: java.io.NotSerializableException: org.apache.spark.SparkContext]
    import scala.collection.mutable.ListBuffer
    //Key (path: String) , Value (rdd: RDD)
    var RDDsListBuffer = new ListBuffer[ (String,  RDD[( ProjectedExtent, (Int, Tile)) ] )]

    for(s <-  filesString){
        RDDsListBuffer += {
            val path = s.split('/').reverse.drop(1).head
            // println(path)
            val rdd = sc.hadoopGeoTiffRDD(s).map{ case (pe,tile) =>
                val bandname = s.split('_').reverse.head
                var bandIndex = 0
                if(bandname == "B2.TIF") {
                    bandIndex = 0
                }
                else if(bandname == "B3.TIF"){
                     bandIndex = 1
                }
                else if(bandname == "B5.TIF"){
                     bandIndex = 2
                }
                else{
                    bandIndex = 3
                }

                //magic number
                val (min, max) = (4000, 15176)

                def clamp(z: Int) = {
                  if(isData(z)) { if(z > max) { max } else if(z < min) { min } else { z } }
                          else { z }
                }

                val newtile = tile.map(clamp _).convert(IntCellType).normalize(min, max, 0, 255)
                
                (pe, (bandIndex, newtile))
            }

            (path, rdd)
        }
    }
    val RDDsList = RDDsListBuffer.toList
    // println(RDDsList.size)
    val SourceRDDs = RDDsList.groupBy(_._1)
        .mapValues{ case list => 
            val RddTiles :RDD[( ProjectedExtent, (Int, Tile)) ] = sc.union(list.map{ case (path, rdd) => rdd})
            RddTiles
        } 

    val imgs = SourceRDDs
    //println(SourceRDDs.size)
        
        .mapValues{ case rdd => 
            val RddTiles = rdd.groupByKey()
                .map{ case (pe, groupedValues) =>
                    val projectedExtent = pe
                    val bands = Array.ofDim[Tile](groupedValues.size)

                    for((bandIndex, tile) <- groupedValues) {

                    bands(bandIndex) = tile
                    }

                    (projectedExtent, MultibandTile(bands))

                }

            val layoutScheme = FloatingLayoutScheme(512)
            val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
                RddTiles.collectMetadata[SpatialKey](layoutScheme)

            val tiles = RddTiles.tileToLayout[SpatialKey](metadata)
            val raster =  ContextRDD(tiles, metadata).stitch

            // val ramp =
                // ColorRamp.createWithRGBColors(0xFF0000, 0x00FF00, 0x0000FF)
            // val colorMap = ColorMap.fromQuantileBreaks(raster.tile.color.histogram, ColorRamp(0xFF0000, 0x0000FF).stops(256))
            val img: Jpg = raster.tile.color.renderJpg//ColorRamps.HeatmapBlueToYellowToRedSpectrum)

            img.bytes
        }

        for((path, img) <-  imgs){
            val hdfsconf = new Configuration();
    
            val fs = FileSystem.get(URI.create(outdir+ path + ".jpg"), hdfsconf);
            val out = fs.create(new Path(URI.create(outdir+ path + ".jpg")));
            try{
                out.write(img)
            }finally{
                out.close()
            }
        }
        // imgs.map{case (path, img) =>

           
        // }




    // println("***************" + SourceRDDs.partitions.size)
    // val conf = new Configuration();
    //  SourceRDDs.map{ case(path, img) =>
        
    //     val fs = FileSystem.get(URI.create(outdir+ path + ".jpg"), conf);
    //     val out = fs.create(new Path(URI.create(outdir+ path + ".jpg")));
    //     out.write(img.bytes)
    // }


    // val rRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(dir+path("B2"))
    // var rSourceRdd: RDD[(ProjectedExtent, (Int, Tile))] = 
    //     rRdd.map{ case (pe,tile) =>
    //         (pe, (0, tile))
    //     } 
    // val gRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(dir+path("B3"))
    // var gSourceRdd: RDD[(ProjectedExtent, (Int, Tile))] =
    //     gRdd.map{ case (pe,tile) =>
    //         (pe, (1, tile))
    //     }
    // val bRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(dir+path("B5"))
    // var bSourceRdd: RDD[(ProjectedExtent, (Int, Tile))] =
    //     bRdd.map{ case (pe,tile) =>
    //         (pe, (2, tile))
    //     }

    // // Union these together, rearrange the elements so that we'll be able to group by key,
    // // group them by key, and the rearrange again to produce multiband tiles.
    // val sourceTiles: RDD[(ProjectedExtent, MultibandTile)] = {
    //     sc.union(rSourceRdd, gSourceRdd, bSourceRdd)
    //         .map{ case(pe, (bandIndex, tile)) =>
    //             // Get the center of the tile, which we will join on
    //             val (x, y) = (pe.extent.center.x, pe.extent.center.y)
    //             // Round the center coordinates in case there's any floating point errors
    //             val center = 
    //             (
    //                 BigDecimal(x).setScale(5, RoundingMode.HALF_UP).doubleValue(),
    //                 BigDecimal(y).setScale(5, RoundingMode.HALF_UP).doubleValue()
    //             )

    //             val newValue = (pe, bandIndex, tile)
                
    //             (center, newValue)
    //         }
    //         .groupByKey()
    //         .map { case (oldKey, groupedValues) =>
    //             val projectedExtent = groupedValues.head._1
    //             val bands = Array.ofDim[Tile](groupedValues.size)

    //             val (min, max) = (4000, 15176)

    //             def clamp(z: Int) = {
    //               if(isData(z)) { if(z > max) { max } else if(z < min) { min } else { z } }
    //                       else { z }
    //             }

    //             // def clampColor(c: Int): Int =
    //             //     if(isNoData(c)) { c }
    //             //     else {
    //             //         if(c < 0) { 0 }
    //             //         else if(c > 255) { 255 }
    //             //         else c
    //             //     }

    //             //     // -255 to 255
    //             // val brightness = 15
    //             // def brightnessCorrect(v: Int): Int =
    //             //     if(v > 0) { v + brightness }
    //             //     else { v }

    //             // // 0.01 to 7.99
    //             // val gamma = 0.8
    //             // val gammaCorrection = 1 / gamma
    //             // def gammaCorrect(v: Int): Int =
    //             //     (255 * math.pow(v / 255.0, gammaCorrection)).toInt

    //             // // -255 to 255
    //             // val contrast: Double = 30.0
    //             // val contrastFactor = (259 * (contrast + 255)) / (255 * (259 - contrast))
    //             // def contrastCorrect(v: Int): Int =
    //             //     ((contrastFactor * (v - 128)) + 128).toInt

    //             // def adjust(c: Int): Int = {
    //             //     if(isData(c)) {
    //             //         var cc = c
    //             //         cc = clampColor(brightnessCorrect(cc))
    //             //         cc = clampColor(gammaCorrect(cc))
    //             //         cc = clampColor(contrastCorrect(cc))
    //             //         cc
    //             //     } else {
    //             //         c
    //             //     }
    //             // }

    //             for((_, bandIndex, tile) <- groupedValues) {
    //                 val newtile = tile.map(clamp _).convert(IntCellType).normalize(min, max, 0, 255)//.map(adjust _)
    //                 bands(bandIndex) = newtile
    //                 // bands(bandIndex) = tile
    //             }

    //             (projectedExtent, MultibandTile(bands))
    //         }
    // }


    // // Tile this RDD to a grid layout. This will transform our raster data into a
    // // common grid format, and merge any overlapping data.

    // // We'll be tiling to a 512 x 512 tile size, and using the RDD's bounds as the tile bounds.
    // val layoutScheme = FloatingLayoutScheme(512)

    // // We gather the metadata that we will be targeting with the tiling here.
    // // The return also gives us a zoom level, which we ignore.
    // val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
    //     sourceTiles.collectMetadata[SpatialKey](layoutScheme)

    // // Here we set some options for our tiling.
    // // For this example, we will set the target partitioner to one
    // // that has the same number of partitions as our original RDD.
    // // val tilerOptions =
    // //   Tiler.Options(
    // //     resampleMethod = Bilinear,
    // //     partitioner = new HashPartitioner(rRdd.partitions.length)
    // //   )

    // // Now we tile to an RDD with a SpaceTimeKey.

    // // val tiledRdd =
    // //   rRdd.tileToLayout[SpatialKey](metadata, tilerOptions)
    // val tiles = sourceTiles.tileToLayout[SpatialKey](metadata)






    // //aaaa//
    // val raster = 
    //     ContextRDD(tiles, metadata)
    //         // .withContext{ rdd =>
    //         //     rdd.mapValues { tile =>
    //         //         val (min, max) = (4000, 15176)

    //         //         def clamp(z: Int) = {
    //         //             if(isData(z)) { if(z > max) { max } else if(z < min) { min } else { z } }
    //         //             else { z }
    //         //         }

    //         //         val red = tile.band(0).map(clamp _).convert(ByteCellType).normalize(min, max, 0, 255)
    //         //         val green = tile.band(1).map(clamp _).convert(ByteCellType).normalize(min, max, 0, 255)
    //         //         val blue = tile.band(2).map(clamp _).convert(ByteCellType).normalize(min, max, 0, 255)

    //         //         MultibandTile(red, green, blue)
    //         //     }
    //         // }
    //         .stitch


    // // Generate a color ramp with
    // // red (#FF0000), green (#00FF00), and blue (#0000FF)
    // // val ramp =
    // //     ColorRamp.createWithRGBColors(0xFF0000, 0x00FF00, 0x0000FF)
    // val colorMap = ColorMap.fromQuantileBreaks(
    //         raster.tile.color.histogram, 
    //         ColorRamp(0xFF0000, 0x0000FF).stops(256), 
    //         ColorMap.Options(
    //             classBoundaryType = GreaterThanOrEqualTo,
    //             noDataColor = 0xFFFFFFFF,
    //             fallbackColor = 0xFFFFFFFF
    //         )
    //     )
        
  
    // val img: Jpg = raster.tile.color.renderJpg(colorMap)//ColorRamps.HeatmapBlueToYellowToRedSpectrum)//colorMap)//ColorRamps.HeatmapBlueToYellowToRedSpectrum)
    // //write(outdir + "result.jpg")
    // val conf = new Configuration();
    // val fs = FileSystem.get(URI.create(outdir+"result.jpg"), conf);
    // val out = fs.create(new Path(URI.create(outdir+"result.jpg")));
    // out.write(img.bytes)



    }

}
