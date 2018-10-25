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


    val dir = "hdfs://hathi-surfsara/user/pboncz/landsat-s3/"
    // val dir = "/home/laiwch/lsde/data/"
    // val outdir = "/home/laiwch/lsde/data/"
    var outdir = "hdfs://hathi-surfsara/user/lsde10/data/"
    def path(b: String) = s"187/014/LC08_L1TP_187014_20170710_20170710_01_RT/LC08_L1TP_187014_20170710_20170710_01_RT_${b}.TIF"





    val rRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(dir+path("B2"))
    var rSourceRdd: RDD[(ProjectedExtent, (Int, Tile))] = 
        rRdd.map{ case (pe,tile) =>
            (pe, (0, tile))
        } 
    val gRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(dir+path("B3"))
    var gSourceRdd: RDD[(ProjectedExtent, (Int, Tile))] =
        gRdd.map{ case (pe,tile) =>
            (pe, (1, tile))
        }
    val bRdd: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(dir+path("B4"))
    var bSourceRdd: RDD[(ProjectedExtent, (Int, Tile))] =
        bRdd.map{ case (pe,tile) =>
            (pe, (2, tile))
        }

    // Union these together, rearrange the elements so that we'll be able to group by key,
    // group them by key, and the rearrange again to produce multiband tiles.
    val sourceTiles: RDD[(ProjectedExtent, MultibandTile)] = {
        sc.union(rSourceRdd, gSourceRdd, bSourceRdd)
            .map{ case(pe, (bandIndex, tile)) =>
                // Get the center of the tile, which we will join on
                val (x, y) = (pe.extent.center.x, pe.extent.center.y)
                // Round the center coordinates in case there's any floating point errors
                val center = 
                (
                    BigDecimal(x).setScale(5, RoundingMode.HALF_UP).doubleValue(),
                    BigDecimal(y).setScale(5, RoundingMode.HALF_UP).doubleValue()
                )

                val newValue = (pe, bandIndex, tile)
                
                (center, newValue)
            }
            .groupByKey()
            .map { case (oldKey, groupedValues) =>
                val projectedExtent = groupedValues.head._1
                val bands = Array.ofDim[Tile](groupedValues.size)

                // val (min, max) = (4000, 15176)

                // def clamp(z: Int) = {
                //   if(isData(z)) { if(z > max) { max } else if(z < min) { min } else { z } }
                //           else { z }
                // }

                for((_, bandIndex, tile) <- groupedValues) {
                // val newtile = tile.map(clamp _).convert(IntCellType).normalize(min, max, 0, 255)
                // bands(bandIndex) = newtile
                bands(bandIndex) = tile
                }

                (projectedExtent, MultibandTile(bands))
            }
    }


    // Tile this RDD to a grid layout. This will transform our raster data into a
    // common grid format, and merge any overlapping data.

    // We'll be tiling to a 512 x 512 tile size, and using the RDD's bounds as the tile bounds.
    val layoutScheme = FloatingLayoutScheme(512)

    // We gather the metadata that we will be targeting with the tiling here.
    // The return also gives us a zoom level, which we ignore.
    val (_: Int, metadata: TileLayerMetadata[SpatialKey]) =
        sourceTiles.collectMetadata[SpatialKey](layoutScheme)

    // Here we set some options for our tiling.
    // For this example, we will set the target partitioner to one
    // that has the same number of partitions as our original RDD.
    // val tilerOptions =
    //   Tiler.Options(
    //     resampleMethod = Bilinear,
    //     partitioner = new HashPartitioner(rRdd.partitions.length)
    //   )

    // Now we tile to an RDD with a SpaceTimeKey.

    // val tiledRdd =
    //   rRdd.tileToLayout[SpatialKey](metadata, tilerOptions)
    val tiles = sourceTiles.tileToLayout[SpatialKey](metadata)



    //  val layerRdd: MultibandTileLayerRDD[SpatialKey] =
    //       ContextRDD(tiles, metadata)

    //     // Convert the values of the layer to  MultibandGeoTiffs
    // val geoTiffRDD: RDD[(SpatialKey,  MultibandGeoTiff)] = layerRdd.map { case (k, v) =>
    //           (k, MultibandGeoTiff(v, layerRdd.metadata.layout.mapTransform(k), layerRdd.metadata.crs))
    //         }

    // // Convert the GeoTiffs to Array[Byte]]
    // val byteRDD: RDD[(SpatialKey, Array[Byte])] = geoTiffRDD.mapValues { _.toByteArray }

    // // In order to save files to S3, we need a function that converts the
    // // Keys of the layer to URIs of their associated values.
    // val keyToURI = (k: SpatialKey) => (outdir + s"${k.col}_${k.row}.tif")

    // byteRDD.saveToHadoop(keyToURI)


    //aaaa//
    val raster = 
        ContextRDD(tiles, metadata)
            .withContext{ rdd =>
                rdd.mapValues { tile =>
                    val (min, max) = (4000, 15176)

                    def clamp(z: Int) = {
                        if(isData(z)) { if(z > max) { max } else if(z < min) { min } else { z } }
                        else { z }
                    }

                    val red = tile.band(0).map(clamp _).convert(ByteCellType).normalize(min, max, 0, 255)
                    val green = tile.band(1).map(clamp _).convert(ByteCellType).normalize(min, max, 0, 255)
                    val blue = tile.band(2).map(clamp _).convert(ByteCellType).normalize(min, max, 0, 255)

                    MultibandTile(red, green, blue)
                }
            }
            .stitch

    val img: Jpg = GeoTiff(raster, metadata.crs).tile.convert(ByteConstantNoDataCellType).renderJpg()
    //write(outdir + "result.jpg")
    val conf = new Configuration();
    val fs = FileSystem.get(URI.create(outdir+"result.jpg"), conf);
    val out = fs.create(new Path(URI.create(outdir+"result.jpg")));
    out.write(img.bytes)



    }

}
