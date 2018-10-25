# project_lsde10





~~~
spark-submit --master yarn --deploy-mode cluster --num-executors 8  --jars lib/geotrellis-util_2.11-0.10.2.jar,lib/geotrellis-spark_2.11-0.10.2.jar,lib/spark-core_2.11-1.5.2.logging.jar,lib/geotrellis-raster_2.11-0.10.2.jar,lib/geotrellis-vector_2.11-0.10.2.jar,lib/geotrellis-macros_2.11-0.10.2.jar,lib/kryo-serializers-0.28.jar,lib/nscala-time_2.11-2.12.0.jar,lib/joda-time-2.9.3.jar,lib/geotrellis-proj4_2.11-0.10.2.jar,lib/monocle-macro_2.11-1.2.1.jar,lib/monocle-core_2.11-1.2.1.jar,lib/jts-core-1.14.0.jar,lib/jackson-core-asl-1.9.13.jar,lib/spray-json_2.11-1.3.1.jar,lib/avro-1.8.0.jar,lib/config-1.2.1.jar,lib/scala-logging-slf4j_2.11-2.1.2.jar,lib/scala-logging_2.11-3.1.0.jar,lib/uzaygezen-core-0.2.jar \
             --class BandCombination target/scala-2.11/bandcombination_2.11-1.0.jar
~~~

