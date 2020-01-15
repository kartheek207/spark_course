from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql import SparkSession
conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName("av_sf").getOrCreate()

#s3://lpm-line-plan-domain-data-preprod/av/channel.json

av_df = spark.read.json("s3a://lpm-line-plan-domain-data-preprod/raw/stg/assort_visual_cluster.json")

av_df.registerTempTable("av_df")

av_df_sql = sqlContext.sql("select key_id, explode(newValue) as new_value  from av_df  ")
av_df_sql.show()
av_df.printSchema()