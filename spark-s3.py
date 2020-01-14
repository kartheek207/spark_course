from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName("av_sf").getOrCreate()

#s3://lpm-line-plan-domain-data-preprod/av/channel.json

av_df = spark.read.csv("s3a://lpm-line-plan-domain-data-preprod/av/channel.json",header=False)
csv_data = av_df.map(lambda l: l.split("\n"))
row_data = csv_data.map(lambda p: Row(
    avProductId=p["avProductId"], 
    seasonYear_abbrn=p["seasonYearAbbreviation"]
    )
)

av_parsed_df = sqlContext.createDataFrame(row_data)
av_parsed_df.registerTempTable("av_df")

av_df_sql = sqlContext.sql("select avProductId from av_parsed_df ")
av_df_sql.show()