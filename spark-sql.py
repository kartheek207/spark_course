from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")


data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file).cache()
csv_data = raw_data.map(lambda l: l.split(","))
row_data = csv_data.map(lambda p: Row(
    duration=int(p[0]), 
    protocol_type=p[1],
    service=p[2],
    flag=p[3],
    src_bytes=int(p[4]),
    dst_bytes=int(p[5])
    )
)

interactions_df = sqlContext.createDataFrame(row_data)
interactions_df.registerTempTable("interactions")

tcp_interactions = sqlContext.sql("""
    SELECT duration, dst_bytes FROM interactions WHERE protocol_type = 'tcp' AND duration > 1000 AND dst_bytes = 0
""")
tcp_interactions.show()
interactions_df.printSchema()

