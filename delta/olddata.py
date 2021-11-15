from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

dataPath = "delta-table"
df = spark.read.format("delta").option("versionAsOf", 0).load(dataPath)
df.show()