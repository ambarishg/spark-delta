from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

dataPath = "delta-table"
print("dataPath: "+dataPath)
data = spark.range(0, 5)
data.write.format("delta").save(dataPath)