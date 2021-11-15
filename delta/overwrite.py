from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

dataPath = "delta-table"
print("dataPath: "+dataPath)
data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save(dataPath)