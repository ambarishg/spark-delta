from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

dataPath = "delta-table"
print("dataPath: "+dataPath)
df = spark.read.format("delta").load(dataPath)
df.show()