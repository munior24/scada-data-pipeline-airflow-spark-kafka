from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import avg, count, min, max

csv_schema = StructType([
    StructField("Datetime", TimestampType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("WindSpeed", DoubleType(), True),
    StructField("GeneralDiffuseFlows", DoubleType(), True),
    StructField("DiffuseFlows", DoubleType(), True),
    StructField("PowerConsumption_Zone1", DoubleType(), True),
    StructField("PowerConsumption_Zone2", DoubleType(), True),
    StructField("PowerConsumption_Zone3", DoubleType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config('spark.jars.packages', "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0") \
    .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "testTopic") \
    .option("startingOffsets", "earliest") \
    .load()

df = df1.selectExpr("cast(value AS string) as value")
dff = df.withColumn("value", from_json(df["value"], csv_schema)).select("value.*") 


#Exemple of analysis 

# Calculate average temperature
average_temperature = dff.select(avg("Temperature").alias("AverageTemperature"))

query = average_temperature.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()
query.awaitTermination()
