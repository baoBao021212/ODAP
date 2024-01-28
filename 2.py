from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import expr
import time
import requests
from pyspark.sql.functions import regexp_replace, translate, when
from pyspark.sql.functions import udf
from pyspark.sql.functions import concat_ws, col


cur = 24575.0
def USDtoVND():
    params = {
    'source' : 'USD',
    'target' : 'VND',
    'length' : 1,
    'resolution' : 'daily',
    'unit' : 'day'
    }
    res = requests.get('https://wise.com/rates/history+live?', params = params)
    cur = res.json()[0]['value']
    return cur

udf_USDtoVND = udf(USDtoVND)


import os
os.environ["HADOOP_USER_NAME"] = "hdfs"
os.environ["PYTHON_VERSION"] = "3.10.0"

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamProcessing") \
    .getOrCreate()

# # Define schema cho dữ liệu từ Kafka
# schema = StructType() \
#     .add("User", StringType()) \
#     .add("Card", StringType()) \
#     .add("Year", IntegerType()) \
#     .add("Month", IntegerType()) \
#     .add("Day", IntegerType()) \
#     .add("Time", StringType()) \
#     .add("Amount", StringType()) \
#     .add("Use Chip", StringType()) \
#     .add("Merchant Name", StringType()) \
#     .add("Merchant City", StringType()) \
#     .add("Merchant State", StringType()) \
#     .add("Zip", StringType()) \
#     .add("MCC", StringType()) \
#     .add("Errors?", StringType()) \
#     .add("Is Fraud?", StringType())

# Đọc dữ liệu từ Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "projectck") \
    .load()

kafka_df.printSchema()

# Chuyển đổi dữ liệu nhận được từ Kafka thành DataFrame
#  parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
#      .select(from_json("value", schema).alias("data")) \
#      .select("data.*")


parsed_df = kafka_df.select(col("value").cast("string")).alias("csv").select("csv.*") \
    .withColumn("User",expr("split(value,',')[0]")) \
    .withColumn("Card", expr("split(value,',')[1]")) \
    .withColumn("Year", expr("split(value,',')[2]")) \
    .withColumn("Month", expr("split(value,',')[3]")) \
    .withColumn("Day", expr("split(value,',')[4]")) \
    .withColumn("Time", expr("split(value,',')[5]")) \
    .withColumn("Amount", expr("split(value,',')[6]")) \
    .withColumn("Use Chip", expr("split(value,',')[7]")) \
    .withColumn("Merchant Name", expr("split(value,',')[8]")) \
    .withColumn("Merchant City", expr("split(value,',')[9]")) \
    .withColumn("Merchant State", expr("split(value,',')[10]")) \
    .withColumn("Zip", expr("split(value,',')[11]")) \
    .withColumn("MCC", expr("split(value,',')[12]")) \
    .withColumn("Errors?", expr("split(value,',')[13]")) \
    .withColumn("Is Fraud?", expr("split(value,',')[14]"))\
    .withColumn('Amount', translate('Amount', '$', '0'))\
    .withColumn("currency", udf_USDtoVND())\
    .withColumn("Date", concat_ws("/", col("Day"), col("Month"), col("Year")))

#.withColumn("AmountVND", when(parsed_df.Amount == '', '').otherwise( parsed_df.currency * parsed_df.Amount))\
# Thực hiện các xử lý hoặc tính toán dữ liệu ở đây
processed_df = parsed_df.withColumn("Amount",parsed_df["Amount"].cast(DoubleType()))
processed_df = processed_df .withColumn("AmountVN", when(processed_df.Amount == '', '').otherwise(processed_df.currency * processed_df.Amount))

processed_df = processed_df.drop("value")\
    .drop("currency")\

# Hiển thị kết quả xử lý dữ liệu
# processed_df \
#     .write.csv("hdfs://localhost:9000/ODAP/example.csv")\
#     .start() \
#     .awaitTermination()

# Chạy StreamingContext
qr = processed_df.writeStream.format("csv")\
.trigger(processingTime="10 second")\
.option("checkpointLocation", "./checkpoints/")\
.option("path", "hdfs://localhost:9000/ODAP/transaction")\
.option("format", "append")\
.option("header","true")\
.start()

qr.awaitTermination()


# # To create data.
# data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
# df = SparkSession.createDataFrame(data)

# # To write files in HDFS.
# df.write.csv("hdfs://KafkaStreamProcessing/user/hdfs/test/example.csv")