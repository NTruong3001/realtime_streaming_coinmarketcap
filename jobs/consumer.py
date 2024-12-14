from pyspark.sql import SparkSession , DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType,  DoubleType, TimestampType
from pyspark.sql.functions import from_json, col ,when , lag , regexp_replace , coalesce, lit, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql import Row 
from pyspark.sql.window import Window
from pyspark.sql.streaming import DataStreamWriter
from datetime import datetime, timezone
from pyspark.sql.functions import udf, date_format,when,col, sum,avg
import uuid
import os 
from config import configuration


# spark = SparkSession.builder \
#     .appName("Spark Kafka Example") \
#     .config("spark.jars.packages", 
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," 
#             "org.apache.kafka:kafka-clients:3.3.1") \
#     .getOrCreate()
# Khởi tạo SparkSession với Kafka nguồn
# spark = SparkSession.builder.appName("Coinmarketcap_streaming") \
#         .config("spark.jars.packages",
#                 "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
#                 "org.apache.hadoop:hadoop-aws:3.3.6,"
#                 "com.amazonaws:aws-java-sdk-s3:1.12.539") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
#         .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
#         .config('spark.hadoop.fs.s3a.aws.credentials.provider',
#                 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
#         .getOrCreate()
spark = SparkSession.builder.appName("Coinmarketcap_streaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Kafka broker và topic
kafka_bootstrap_servers = 'localhost:9092'  # Đảm bảo Kafka đang chạy tại địa chỉ này
kafka_topic = 'current_coin_prices_topic'


# Đọc dữ liệu từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

raw_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define the schema
schema = StructType([
    StructField("Rank", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Symbol", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Market Cap", StringType(), True),
    StructField("Volume", StringType(), True),
    StructField("Change (1h)", StringType(), True),
    StructField("Change (24h)", StringType(), True),
    StructField("Change (7d)", StringType(), True),
    StructField("uuid", StringType(), True)
])


# Parse JSON value
parsed_df = raw_df.withColumn("value", from_json(col("value"), schema)) \
                  .select("value.*")  # Trích xuất các trường từ JSON
# Chuẩn hóa dữ liệu 
def uuid_to_datetime(uuid_v1):
    uuid_obj = uuid.UUID(uuid_v1)
    timestamp = (uuid_obj.time - 0x01B21DD213814000) / 1e7
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt

uuid_to_datetime_udf = udf(uuid_to_datetime, TimestampType()) 

def add_collum_date_time(df):
    result = df \
        .withColumn("dates", date_format(uuid_to_datetime_udf("uuid"), "yyyy-MM-dd")) \
        .withColumn("hours", date_format(uuid_to_datetime_udf("uuid"), "HH:mm:ss"))
    return result

def remove_special_characters_udf(input_df, column_names):
    for column_name in column_names:
        input_df = input_df.withColumn(column_name, regexp_replace(col(column_name), "[^a-zA-Z0-9 ]", " "))
    return input_df

query_1= add_collum_date_time(parsed_df)
coin_market_cap = remove_special_characters_udf(query_1,["Price","Market Cap","Volume"])

# Writes data directly to S3
def streamWriter(input: DataFrame, checkpointFolder: str, output: str):
    try:
        query = (input.writeStream
                 .format('json')  
                 .option('checkpointLocation', checkpointFolder) 
                 .option('path', output)  
                 .outputMode('append')  
                 .trigger(processingTime='30 seconds')  
                 .start())
        
        return query
    except Exception as e:
        print(f"Error in streamWriter: {e}")
        raise  # Re-raise the exception to let the caller handle it

stream_coin_data = streamWriter(
    coin_market_cap, 
    's3a://coin-streaming-tr/checkpoint/coin_market_cap_data',  # Checkpoint location
    's3a://coin-streaming-tr/data/coin_market_cap_data'  # Output location
)

stream_coin_data.awaitTermination()

# query = coin_market_cap.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Chạy stream
# query.awaitTermination()