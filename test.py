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
# Khởi tạo SparkSession với Kafka nguồn
spark = SparkSession.builder \
    .appName("Spark Kafka Example") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," 
            "org.apache.kafka:kafka-clients:3.3.1") \
    .getOrCreate()

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
        .withColumn("hours", date_format(uuid_to_datetime_udf("uuid"), "HH:mm:ss"))\
        .withColumn("StartDate", date_format(uuid_to_datetime_udf("uuid"), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("EndDate", lit(" "))\
        .withColumn("is_current", lit("Y"))
    return result

def remove_special_characters_udf(input_df, column_names):
    for column_name in column_names:
        input_df = input_df.withColumn(column_name, regexp_replace(col(column_name), "[^a-zA-Z0-9 ]", " "))
    return input_df

query_1= add_collum_date_time(parsed_df)
query_2= remove_special_characters_udf(query_1,["Price","Market Cap","Volume"])

SQL_USERNAME = 'sa'
SQL_PASSWORD = '123'
SQL_DBNAME = 'test'
SQL_SERVERNAME = 'localhost:1433'  # Default SQL Server port
TABLENAME = 'dbo.test'
url = f"jdbc:sqlserver://{SQL_SERVERNAME};databaseName={SQL_DBNAME};encrypt=false"

# SQL Server properties
sqlserver_properties = {
    "user": SQL_USERNAME,
    "password": SQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# Write the resulting DataFrame to the console
query = query_2.writeStream \
    .outputMode("append") \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", TABLENAME) \
    .option("user", SQL_USERNAME) \
    .option("password", SQL_PASSWORD) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .trigger(processingTime="0 seconds") \
    .start()

query.awaitTermination()
