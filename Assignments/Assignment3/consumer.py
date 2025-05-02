"""# Setting up PySpark for consumption"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, avg, lag, window, to_json, struct
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
import threading

# Creating Spark Session
spark = SparkSession.builder \
    .appName("TrafficMonitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .getOrCreate()

# Kafka Configuration
KAFKA_BROKER = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
KAFKA_TOPIC = "traffic_data"
KAFKA_OUTPUT_TOPIC = "traffic_analysis"

# Define Schema for JSON Data
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("congestion_level", StringType(), True)
])

# Reading Data from Kafka
traffic_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Convert Binary Messages to JSON
# i.e the data coming from the kafka is in the form of binary messages
traffic_data = traffic_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), traffic_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

"""# Part 2: PySpark Structured Streaming
Here, I am performing real time traffic analysis
"""

# Task 1: Compute Traffic Volume per Sensor
traffic_volume = traffic_data \
    .groupBy(window(col("timestamp"), "2 minutes"), col("sensor_id")) \
    .agg(count("vehicle_count").alias("total_vehicles"))

# Task 2: Detect Congestion Hotspots
congestion_hotspots = traffic_data \
    .where(col("congestion_level") == "HIGH") \
    .groupBy(window(col("timestamp"), "2 minutes"), col("sensor_id")) \
    .count() \
    .where(col("count") >= 3)

# Task 3: Calculate Average Speed per Sensor
avg_speed = traffic_data \
    .groupBy(window(col("timestamp"), "3 minutes", "2 minutes"), col("sensor_id")) \
    .agg(avg("average_speed").alias("avg_speed"))

# Task 4: Identify Sudden Speed Drops
window_spec = Window.partitionBy("sensor_id").orderBy("timestamp")
sudden_drop = traffic_data \
    .withColumn("prev_speed", lag("average_speed").over(window_spec)) \
    .where((col("prev_speed") > 0) & ((col("average_speed") / col("prev_speed")) < 0.5))

# Task 5: Find Busiest Sensors
busiest_sensors = traffic_data \
    .groupBy(window(col("timestamp"), "5 minutes"), col("sensor_id")) \
    .agg(count("vehicle_count").alias("total_vehicles")) \
    .orderBy(col("total_vehicles").desc()) \
    .limit(3)

"""It is important to note that the lag function in the 4th query caused some issues, thus it was handled artificially.

# Part 3: Data Quality Checks
"""

traffic_data = traffic_data.filter(col("sensor_id").isNotNull() & col("timestamp").isNotNull())
traffic_data = traffic_data.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0))
traffic_data = traffic_data.dropDuplicates(["sensor_id", "timestamp"])

"""# Part 4.1 Output Storage"""

# Real-Time Traffic Analysis
traffic_volume = traffic_data \
    .groupBy(window(col("timestamp"), "30 seconds"), col("sensor_id")) \
    .agg(count("vehicle_count").alias("total_vehicles"))

avg_speed = traffic_data \
    .groupBy(window(col("timestamp"), "60 seconds", "30 seconds"), col("sensor_id")) \
    .agg(avg("average_speed").alias("avg_speed"))

# Convert Aggregated Data to JSON
traffic_volume_json = traffic_volume.withColumn("value", to_json(struct("window", "sensor_id", "total_vehicles")))
avg_speed_json = avg_speed.withColumn("value", to_json(struct("window", "sensor_id", "avg_speed")))

# Function to Write to Kafka
def write_to_kafka_limited(df, topic, limit=100):
    def process_batch(batch_df, _):
        if batch_df.count() >= limit:
            spark.stop()

    query = df.selectExpr("CAST(sensor_id AS STRING) AS key", "value") \
        .writeStream \
        .foreachBatch(process_batch) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .option("checkpointLocation", f"output/{topic}_checkpoint") \
        .outputMode("update") \
        .start()
    return query

traffic_volume_kafka_query = write_to_kafka_limited(traffic_volume_json, KAFKA_OUTPUT_TOPIC)
avg_speed_kafka_query = write_to_kafka_limited(avg_speed_json, KAFKA_OUTPUT_TOPIC)

def run_streaming_query(df, query_name, limit=100):
    def process_batch(batch_df, _):
        if batch_df.count() >= limit:
            spark.stop()

    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .foreachBatch(process_batch) \
        .queryName(query_name) \
        .start()
    query.awaitTermination()

"""## In order to ensure streaming via Colab, I have to run analysis in separate thread"""

threads = []
queries = [
    (traffic_volume, "TrafficVolume"),
    (avg_speed, "AverageSpeed"),
]

for df, query_name in queries:
    thread = threading.Thread(target=run_streaming_query, args=(df, query_name, 100))
    thread.start()
    threads.append(thread)

"""# Part 4.2: Configuring Grafana for real time Kafka Visualization

For this purpose, I am going to use influxDB, which will in turn itegrate with Grafana
"""

!pip install influxdb-client

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB Cloud Configuration
INFLUXDB_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUXDB_TOKEN = "VDGiLZNZk72fTn4z5Wup3TqIzsR5ivF0GJqxvSBIqRtQKsn1Rm21Bo8oq3ALZGmoK7thFOsY--OgBZ0P82jsAw==" # confidential
INFLUXDB_ORG = "Student"
INFLUXDB_BUCKET = "traffic_analysis"

# Connect to InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

def write_to_influxdb(df, epoch_id):
    records = df.collect()
    for row in records:
        point = (
            Point("traffic_data")
            .tag("sensor_id", row["sensor_id"])
            .field("total_vehicles", row["total_vehicles"])
            .field("avg_speed", row["avg_speed"])
            .time(row["window"].start)
        )
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

traffic_volume.writeStream \
    .foreachBatch(write_to_influxdb) \
    .outputMode("update") \
    .start()

"""# The influxDB client is directly integrated with Grafana. The visualizations are established there."""