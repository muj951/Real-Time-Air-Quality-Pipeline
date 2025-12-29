# Import the necessary Libraries & function
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from datetime import datetime
import time

# --- CONFIGURATION ---
KAFKA_TOPIC = "openaq_raw_readings"
KAFKA_SERVER = "master:9092"

# Database Config
POSTGRES_URL = "jdbc:postgresql://10.0.0.41:5432/air_quality_db"
DB_USER = "postgres"
DB_PASS = "Paris/muj@890"
# NOTE: The target table must be configured to handle the larger volume of data.
DB_TABLE_TS = "air_quality_all_sensors" 
# ---------------------------------------------------------------------------------

# HDFS Config
HDFS_OUTPUT_PATH = "hdfs://master:9000/user/adm-mcsc/air_quality_archive_all" 
CHECKPOINT_PATH = "hdfs://master:9000/user/adm-mcsc/checkpoints_dual"

TARGET_INDEX = 1
TARGET_PARAMETER_LABEL = "PM2.5 µg/m³"

# --- SCHEMAS ---

# 1. Schema for a single sensor reading (nested inside the main payload)
sensor_reading_schema = StructType([
    StructField("datetime", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ])),
    StructField("value", DoubleType(), True),
    StructField("coordinates", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])),
    StructField("sensorsId", LongType(), True),
    StructField("locationsId", LongType(), True)
])

# 2. Master Schema for the Kafka message value (The JSON sent by the producer)
master_input_schema = StructType([
    StructField("location_id", LongType(), True),
    StructField("station_name", StringType(), True),
    StructField("scrape_time_utc", StringType(), True),
    StructField("target_index", LongType(), True),
    StructField("parameter_label", StringType(), True),
    StructField("raw_readings", ArrayType(sensor_reading_schema), True) # The full raw list
])


def process_batch(df, epoch_id):
    print(f"--- Processing Batch {epoch_id} ---")

    if df.rdd.isEmpty():
        print("Batch is empty. Skipping.")
        return

    # --- 1. TRANSFORMATION: EXPLODE ALL SENSOR DATA ---

    # Use the explode function to create a new row for every element in the 'raw_readings' array.
    # The 'reading' column now represents one single sensor measurement.
    exploded_df = df.select(
        col("location_id"),
        col("station_name"),
        col("scrape_time_utc"),
        explode(col("raw_readings")).alias("reading")
    )

    # --- 2. FINAL SELECTION & CLEANING (for all sensors) ---
    final_df = exploded_df.select(
        col("location_id"),
        col("station_name"),
        # Select the unique Sensor ID for this reading
        col("reading.sensorsId").alias("sensor_id"),
        col("reading.value").alias("sensor_value"),
        # Extract metadata
        to_timestamp(col("reading.datetime.utc")).alias("sensor_timestamp_utc"),
        to_timestamp(col("scrape_time_utc")).alias("scrape_timestamp_utc"),
        col("reading.coordinates.latitude").alias("latitude"),
        col("reading.coordinates.longitude").alias("longitude")
    )

    if final_df.rdd.isEmpty():
        print("No data found in this batch. Skipping writes.")
        return

    # Persist the DF so it's only computed once before writing to two sinks
    final_df.persist()

    # --- 3. Write to HDFS (contains ALL sensor data) ---
    (final_df.write
        .mode("append")
        .format("parquet")
        .save(HDFS_OUTPUT_PATH))
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Written all sensor data to HDFS.")

    # --- 4. Write to Postgres ---
    try:
        # NOTE: MUST create the air_quality_all_sensors table in Postgres first.
        (final_df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", DB_TABLE_TS)
            .option("user", DB_USER)
            .option("password", DB_PASS)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save())
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Written all sensor data to Database.")

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Database Write Failed: {e}")

    final_df.unpersist() # Release the cache

# --- MAIN JOB ---
spark = SparkSession.builder \
    .appName("OpenAQAllSensorsWriter") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Read Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Parse the Stream using the NEW nested schema
parsed_df = df.select(from_json(col("value").cast("string"), master_input_schema).alias("data")) \
    .select("data.*")

# 3. Start the Processing Query
streaming_query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="1 minute") \
    .start()

# 4. Wait for the termination of the streaming query
streaming_query.awaitTermination()