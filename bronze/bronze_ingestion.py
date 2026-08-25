from pyspark.sql import SparkSession
import sys

# ==========================================================
# BRONZE LAYER - RAW DATA INGESTION
# ==========================================================

spark = SparkSession.builder \
    .appName("Delhivery_Bronze_Ingestion") \
    .getOrCreate()

print("Spark Session Created Successfully")


# ==========================================================
# INPUT AND OUTPUT PATHS
# ==========================================================

raw_path = "raw_data"

bronze_path = "bronze_data"


# ==========================================================
# 1. ORIGINAL TODAY SHIPMENT DATA
# ==========================================================

today_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{raw_path}/today/*.csv")

today_df.write \
    .mode("overwrite") \
    .parquet(f"{bronze_path}/today_shipments")

print("Today Shipment Bronze Dataset Created")


# ==========================================================
# 2. MASTER SHIPMENT DATA
# ==========================================================

master_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{raw_path}/master/*.csv")

master_df.write \
    .mode("overwrite") \
    .parquet(f"{bronze_path}/master_shipments")

print("Master Shipment Bronze Dataset Created")


# ==========================================================
# 3. DRIVER / DELIVERY STATUS DATA
# ==========================================================

delivery_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{raw_path}/delivery/*.csv")

delivery_df.write \
    .mode("overwrite") \
    .parquet(f"{bronze_path}/delivery_status")

print("Delivery Status Bronze Dataset Created")


# ==========================================================
# VALIDATION
# ==========================================================

print("\n========== BRONZE LAYER VALIDATION ==========")

print("\nToday Shipment Records :", today_df.count())
print("Master Shipment Records :", master_df.count())
print("Delivery Status Records :", delivery_df.count())

print("\nToday Shipment Schema")
today_df.printSchema()

print("\nMaster Shipment Schema")
master_df.printSchema()

print("\nDelivery Status Schema")
delivery_df.printSchema()

print("\nBronze Layer Completed Successfully!")

spark.stop()
