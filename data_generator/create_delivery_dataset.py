from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

spark = SparkSession.builder \
    .appName("Delivery_Status_Dataset_Creation") \
    .getOrCreate()

print("Spark Session Created Successfully")


# ==========================================================
# LOAD MASTER DATASET
# ==========================================================

master_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("master_dataset.csv")

print("Master Dataset Loaded Successfully")


# ==========================================================
# CREATE DELIVERY STATUS DATASET
# ==========================================================

delivery_df = master_df.select(
    "MWN",
    "Hub"
)


# ==========================================================
# CREATE ROW ID
# ==========================================================

delivery_df = delivery_df.withColumn(
    "row_id",
    monotonically_increasing_id()
)


# ==========================================================
# DRIVER ID
# ==========================================================

delivery_df = delivery_df.withColumn(
    "Driver_ID",
    concat(
        lit("DRV"),
        lpad(
            ((col("row_id") % 500) + 1).cast("string"),
            4,
            "0"
        )
    )
)


# ==========================================================
# DRIVER NAME
# ==========================================================

delivery_df = delivery_df.withColumn(
    "Driver_Name",
    when(col("row_id") % 5 == 0, "Ramesh")
    .when(col("row_id") % 5 == 1, "Suresh")
    .when(col("row_id") % 5 == 2, "Karthik")
    .when(col("row_id") % 5 == 3, "Praveen")
    .otherwise("Arun")
)


# ==========================================================
# VEHICLE NUMBER
# ==========================================================

delivery_df = delivery_df.withColumn(
    "Vehicle_No",
    concat(
        lit("TN"),
        ((col("row_id") % 90) + 10).cast("string"),
        lit("AB"),
        ((col("row_id") % 9000) + 1000).cast("string")
    )
)


# ==========================================================
# DELIVERY STATUS
# ==========================================================

delivery_df = delivery_df.withColumn(
    "Delivery_Status",
    when(col("row_id") % 6 == 0, "Delivered")
    .when(col("row_id") % 6 == 1, "Pending")
    .when(col("row_id") % 6 == 2, "Out For Delivery")
    .when(col("row_id") % 6 == 3, "Hold")
    .when(col("row_id") % 6 == 4, "Returned")
    .otherwise("RTO")
)


# ==========================================================
# ATTEMPT COUNT
# ==========================================================

delivery_df = delivery_df.withColumn(
    "Attempt_Count",
    ((col("row_id") % 3) + 1).cast(IntegerType())
)


# ==========================================================
# DELIVERY DATE
# ==========================================================

delivery_df = delivery_df.withColumn(
    "Delivery_Date",
    date_sub(
        current_date(),
        (col("row_id") % 15).cast(IntegerType())
    )
)


# ==========================================================
# REMOVE HELPER COLUMN
# ==========================================================

delivery_df = delivery_df.drop("row_id")


# ==========================================================
# PREVIEW
# ==========================================================

delivery_df.show(10, False)

print("Total Rows :", delivery_df.count())
print("Total Columns :", len(delivery_df.columns))

delivery_df.printSchema()


# ==========================================================
# SAVE DELIVERY STATUS DATASET
# ==========================================================

delivery_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("delivery_status")

print("Delivery Status Dataset Created Successfully")
