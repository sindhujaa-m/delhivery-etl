from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import shutil

# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

spark = SparkSession.builder \
    .appName("Master_Dataset_Creation") \
    .getOrCreate()

print("Spark Session Created Successfully")


# ==========================================================
# LOAD ORIGINAL DATASET
# ==========================================================

# Upload today's original CSV before running this script.
filename = "today_dataset.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(filename)

print("Original Dataset Loaded Successfully")


# ==========================================================
# CREATE ALL INDIA MASTER DATASET
# ==========================================================

copies = 10

master_df = df

for i in range(copies - 1):
    master_df = master_df.union(df)

print("Master Dataset Created Successfully")
print("Total Rows :", master_df.count())


# ==========================================================
# ADD ROW ID
# ==========================================================

master_df = master_df.withColumn(
    "row_id",
    monotonically_increasing_id()
)


# ==========================================================
# ADD STATE
# ==========================================================

master_df = master_df.withColumn(
    "State",
    when(col("row_id") % 6 == 0, "Tamil Nadu")
    .when(col("row_id") % 6 == 1, "Karnataka")
    .when(col("row_id") % 6 == 2, "Kerala")
    .when(col("row_id") % 6 == 3, "Telangana")
    .when(col("row_id") % 6 == 4, "Andhra Pradesh")
    .otherwise("Maharashtra")
)


# ==========================================================
# ADD CITY
# ==========================================================

master_df = master_df.withColumn(
    "City",
    when(col("State") == "Tamil Nadu", "Madurai")
    .when(col("State") == "Karnataka", "Bangalore")
    .when(col("State") == "Kerala", "Kochi")
    .when(col("State") == "Telangana", "Hyderabad")
    .when(col("State") == "Andhra Pradesh", "Vijayawada")
    .otherwise("Mumbai")
)


# ==========================================================
# ADD REGION
# ==========================================================

master_df = master_df.withColumn(
    "Region",
    when(
        col("State").isin(
            "Tamil Nadu",
            "Karnataka",
            "Kerala",
            "Telangana",
            "Andhra Pradesh"
        ),
        "South"
    )
    .otherwise("West")
)


# ==========================================================
# ADD HUB
# ==========================================================

master_df = master_df.withColumn(
    "Hub",
    when(col("City") == "Madurai", "MDU01")
    .when(col("City") == "Bangalore", "BLR01")
    .when(col("City") == "Kochi", "COK01")
    .when(col("City") == "Hyderabad", "HYD01")
    .when(col("City") == "Vijayawada", "VJA01")
    .otherwise("BOM01")
)


# ==========================================================
# CREATE DUPLICATE ROWS
# ==========================================================

duplicate_df = master_df.limit(100)

master_df = master_df.union(duplicate_df)

print("Duplicate Rows Added Successfully")
print("Total Rows :", master_df.count())


# ==========================================================
# CREATE NULL VALUES
# ==========================================================

master_df = master_df.withColumn(
    "Pincode",
    when(col("row_id") % 10 == 0, None)
    .otherwise(col("Pincode"))
)

master_df = master_df.withColumn(
    "Route",
    when(col("row_id") % 15 == 0, None)
    .otherwise(col("Route"))
)

master_df = master_df.withColumn(
    "Remark",
    when(col("row_id") % 20 == 0, None)
    .otherwise(col("Remark"))
)


# ==========================================================
# CREATE DIRTY DATA
# ==========================================================

# Invalid Weight
master_df = master_df.withColumn(
    "Weight (Kg)",
    when(col("row_id") % 25 == 0, -5)
    .otherwise(col("Weight (Kg)"))
)

# Invalid Pincode
master_df = master_df.withColumn(
    "Pincode",
    when(col("row_id") % 30 == 0, 123)
    .otherwise(col("Pincode"))
)

# Extra spaces in Client
master_df = master_df.withColumn(
    "Client",
    when(
        col("row_id") % 40 == 0,
        concat(
            lit("   "),
            col("Client"),
            lit("   ")
        )
    )
    .otherwise(col("Client"))
)

# Mixed case Client
master_df = master_df.withColumn(
    "Client",
    when(
        col("row_id") % 50 == 0,
        "ajio surface"
    )
    .otherwise(col("Client"))
)

# Invalid Route
master_df = master_df.withColumn(
    "Route",
    when(
        col("row_id") % 35 == 0,
        "UNKNOWN_ROUTE"
    )
    .otherwise(col("Route"))
)


# ==========================================================
# PREVIEW MASTER DATASET
# ==========================================================

print("\n========== MASTER DATASET ==========\n")

master_df.show(10, truncate=False)

print("Total Rows :", master_df.count())
print("Total Columns :", len(master_df.columns))

print("\nSchema:")
master_df.printSchema()


# ==========================================================
# SAVE MASTER DATASET
# ==========================================================

master_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("master_dataset")

print("Master Dataset Saved Successfully")


# ==========================================================
# SAVE BRONZE LAYER
# ==========================================================

master_df.write \
    .mode("overwrite") \
    .parquet("bronze_layer")

print("Bronze Layer Created Successfully")
