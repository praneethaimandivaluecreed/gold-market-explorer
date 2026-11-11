import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)



#  PROJECT PATHS

# __file__ represents the current Python file (ingestion.py).
# parents[1] moves from src/ to the main project directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# All original CSV files will be stored in this directory.
# We only read from this folder; we never modify the raw files.
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"



#  SOURCE SCHEMAS

# We initially define every column as StringType.
# Some source values contain invalid placeholders such as
# "unknown", "N/A", "TBD", or "bad_date".
# Reading them as strings allows us to inspect and handle
# those values safely during validation and transformation.



REGIONAL_MARKET_SCHEMA = StructType([
    StructField("region_id", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("state", StringType(), True),

    # Match the actual source CSV header exactly.
    StructField("making_charge_pct", StringType(), True)
])



GOLD_DAILY_PRICES_SCHEMA = StructType([
    StructField("price_date", StringType(), True),
    StructField("market", StringType(), True),
    StructField("price_per_10g_inr", StringType(), True),
    StructField("usd_inr", StringType(), True),
    StructField("global_gold_usd_oz", StringType(), True),
    StructField("source_label", StringType(), True)
])


GOLD_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("sale_date", StringType(), True),
    StructField("region_id", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("purity", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("quoted_rate_per_10g", StringType(), True),
    StructField("making_charge_pct", StringType(), True),
    StructField("transaction_status", StringType(), True)
])



MARKET_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("region_id", StringType(), True),
    StructField("notes", StringType(), True)
])




# CREATE SPARK SESSION

def create_spark_session():

    # Tell Spark to use the same Python interpreter running this application.
    # This prevents Windows from trying to find Python through the Microsoft Store alias.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("GoldMarketExplorer")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.pyspark.python", sys.executable)
        .getOrCreate()
    )

    return spark

# 4. LOAD RAW CSV FILES

def load_raw_data(spark):

    # Map each logical dataset name to its CSV filename
    # and the schema that should be applied while reading it.

    file_config = {
        "regional_market": {
            "filename": "regional_market.csv",
            "schema": REGIONAL_MARKET_SCHEMA
        },

        "gold_daily_prices": {
            "filename": "gold_daily_prices.csv",
            "schema": GOLD_DAILY_PRICES_SCHEMA
        },

        "gold_transactions": {
            "filename": "gold_transactions.csv",
            "schema": GOLD_TRANSACTIONS_SCHEMA
        },

        "market_events": {
            "filename": "market_events.csv",
            "schema": MARKET_EVENTS_SCHEMA
        }
    }

    # This dictionary will hold the four loaded DataFrames.
    dataframes = {}

    for dataset_name, config in file_config.items():

        # Build the complete path to the current CSV file.
        file_path = RAW_DATA_PATH / config["filename"]

        # Check that the expected source file actually exists.
        # This gives us a clear error instead of a confusing
        # Spark file-not-found exception later.
        if not file_path.exists():
            raise FileNotFoundError(
                f"Missing source file: {file_path}"
            )

        # Read the CSV using its predefined schema.
        # header=True means the first row contains column names.
        # inferSchema=False prevents Spark from guessing data types.
        # PERMISSIVE allows the reader to handle malformed CSV
        # records without immediately stopping the entire load.

        df = (
            spark.read
            .format("csv")
            .option("header", True)
            .option("inferSchema", False)
            .option("mode", "PERMISSIVE")
            .option("encoding", "UTF-8")
            .schema(config["schema"])
            .load(str(file_path))
        )

        # Store the DataFrame using a meaningful dataset name.
        dataframes[dataset_name] = df

        print(f"Loaded successfully: {config['filename']}")

    return dataframes
