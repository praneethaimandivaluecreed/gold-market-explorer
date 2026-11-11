
from src.ingestion import create_spark_session, load_raw_data

# Start Spark.
spark = create_spark_session()

# Load all four raw datasets.
dfs = load_raw_data(spark)

# Display the schema and a few records from each dataset.
for dataset_name, df in dfs.items():

    print(f"\n========== {dataset_name} ==========")

    df.printSchema()
    df.show(5, truncate=False)

# Stop Spark after testing.
spark.stop()
