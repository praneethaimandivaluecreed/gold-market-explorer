
from src.ingestion import create_spark_session, load_raw_data
from src.transformations import run_transformations
from src.analytics import run_analytics


def main():
    spark = create_spark_session()

    try:
        # Step 1: Read the source CSV files
        print("\n========== DATA INGESTION ==========")
        raw_dfs = load_raw_data(spark)

        # Step 2: Apply data cleaning and transformation rules
        print("\n========== DATA TRANSFORMATION ==========")
        curated_dfs, quarantine_dfs = run_transformations(raw_dfs)

        # Step 3: Execute all core and additional analytics
        print("\n========== DATA ANALYTICS ==========")
        analytics_dfs, sql_comparisons = run_analytics(
            spark,
            curated_dfs
        )

        print("\n========== SQL VALIDATION ==========")
        for check, result in sql_comparisons.items():
            print(f"{check}: {result}")

        print("\n========== PIPELINE COMPLETED ==========")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
