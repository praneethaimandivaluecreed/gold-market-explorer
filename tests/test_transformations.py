
from src.ingestion import create_spark_session, load_raw_data
from src.transformations import transform_regional_market


def main():

    spark = create_spark_session()

    try:
        # Load the original raw datasets.
        dfs = load_raw_data(spark)

        # Transform regional market data.
        curated_df, quarantine_df = transform_regional_market(
            dfs["regional_market"]
        )

        print("\nCURATED REGIONAL MARKET DATA")
        curated_df.show(truncate=False)

        print("\nQUARANTINED REGIONAL MARKET RECORDS")
        quarantine_df.show(truncate=False)

        print("\nCURATED ROW COUNT:", curated_df.count())
        print("QUARANTINE ROW COUNT:", quarantine_df.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
