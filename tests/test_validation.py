from pyspark.sql import DataFrame

from src.ingestion import create_spark_session, load_raw_data
from src.validation import run_all_validations


def main():
    spark = create_spark_session()

    try:
        # Load all four raw datasets.
        dfs = load_raw_data(spark)

        # Run individual-file and cross-file validations.
        validation_results = run_all_validations(dfs)

        # Display all validation findings.
        for category, datasets in validation_results.items():

            print(f"\n========== {category.upper()} VALIDATION ==========")

            for dataset_name, findings in datasets.items():

                print(f"\n--- {dataset_name} ---")

                # Individual validation returns a dictionary of DataFrames.
                if isinstance(findings, dict):

                    for finding_name, finding_df in findings.items():

                        issue_count = finding_df.count()

                        print(f"\n{finding_name}: {issue_count} records")

                        if issue_count > 0:
                            finding_df.show(5, truncate=False)

                # Cross-file validation returns individual DataFrames.
                elif isinstance(findings, DataFrame):

                    issue_count = findings.count()

                    print(f"\nTotal records: {issue_count}")

                    if issue_count > 0:
                        findings.show(5, truncate=False)

    finally:
        # Always stop Spark, even if an error occurs.
        spark.stop()


if __name__ == "__main__":
    main()