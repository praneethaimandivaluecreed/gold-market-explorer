
from pathlib import Path

import pandas as pd

from pyspark.sql import functions as F



# PROJECT OUTPUT CONFIGURATION

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "outputs"



# REGIONAL MARKET TRANSFORMATION

def transform_regional_market(df):
    """
    Standardize regional reference data.

    Invalid making charges are set to NULL in curated data
    and preserved in quarantine.

    Returns:
        curated_df, quarantine_df
    """

    standardized_df = (
        df
        .withColumn("region_id", F.trim(F.col("region_id")))
        .withColumn("region_name", F.trim(F.col("region_name")))
        .withColumn("state", F.trim(F.col("state")))
        .withColumn(
            "making_charge_pct",
            F.trim(F.col("making_charge_pct"))
        )
        .withColumn(
            "parsed_making_charge_pct",
            F.expr("try_cast(making_charge_pct AS DOUBLE)")
        )
    )

    # Identify invalid making charges.
    invalid_df = standardized_df.filter(
        F.col("parsed_making_charge_pct").isNull()
        | (F.col("parsed_making_charge_pct") <= 0)
    )

    # Preserve original values and add quarantine reason.
    quarantine_df = (
        invalid_df
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("parsed_making_charge_pct").isNull(),
                F.lit("INVALID_MAKING_CHARGE")
            ).otherwise(
                F.lit("NON_POSITIVE_MAKING_CHARGE")
            )
        )
        .drop("parsed_making_charge_pct")
    )

    # Retain the region but set invalid making charge to NULL.
    curated_df = (
        standardized_df
        .withColumn(
            "making_charge_pct",
            F.when(
                F.col("parsed_making_charge_pct") > 0,
                F.col("parsed_making_charge_pct")
            ).otherwise(
                F.lit(None).cast("double")
            )
        )
        .drop("parsed_making_charge_pct")
    )

    return curated_df, quarantine_df


# ============================================================
# 2. GOLD DAILY PRICES TRANSFORMATION
# ============================================================

def transform_gold_daily_prices(df):
    """
    Standardize daily gold prices.

    - Normalize date formatting.
    - Standardize market and source labels.
    - Remove excess exact duplicates.
    - Quarantine conflicting price records.
    - Quarantine invalid prices.

    Returns:
        curated_df, quarantine_df
    """

    standardized_df = (
        df
        .withColumn(
            "normalized_price_date",
            F.regexp_replace(
                F.trim(F.col("price_date")),
                "/",
                "-"
            )
        )
        .withColumn("market", F.trim(F.col("market")))
        .withColumn(
            "source_label",
            F.lower(F.trim(F.col("source_label")))
        )
    )

    # Parse dates and numeric values safely.
    parsed_df = (
        standardized_df
        .withColumn(
            "parsed_price_date",
            F.expr(
                "try_to_timestamp(normalized_price_date, 'yyyy-MM-dd')"
            ).cast("date")
        )
        .withColumn(
            "parsed_price_inr",
            F.expr("try_cast(price_per_10g_inr AS DOUBLE)")
        )
        .withColumn(
            "parsed_usd_inr",
            F.expr("try_cast(usd_inr AS DOUBLE)")
        )
        .withColumn(
            "parsed_global_gold_usd_oz",
            F.expr("try_cast(global_gold_usd_oz AS DOUBLE)")
        )
    )

    # Identify conflicting date-market combinations.
    conflicting_keys = (
        parsed_df
        .groupBy("parsed_price_date", "market")
        .agg(
            F.countDistinct(
                F.struct(
                    "price_per_10g_inr",
                    "usd_inr",
                    "global_gold_usd_oz"
                )
            ).alias("distinct_records")
        )
        .filter(F.col("distinct_records") > 1)
        .select("parsed_price_date", "market")
    )

    # Keep both records for conflicting keys.
    conflict_records = parsed_df.join(
        conflicting_keys,
        on=["parsed_price_date", "market"],
        how="inner"
    )

    # Separate non-conflicting records.
    non_conflicting_records = parsed_df.join(
        conflicting_keys,
        on=["parsed_price_date", "market"],
        how="left_anti"
    )

    # Remove only excess exact duplicates.
    deduplicated_df = non_conflicting_records.dropDuplicates(
        standardized_df.columns
    )

    # Add conflict flag.
    conflict_records = conflict_records.withColumn(
        "is_conflicting",
        F.lit(True)
    )

    deduplicated_df = deduplicated_df.withColumn(
        "is_conflicting",
        F.lit(False)
    )

    combined_df = deduplicated_df.unionByName(conflict_records)

    # Identify invalid price records.
    invalid_df = combined_df.filter(
        F.col("parsed_price_date").isNull()
        | F.col("parsed_price_inr").isNull()
        | (F.col("parsed_price_inr") <= 0)
        | F.col("parsed_usd_inr").isNull()
        | (F.col("parsed_usd_inr") <= 0)
        | F.col("parsed_global_gold_usd_oz").isNull()
        | (F.col("parsed_global_gold_usd_oz") <= 0)
        | F.col("is_conflicting")
    )

    # Add quarantine reasons.
    quarantine_df = (
        invalid_df
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("is_conflicting"),
                F.lit("CONFLICTING_PRICE_RECORDS")
            )
            .when(
                F.col("parsed_price_date").isNull(),
                F.lit("INVALID_PRICE_DATE")
            )
            .when(
                F.col("parsed_price_inr").isNull(),
                F.lit("INVALID_INR_PRICE")
            )
            .when(
                F.col("parsed_price_inr") <= 0,
                F.lit("NON_POSITIVE_INR_PRICE")
            )
            .when(
                F.col("parsed_usd_inr").isNull()
                | (F.col("parsed_usd_inr") <= 0),
                F.lit("INVALID_USD_INR")
            )
            .otherwise(
                F.lit("INVALID_GLOBAL_GOLD_PRICE")
            )
        )
        .drop(
            "parsed_price_date",
            "parsed_price_inr",
            "parsed_usd_inr",
            "parsed_global_gold_usd_oz",
            "is_conflicting"
        )
    )

    # Retain only valid daily price records.
    curated_df = (
        combined_df
        .filter(
            (~F.col("is_conflicting"))
            & F.col("parsed_price_date").isNotNull()
            & (F.col("parsed_price_inr") > 0)
            & (F.col("parsed_usd_inr") > 0)
            & (F.col("parsed_global_gold_usd_oz") > 0)
        )
        .select(
            F.col("parsed_price_date").alias("price_date"),
            "market",
            F.col("parsed_price_inr").alias("price_per_10g_inr"),
            F.col("parsed_usd_inr").alias("usd_inr"),
            F.col("parsed_global_gold_usd_oz").alias(
                "global_gold_usd_oz"
            ),
            "source_label"
        )
    )

    return curated_df, quarantine_df



# GOLD TRANSACTIONS TRANSFORMATION

def transform_gold_transactions(df, regional_df=None):
    """
    Standardize transaction records.

    - Standardize text columns.
    - Remove excess exact duplicates.
    - Quarantine conflicting transaction IDs.
    - Quarantine invalid dates and negative weights.
    - Convert malformed numeric values to NULL.
    - Flag unmatched region IDs.

    Returns:
        curated_df, quarantine_df
    """

    standardized_df = (
        df
        .withColumn(
            "transaction_id",
            F.trim(F.col("transaction_id"))
        )
        .withColumn("sale_date", F.trim(F.col("sale_date")))
        .withColumn("region_id", F.trim(F.col("region_id")))
        .withColumn(
            "product_type",
            F.lower(F.trim(F.col("product_type")))
        )
        .withColumn(
            "purity",
            F.upper(F.trim(F.col("purity")))
        )
        .withColumn(
            "transaction_status",
            F.lower(F.trim(F.col("transaction_status")))
        )
        .withColumn(
            "weight_grams",
            F.trim(F.col("weight_grams"))
        )
        .withColumn(
            "quoted_rate_per_10g",
            F.trim(F.col("quoted_rate_per_10g"))
        )
        .withColumn(
            "making_charge_pct",
            F.trim(F.col("making_charge_pct"))
        )
    )

    # Parse date and numeric fields.
    parsed_df = (
        standardized_df
        .withColumn(
            "parsed_sale_date",
            F.expr(
                "try_to_timestamp(sale_date, 'yyyy-MM-dd')"
            ).cast("date")
        )
        .withColumn(
            "parsed_weight",
            F.expr("try_cast(weight_grams AS DOUBLE)")
        )
        .withColumn(
            "parsed_quoted_rate",
            F.expr("try_cast(quoted_rate_per_10g AS DOUBLE)")
        )
        .withColumn(
            "parsed_making_charge",
            F.expr("try_cast(making_charge_pct AS DOUBLE)")
        )
    )

    # Remove excess exact duplicate records.
    deduplicated_df = parsed_df.dropDuplicates(
        standardized_df.columns
    )

    # Identify conflicting transaction IDs.
    conflicting_ids = (
        deduplicated_df
        .groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .select("transaction_id")
    )

    # Preserve all records belonging to conflicting IDs.
    conflicting_records = deduplicated_df.join(
        conflicting_ids,
        on="transaction_id",
        how="inner"
    )

    # Separate records with non-conflicting IDs.
    valid_id_records = deduplicated_df.join(
        conflicting_ids,
        on="transaction_id",
        how="left_anti"
    )

    # Identify unmatched region IDs.
    if regional_df is not None:

        valid_region_ids = (
            regional_df
            .select("region_id")
            .distinct()
            .withColumn("region_found", F.lit(True))
        )

        valid_id_records = valid_id_records.join(
            valid_region_ids,
            on="region_id",
            how="left"
        )

        valid_id_records = valid_id_records.withColumn(
            "region_found",
            F.coalesce(F.col("region_found"), F.lit(False))
        )

    else:
        valid_id_records = valid_id_records.withColumn(
            "region_found",
            F.lit(True)
        )

    # Determine field-level data quality issues.
    invalid_date = F.col("parsed_sale_date").isNull()

    negative_weight = F.col("parsed_weight") < 0

    malformed_weight = (
        F.col("weight_grams").isNotNull()
        & (F.trim(F.col("weight_grams")) != "")
        & F.col("parsed_weight").isNull()
    )

    malformed_rate = (
        F.col("quoted_rate_per_10g").isNotNull()
        & (F.trim(F.col("quoted_rate_per_10g")) != "")
        & F.col("parsed_quoted_rate").isNull()
    )

    malformed_making_charge = (
        F.col("making_charge_pct").isNotNull()
        & (F.trim(F.col("making_charge_pct")) != "")
        & F.col("parsed_making_charge").isNull()
    )

    unmatched_region = ~F.col("region_found")

    # Create quarantine records with one or more reasons.
    issue_records = (
        valid_id_records
        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                "|",
                F.when(
                    invalid_date,
                    F.lit("INVALID_SALE_DATE")
                ),
                F.when(
                    negative_weight,
                    F.lit("NEGATIVE_TRANSACTION_WEIGHT")
                ),
                F.when(
                    malformed_weight,
                    F.lit("INVALID_WEIGHT_VALUE")
                ),
                F.when(
                    malformed_rate,
                    F.lit("INVALID_QUOTED_RATE")
                ),
                F.when(
                    malformed_making_charge,
                    F.lit("INVALID_MAKING_CHARGE")
                ),
                F.when(
                    unmatched_region,
                    F.lit("UNMATCHED_REGION_ID")
                )
            )
        )
    )

    # Conflicting IDs are also quarantined.
    conflict_quarantine = conflicting_records.withColumn(
        "quarantine_reason",
        F.lit("CONFLICTING_TRANSACTION_ID")
    )

    # Include only records that have at least one issue.
    field_quarantine = issue_records.filter(
        F.col("quarantine_reason") != ""
    )

    quarantine_df = (
        field_quarantine
        .unionByName(conflict_quarantine, allowMissingColumns=True)
        .withColumn(
            "record_action",
            F.when(
                F.col("quarantine_reason").contains(
                    "CONFLICTING_TRANSACTION_ID"
                )
                | F.col("quarantine_reason").contains(
                    "INVALID_SALE_DATE"
                )
                | F.col("quarantine_reason").contains(
                    "NEGATIVE_TRANSACTION_WEIGHT"
                ),
                F.lit("EXCLUDED_FROM_CURATED")
            ).otherwise(
                F.lit("RETAINED_WITH_FIELD_CLEANUP_OR_FLAG")
            )
        )
        .drop(
            "parsed_sale_date",
            "parsed_weight",
            "parsed_quoted_rate",
            "parsed_making_charge",
            "region_found"
        )
    )

    # Curated data excludes conflicting IDs, invalid dates,
    # and negative weights.
    curated_df = (
        valid_id_records
        .filter(
            F.col("parsed_sale_date").isNotNull()
            & (
                F.col("parsed_weight").isNull()
                | (F.col("parsed_weight") >= 0)
            )
        )
        .withColumn(
            "sale_date",
            F.col("parsed_sale_date")
        )
        .withColumn(
            "weight_grams",
            F.col("parsed_weight")
        )
        .withColumn(
            "quoted_rate_per_10g",
            F.col("parsed_quoted_rate")
        )
        .withColumn(
            "making_charge_pct",
            F.col("parsed_making_charge")
        )
        .withColumn(
            "data_quality_flag",
            F.when(
                ~F.col("region_found"),
                F.lit("UNMATCHED_REGION_ID")
            )
        )
        .drop(
            "parsed_sale_date",
            "parsed_weight",
            "parsed_quoted_rate",
            "parsed_making_charge",
            "region_found"
        )
    )

    return curated_df, quarantine_df



# MARKET EVENTS TRANSFORMATION
def transform_market_events(df, regional_df=None):
    """
    Standardize market event records.

    Invalid dates are quarantined.
    Unmatched region IDs are retained and flagged.

    Returns:
        curated_df, quarantine_df
    """

    standardized_df = (
        df
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("event_date", F.trim(F.col("event_date")))
        .withColumn("region_id", F.trim(F.col("region_id")))
        .withColumn(
            "event_type",
            F.lower(F.trim(F.col("event_type")))
        )
    )

    parsed_df = standardized_df.withColumn(
        "parsed_event_date",
        F.expr(
            "try_to_timestamp(event_date, 'yyyy-MM-dd')"
        ).cast("date")
    )

    # Identify invalid event dates.
    invalid_df = parsed_df.filter(
        F.col("parsed_event_date").isNull()
    )

    quarantine_df = invalid_df.withColumn(
        "quarantine_reason",
        F.lit("INVALID_EVENT_DATE")
    ).drop("parsed_event_date")

    # Keep valid event dates.
    curated_df = (
        parsed_df
        .filter(F.col("parsed_event_date").isNotNull())
        .withColumn(
            "event_date",
            F.col("parsed_event_date")
        )
        .drop("parsed_event_date")
    )

    # Flag unmatched region IDs without removing events.
    if regional_df is not None:

        valid_region_ids = (
            regional_df
            .select("region_id")
            .distinct()
            .withColumn("region_found", F.lit(True))
        )

        curated_df = curated_df.join(
            valid_region_ids,
            on="region_id",
            how="left"
        )

        curated_df = (
            curated_df
            .withColumn(
                "region_found",
                F.coalesce(F.col("region_found"), F.lit(False))
            )
            .withColumn(
                "data_quality_flag",
                F.when(
                    ~F.col("region_found"),
                    F.lit("UNMATCHED_REGION_ID")
                )
            )
            .drop("region_found")
        )

    else:
        curated_df = curated_df.withColumn(
            "data_quality_flag",
            F.lit(None).cast("string")
        )

    return curated_df, quarantine_df



# OUTPUT STORAGE FUNCTIONS


def save_dataframe(df, output_path):
    """
    Save a Spark DataFrame as a CSV file using Pandas.

    This avoids Spark's Hadoop-based CSV writer on Windows.
    """

    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    # Convert Spark DataFrame to Pandas.
    pandas_df = df.toPandas()

    # Save as CSV.
    pandas_df.to_csv(
        output_path / "data.csv",
        index=False
    )

    print(f"Saved successfully: {output_path / 'data.csv'}")


def save_transformation_outputs(curated_dfs, quarantine_dfs):
    """
    Save all curated and quarantine datasets as CSV.

    Also creates one Excel workbook with a separate
    worksheet for each dataset's quarantine records.
    """

    # Save curated datasets.
    for name, df in curated_dfs.items():

        output_path = OUTPUT_PATH / "curated" / name

        save_dataframe(df, output_path)

        print(f"Curated data saved: {name}")

    # Save quarantine datasets.
    for name, df in quarantine_dfs.items():

        output_path = OUTPUT_PATH / "quarantine" / name

        save_dataframe(df, output_path)

        print(f"Quarantine data saved: {name}")

    # Create consolidated Excel quarantine report.
    excel_path = (
        OUTPUT_PATH / "quarantine" / "quarantine_report.xlsx"
    )

    excel_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:

        for name, df in quarantine_dfs.items():

            pandas_df = df.toPandas()

            pandas_df.to_excel(
                writer,
                sheet_name=name,
                index=False
            )

    print(f"\nExcel quarantine report saved: {excel_path}")



# COMPLETE TRANSFORMATION PIPELINE

def run_transformations(raw_dfs):
    """
    Execute transformations for all four datasets
    and save curated and quarantine outputs.
    """

    curated_dfs = {}
    quarantine_dfs = {}

    # Step 1: Regional market.
    curated_dfs["regional_market"], quarantine_dfs[
        "regional_market"
    ] = transform_regional_market(
        raw_dfs["regional_market"]
    )

    # Step 2: Daily gold prices.
    curated_dfs["gold_daily_prices"], quarantine_dfs[
        "gold_daily_prices"
    ] = transform_gold_daily_prices(
        raw_dfs["gold_daily_prices"]
    )

    # Step 3: Gold transactions.
    curated_dfs["gold_transactions"], quarantine_dfs[
        "gold_transactions"
    ] = transform_gold_transactions(
        raw_dfs["gold_transactions"],
        curated_dfs["regional_market"]
    )

    # Step 4: Market events.
    curated_dfs["market_events"], quarantine_dfs[
        "market_events"
    ] = transform_market_events(
        raw_dfs["market_events"],
        curated_dfs["regional_market"]
    )

    # Print transformation summary.
    print("\n========== TRANSFORMATION SUMMARY ==========")

    for name in curated_dfs:

        print(f"\nDataset: {name}")
        print(
            "Curated records:",
            curated_dfs[name].count()
        )
        print(
            "Quarantined records:",
            quarantine_dfs[name].count()
        )

    # Persist all outputs.
    save_transformation_outputs(
        curated_dfs,
        quarantine_dfs
    )

    print("\nAll transformations completed successfully.")

    return curated_dfs, quarantine_dfs
