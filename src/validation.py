
from pyspark.sql import functions as F



# REUSABLE VALIDATION HELPERS

def get_null_blank_summary(df):
    """
    Counts NULL and blank values in every column.

    NULL means the value is missing.
    Blank means the value exists as a string but contains
    only whitespace.

    We report these separately because they are different
    data-quality problems.
    """

    expressions = []

    for column_name in df.columns:

        # Count actual NULL values.
        expressions.append(
            F.count(
                F.when(F.col(column_name).isNull(), 1)
            ).alias(f"{column_name}_nulls")
        )

        # Count empty or whitespace-only strings.
        expressions.append(
            F.count(
                F.when(
                    F.col(column_name).isNotNull()
                    & (F.trim(F.col(column_name)) == ""),
                    1
                )
            ).alias(f"{column_name}_blanks")
        )

    return df.agg(*expressions)


def find_duplicate_keys(df, key_columns):
    """
    Finds records where the supplied key columns are repeated.

    A repeated key can cause incorrect joins and double-counting.
    We return the duplicate key values and their occurrence count.
    """

    return (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )


def add_parsed_date(df, source_column, parsed_column):
    """
    Adds a safely parsed date column without changing the
    original source column.

    try_to_timestamp returns NULL for invalid date values
    instead of stopping the entire Spark job.
    """

    return df.withColumn(
        parsed_column,
        F.expr(
            f"try_to_timestamp(trim(`{source_column}`), 'yyyy-MM-dd')"
        ).cast("date")
    )


def add_safe_numeric(df, source_column, parsed_column):
    """
    Converts a source string into a numeric value safely.

    Invalid values such as 'unknown', 'N/A', and 'TBD'
    become NULL in the parsed column.

    The original source column is preserved for investigation.
    """

    return df.withColumn(
        parsed_column,
        F.expr(
            f"try_cast(trim(`{source_column}`) AS DOUBLE)"
        )
    )


def find_invalid_numeric_values(df, source_column, parsed_column):
    """
    Identifies non-empty source values that cannot be
    converted into numbers.

    We do not classify a genuine NULL as a malformed number.
    Missing values and malformed values are separate issues.
    """

    checked_df = add_safe_numeric(
        df,
        source_column,
        parsed_column
    )

    return checked_df.filter(
        F.col(source_column).isNotNull()
        & (F.trim(F.col(source_column)) != "")
        & F.col(parsed_column).isNull()
    )


def find_non_positive_values(df, parsed_column):
    """
    Identifies numeric values that are zero or negative.

    This helper is used only where the business rule requires
    a strictly positive value.
    """

    return df.filter(
        F.col(parsed_column).isNotNull()
        & (F.col(parsed_column) <= 0)
    )






# INDIVIDUAL-FILE VALIDATION

def validate_regional_market(df):
    """
    Validates the regional reference dataset.

    Expected grain: One record per region.
    Primary key: region_id.
    """

    findings = {}

    # Check missing and blank values in all columns.
    findings["null_blank_summary"] = get_null_blank_summary(df)

    # A region should appear only once because this dataset
    # will be used as a reference during joins.
    findings["duplicate_region_ids"] = find_duplicate_keys(
        df,
        ["region_id"]
    )

    # Making charges cannot be negative.
    checked_df = add_safe_numeric(
        df,
        "making_charge_pct",
        "parsed_making_charge_pct"
    )

    findings["invalid_making_charge"] = (
        checked_df.filter(
            F.col("making_charge_pct").isNotNull()
            & (F.trim(F.col("making_charge_pct")) != "")
            & F.col("parsed_making_charge_pct").isNull()
        )
    )

    findings["negative_making_charge"] = (
        checked_df.filter(
            F.col("parsed_making_charge_pct") < 0
        )
    )

    return findings


def validate_gold_daily_prices(df):
    """
    Validates daily gold price records.

    Expected grain: One record per date and market.
    Candidate key: price_date + market.
    """

    findings = {}

    findings["null_blank_summary"] = get_null_blank_summary(df)

    # Normalize the date before checking uniqueness.
    # This ensures 2025/02/24 and 2025-02-24 are treated
    # as the same date.
    checked_df = (
        df
        .withColumn(
            "normalized_price_date",
            F.regexp_replace(
                F.trim(F.col("price_date")),
                "/",
                "-"
            )
        )
        .withColumn(
            "normalized_market",
            F.trim(F.col("market"))
        )
    )

    # Parse the normalized date safely.
    checked_df = add_parsed_date(
        checked_df,
        "normalized_price_date",
        "parsed_price_date"
    )

    findings["invalid_price_dates"] = checked_df.filter(
        F.col("parsed_price_date").isNull()
    )

    # Check whether the candidate key is repeated.
    # We use the parsed date and trimmed market so that
    # formatting differences do not hide duplicate keys.
    findings["duplicate_price_keys"] = (
        checked_df
        .filter(F.col("parsed_price_date").isNotNull())
        .groupBy("parsed_price_date", "normalized_market")
        .count()
        .filter(F.col("count") > 1)
    )

    # Safely parse all numeric price-related columns.
    numeric_columns = {
        "price_per_10g_inr": "parsed_price_inr",
        "usd_inr": "parsed_usd_inr",
        "global_gold_usd_oz": "parsed_global_gold_usd"
    }

    for source_column, parsed_column in numeric_columns.items():

        checked_df = add_safe_numeric(
            checked_df,
            source_column,
            parsed_column
        )

        # Malformed numeric values are different from
        # genuine NULL values, so report them separately.
        findings[f"invalid_{source_column}"] = (
            checked_df.filter(
                F.col(source_column).isNotNull()
                & (F.trim(F.col(source_column)) != "")
                & F.col(parsed_column).isNull()
            )
        )

        # All three price-related numeric fields must
        # be strictly positive when a value is provided.
        findings[f"non_positive_{source_column}"] = (
            find_non_positive_values(
                checked_df,
                parsed_column
            )
        )

    return findings


def validate_gold_transactions(df):
    """
    Validates individual transaction records.

    Expected grain: One record per transaction.
    Primary key: transaction_id.
    """

    findings = {}

    findings["null_blank_summary"] = get_null_blank_summary(df)

    # Repeated transaction IDs need investigation.
    findings["duplicate_transaction_ids"] = find_duplicate_keys(
        df,
        ["transaction_id"]
    )

    # Identify exact duplicate rows separately.
    # This helps distinguish repeated copies from conflicting
    # records that share the same transaction ID.
    findings["exact_duplicate_records"] = (
        df.groupBy(*df.columns)
        .count()
        .filter(F.col("count") > 1)
    )

    # Parse sale dates safely.
    checked_df = add_parsed_date(
        df,
        "sale_date",
        "parsed_sale_date"
    )

    findings["invalid_sale_dates"] = checked_df.filter(
        F.col("parsed_sale_date").isNull()
    )

    # Check transaction ID format.
    findings["invalid_transaction_ids"] = df.filter(
        ~F.col("transaction_id").rlike(r"^GT\d{6}$")
    )

    # Safely parse fields that should contain numbers.
    numeric_columns = {
        "weight_grams": "parsed_weight_grams",
        "quoted_rate_per_10g": "parsed_quoted_rate",
        "making_charge_pct": "parsed_making_charge_pct"
    }

    for source_column, parsed_column in numeric_columns.items():

        checked_df = add_safe_numeric(
            checked_df,
            source_column,
            parsed_column
        )

        findings[f"invalid_{source_column}"] = (
            checked_df.filter(
                F.col(source_column).isNotNull()
                & (F.trim(F.col(source_column)) != "")
                & F.col(parsed_column).isNull()
            )
        )

    # A transaction cannot have zero or negative weight.
    # We flag these records rather than changing their values.
    findings["non_positive_weight"] = (
        find_non_positive_values(
            checked_df,
            "parsed_weight_grams"
        )
    )

    # Validate the transaction ID format for consistency.
    # Purity and status standardization will be handled
    # during transformation, based on our documented rules.

    return findings


def validate_market_events(df):
    """
    Validates regional market event records.

    Expected grain: One record per event.
    Primary key: event_id.
    """

    findings = {}

    findings["null_blank_summary"] = get_null_blank_summary(df)

    # Every event should have a unique event ID.
    findings["duplicate_event_ids"] = find_duplicate_keys(
        df,
        ["event_id"]
    )

    # Check event ID format.
    findings["invalid_event_ids"] = df.filter(
        ~F.col("event_id").rlike(r"^ME\d{5}$")
    )

    # Invalid dates cannot safely participate in date-based
    # comparisons, so we report them for quarantine later.
    checked_df = add_parsed_date(
        df,
        "event_date",
        "parsed_event_date"
    )

    findings["invalid_event_dates"] = checked_df.filter(
        F.col("parsed_event_date").isNull()
    )

    return findings





# CROSS-FILE VALIDATION

def validate_cross_file_relationships(
    regional_df,
    prices_df,
    transactions_df,
    events_df
):
    """
    Checks relationships between the four source datasets.

    These checks help us detect unmatched reference IDs,
    missing price dates, unusable price dates, and unexpected
    row multiplication during joins.
    """

    findings = {}

   
    # REGION REFERENCE VALIDATIOn
    # Build a distinct list of valid region IDs.
    # Duplicate region IDs are checked separately in the
    # individual regional_market validation.
    valid_regions = (
        regional_df
        .select(F.trim(F.col("region_id")).alias("region_id"))
        .distinct()
    )

    # Find transactions whose region ID does not exist
    # in the regional reference dataset.
    findings["transactions_unmatched_regions"] = (
        transactions_df.alias("t")
        .join(
            valid_regions.alias("r"),
            F.trim(F.col("t.region_id")) == F.col("r.region_id"),
            "left_anti"
        )
    )

    # Find events whose region ID does not exist
    # in the regional reference dataset.
    findings["events_unmatched_regions"] = (
        events_df.alias("e")
        .join(
            valid_regions.alias("r"),
            F.trim(F.col("e.region_id")) == F.col("r.region_id"),
            "left_anti"
        )
    )

    
    # REGIONAL JOIN CARDINALITY CHECK
    # A left join should preserve the original row count
    # when the reference table has one row per region.
    # If the joined count increases, the reference table
    # may contain duplicate region IDs.

    transaction_count_before = transactions_df.count()

    transaction_count_after = (
        transactions_df.alias("t")
        .join(
            regional_df.alias("r"),
            F.col("t.region_id") == F.col("r.region_id"),
            "left"
        )
        .count()
    )

    event_count_before = events_df.count()

    event_count_after = (
        events_df.alias("e")
        .join(
            regional_df.alias("r"),
            F.col("e.region_id") == F.col("r.region_id"),
            "left"
        )
        .count()
    )

    findings["regional_join_cardinality"] = (
        transactions_df.sparkSession.createDataFrame(
            [
                (
                    "transactions",
                    transaction_count_before,
                    transaction_count_after
                ),
                (
                    "market_events",
                    event_count_before,
                    event_count_after
                )
            ],
            [
                "dataset",
                "rows_before_join",
                "rows_after_join"
            ]
        )
        .withColumn(
            "row_count_changed",
            F.col("rows_before_join") != F.col("rows_after_join")
        )
    )

   
    # PREPARE NORMALIZED DAILY PRICE DATES
    # Normalize slash-separated dates and whitespace in
    # market names before checking date relationships.
    checked_prices = (
        prices_df
        .withColumn(
            "normalized_price_date",
            F.regexp_replace(
                F.trim(F.col("price_date")),
                "/",
                "-"
            )
        )
        .withColumn(
            "normalized_market",
            F.trim(F.col("market"))
        )
    )

    checked_prices = add_parsed_date(
        checked_prices,
        "normalized_price_date",
        "parsed_price_date"
    )

    checked_prices = add_safe_numeric(
        checked_prices,
        "price_per_10g_inr",
        "parsed_price_inr"
    )

    
    # IDENTIFY CONFLICTING PRICE DATES
    # A date-market combination can have multiple records.
    # Exact duplicates can be deduplicated later, but conflicting
    # prices must not be resolved by arbitrarily choosing one.
    #
    # Count distinct price combinations after date normalization.
    # More than one combination indicates conflicting records.

    conflicting_price_dates = (
        checked_prices
        .filter(
            F.col("parsed_price_date").isNotNull()
            & F.col("normalized_market").isNotNull()
        )
        .groupBy(
            "parsed_price_date",
            "normalized_market"
        )
        .agg(
            F.countDistinct(
                F.struct(
                    "price_per_10g_inr",
                    "usd_inr",
                    "global_gold_usd_oz"
                )
            ).alias("distinct_price_records")
        )
        .filter(F.col("distinct_price_records") > 1)
        .select(
            F.col("parsed_price_date").alias("unusable_date")
        )
        .distinct()
    )

    
    # IDENTIFY INVALID INR PRICE DATES
    # INR prices must be present, numeric, and positive
    # before they can be used for transaction price comparisons.
    invalid_inr_price_dates = (
        checked_prices
        .filter(
            F.col("parsed_price_date").isNotNull()
            & (
                F.col("parsed_price_inr").isNull()
                | (F.col("parsed_price_inr") <= 0)
            )
        )
        .select(
            F.col("parsed_price_date").alias("unusable_date")
        )
        .distinct()
    )

    # Combine dates with conflicting prices and dates with
    # invalid INR prices. These dates cannot be trusted for
    # price comparison, even if a date record exists.
    unusable_price_dates = (
        conflicting_price_dates
        .unionByName(invalid_inr_price_dates)
        .distinct()
    )

    findings["unusable_price_dates"] = unusable_price_dates

   
    # PREPARE USABLE PRICE DATES
    # A date is usable only when:
    # 1. Its date can be parsed.
    # 2. Its INR price is positive and numeric.
    # 3. It does not belong to a conflicting date group.

    usable_price_dates = (
        checked_prices
        .filter(
            F.col("parsed_price_date").isNotNull()
            & F.col("parsed_price_inr").isNotNull()
            & (F.col("parsed_price_inr") > 0)
        )
        .select(
            F.col("parsed_price_date").alias("price_date")
        )
        .distinct()
        .join(
            unusable_price_dates,
            F.col("price_date") == F.col("unusable_date"),
            "left_anti"
        )
        .select("price_date")
    )


    # TRANSACTION DATE VS DAILY PRICE DATE

    checked_transactions = add_parsed_date(
        transactions_df,
        "sale_date",
        "parsed_sale_date"
    )

    # Find valid transaction dates that have no corresponding
    # date in the daily price dataset at all.
    transaction_dates = (
        checked_transactions
        .filter(F.col("parsed_sale_date").isNotNull())
        .select(
            F.col("parsed_sale_date").alias("sale_date")
        )
        .distinct()
    )

    findings["transaction_dates_without_price_record"] = (
        transaction_dates.alias("t")
        .join(
            checked_prices
            .filter(F.col("parsed_price_date").isNotNull())
            .select(
                F.col("parsed_price_date").alias("price_date")
            )
            .distinct()
            .alias("p"),
            F.col("t.sale_date") == F.col("p.price_date"),
            "left_anti"
        )
    )

    # Find transactions whose date exists in the price file
    # but whose price record is unusable.
    findings["transactions_on_unusable_price_dates"] = (
        checked_transactions.alias("t")
        .join(
            unusable_price_dates.alias("p"),
            F.col("t.parsed_sale_date") == F.col("p.unusable_date"),
            "inner"
        )
    )


    # EVENT DATE VS DAILY PRICE DATE

    checked_events = add_parsed_date(
        events_df,
        "event_date",
        "parsed_event_date"
    )

    event_dates = (
        checked_events
        .filter(F.col("parsed_event_date").isNotNull())
        .select(
            F.col("parsed_event_date").alias("event_date")
        )
        .distinct()
    )

    # Find event dates that do not exist in the daily price file.
    findings["event_dates_without_price_record"] = (
        event_dates.alias("e")
        .join(
            checked_prices
            .filter(F.col("parsed_price_date").isNotNull())
            .select(
                F.col("parsed_price_date").alias("price_date")
            )
            .distinct()
            .alias("p"),
            F.col("e.event_date") == F.col("p.price_date"),
            "left_anti"
        )
    )

    # Find events whose dates have unusable INR prices.
    findings["events_on_unusable_price_dates"] = (
        checked_events.alias("e")
        .join(
            unusable_price_dates.alias("p"),
            F.col("e.parsed_event_date") == F.col("p.unusable_date"),
            "inner"
        )
    )

    return findings




# RUN ALL VALIDATIONS

def run_all_validations(dfs):

    print("\nStarting individual-file validation...")

    individual_findings = {
        "regional_market": validate_regional_market(
            dfs["regional_market"]
        ),
        "gold_daily_prices": validate_gold_daily_prices(
            dfs["gold_daily_prices"]
        ),
        "gold_transactions": validate_gold_transactions(
            dfs["gold_transactions"]
        ),
        "market_events": validate_market_events(
            dfs["market_events"]
        )
    }

    print("Individual-file validation completed.")

    print("\nStarting cross-file validation...")

    cross_file_findings = validate_cross_file_relationships(
        regional_df=dfs["regional_market"],
        prices_df=dfs["gold_daily_prices"],
        transactions_df=dfs["gold_transactions"],
        events_df=dfs["market_events"]
    )

    print("Cross-file validation completed.")

    return {
        "individual": individual_findings,
        "cross_file": cross_file_findings
    }