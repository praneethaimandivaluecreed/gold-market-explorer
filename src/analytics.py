
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ANALYTICS_OUTPUT_PATH = PROJECT_ROOT / "outputs" / "analytics"



# COMMON HELPERS

def completed_transactions(transactions_df):
    """Return completed transactions only."""
    return transactions_df.filter(
        F.lower(F.trim(F.col("transaction_status"))) == "completed"
    )


def add_transaction_value_columns(transactions_df):
    """
    Estimate gold value, making charge and total transaction value.

    Transaction making_charge_pct is stored as a decimal fraction:
        0.08 represents 8%.

    Regional indicative making charges use percentage values:
        8 represents 8%.

    Missing inputs remain NULL.
    """

    df = transactions_df.withColumn(
        "gold_value_eligible",
        F.col("weight_grams").isNotNull()
        & F.col("quoted_rate_per_10g").isNotNull()
        & (F.col("weight_grams") > 0)
        & (F.col("quoted_rate_per_10g") > 0)
    )

    df = df.withColumn(
        "making_charge_eligible",
        F.col("gold_value_eligible")
        & F.col("making_charge_pct").isNotNull()
        & (F.col("making_charge_pct") >= 0)
    )

    # Gold value = weight in grams * rate per 10 grams / 10
    df = df.withColumn(
        "estimated_gold_value",
        F.when(
            F.col("gold_value_eligible"),
            F.col("weight_grams")
            * F.col("quoted_rate_per_10g") / 10
        )
    )

    # 0.08 represents 8%, so multiply directly by the fraction.
    df = df.withColumn(
        "estimated_making_charge",
        F.when(
            F.col("making_charge_eligible"),
            F.col("estimated_gold_value")
            * F.col("making_charge_pct")
        )
    )

    # Keep gold value for the same population eligible for total value.
    df = df.withColumn(
        "gold_value_for_total_eligible",
        F.when(
            F.col("making_charge_eligible"),
            F.col("estimated_gold_value")
        )
    )

    # Estimated total = gold value + making charge.
    df = df.withColumn(
        "estimated_total_value",
        F.when(
            F.col("making_charge_eligible"),
            F.col("gold_value_for_total_eligible")
            + F.col("estimated_making_charge")
        )
    )

    return df


def save_analytics_outputs(analytics_dfs):
    """
    Save each analytical result as CSV and Parquet.

    Pandas is used for exporting small analytical result sets
    to avoid Windows Hadoop filesystem dependencies.
    """

    output_root = (
        Path(__file__).resolve().parents[1]
        / "outputs"
        / "analytics"
    )

    for name, df in analytics_dfs.items():
        print(f"\nSaving analytical output: {name}")

        try:
            pandas_df = df.toPandas()
            output_dir = output_root / name

            # CSV export
            csv_dir = output_dir / "csv"
            csv_dir.mkdir(parents=True, exist_ok=True)

            pandas_df.to_csv(
                csv_dir / "data.csv",
                index=False
            )

            print(f"CSV saved: {csv_dir / 'data.csv'}")

            # Parquet export
            parquet_dir = output_dir / "parquet"
            parquet_dir.mkdir(parents=True, exist_ok=True)

            pandas_df.to_parquet(
                parquet_dir / "data.parquet",
                index=False,
                engine="pyarrow"
            )

            print(f"Parquet saved: {parquet_dir / 'data.parquet'}")

        except Exception as e:
            print(f"Export failed for {name}: {e}")



# 1. CORE: PRICE-SERIES QUALITY AND PRICE MOVEMENT
def analyze_gold_price_trends(prices_df):
    """
    Calculate daily price changes, rolling average
    and cumulative movement.

    Assumes one curated India price record per date.
    """

    window_order = Window.orderBy("price_date")

    rolling_window = (
        Window.orderBy("price_date")
        .rowsBetween(-6, 0)
    )

    cumulative_window = (
        Window.orderBy("price_date")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    df = prices_df.withColumn(
        "previous_price",
        F.lag("price_per_10g_inr").over(window_order)
    )

    df = df.withColumn(
        "daily_price_change",
        F.col("price_per_10g_inr") - F.col("previous_price")
    )

    df = df.withColumn(
        "daily_price_change_pct",
        F.when(
            F.col("previous_price").isNotNull()
            & (F.col("previous_price") != 0),
            F.col("daily_price_change")
            / F.col("previous_price") * 100
        )
    )

    df = df.withColumn(
        "rolling_avg_7_observations",
        F.avg("price_per_10g_inr").over(rolling_window)
    )

    df = df.withColumn(
        "cumulative_price_change",
        F.sum(
            F.coalesce(
                F.col("daily_price_change"),
                F.lit(0.0)
            )
        ).over(cumulative_window)
    )

    return df.orderBy("price_date")


def analyze_monthly_price_summary(price_trends_df):
    """Calculate monthly price highs, lows, averages and volatility."""

    return (
        price_trends_df
        .withColumn(
            "year_month",
            F.date_format("price_date", "yyyy-MM")
        )
        .groupBy("year_month")
        .agg(
            F.count("*").alias("available_price_days"),
            F.avg("price_per_10g_inr").alias("average_price"),
            F.max("price_per_10g_inr").alias("highest_price"),
            F.min("price_per_10g_inr").alias("lowest_price"),
            F.stddev("price_per_10g_inr").alias("price_stddev"),
            F.avg("daily_price_change_pct").alias(
                "average_daily_change_pct"
            ),
            F.stddev("daily_price_change_pct").alias(
                "daily_return_volatility_pct"
            ),
            F.sum("daily_price_change").alias(
                "monthly_net_price_change"
            )
        )
        .orderBy("year_month")
    )


def analyze_price_movement_rankings(price_trends_df):
    """Rank the strongest daily price increases and decreases."""

    increase_window = Window.orderBy(
        F.col("daily_price_change_pct").desc_nulls_last()
    )

    decrease_window = Window.orderBy(
        F.col("daily_price_change_pct").asc_nulls_last()
    )

    return (
        price_trends_df
        .withColumn(
            "increase_rank",
            F.dense_rank().over(increase_window)
        )
        .withColumn(
            "decrease_rank",
            F.dense_rank().over(decrease_window)
        )
        .filter(
            F.col("daily_price_change_pct").isNotNull()
        )
        .select(
            "price_date",
            "price_per_10g_inr",
            "daily_price_change",
            "daily_price_change_pct",
            "increase_rank",
            "decrease_rank"
        )
        .orderBy("price_date")
    )


def analyze_price_period_extremes(price_trends_df):
    """Identify monthly highest and lowest price observations."""

    monthly_window_high = (
        Window.partitionBy("year_month")
        .orderBy(F.col("price_per_10g_inr").desc())
    )

    monthly_window_low = (
        Window.partitionBy("year_month")
        .orderBy(F.col("price_per_10g_inr").asc())
    )

    df = price_trends_df.withColumn(
        "year_month",
        F.date_format("price_date", "yyyy-MM")
    )

    df = df.withColumn(
        "monthly_high_rank",
        F.dense_rank().over(monthly_window_high)
    )

    df = df.withColumn(
        "monthly_low_rank",
        F.dense_rank().over(monthly_window_low)
    )

    return df.filter(
        (F.col("monthly_high_rank") == 1)
        | (F.col("monthly_low_rank") == 1)
    )



# 2. RETAIL TRANSACTION ANALYSIS


def analyze_transaction_status(transactions_df):
    """Summarize overall and regional transaction statuses."""

    overall = (
        transactions_df
        .groupBy("transaction_status")
        .agg(
            F.count("*").alias("transaction_count")
        )
    )

    regional = (
        transactions_df
        .groupBy("region_id", "transaction_status")
        .agg(
            F.count("*").alias("transaction_count")
        )
    )

    return overall, regional


def analyze_completed_sales(transactions_df):
    """Calculate completed sales by region, product and purity."""

    completed = completed_transactions(transactions_df)

    by_region = (
        completed
        .groupBy("region_id")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams"),
            F.avg("weight_grams").alias("average_weight_grams"),
            F.avg("quoted_rate_per_10g").alias(
                "average_quoted_rate"
            )
        )
    )

    by_product = (
        completed
        .groupBy("product_type")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams"),
            F.avg("weight_grams").alias("average_weight_grams"),
            F.avg("quoted_rate_per_10g").alias(
                "average_quoted_rate"
            )
        )
    )

    by_purity = (
        completed
        .groupBy("purity")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams"),
            F.avg("weight_grams").alias("average_weight_grams"),
            F.avg("quoted_rate_per_10g").alias(
                "average_quoted_rate"
            )
        )
    )

    return by_region, by_product, by_purity


def analyze_transaction_value(transactions_df):
    """
    Summarize estimated transaction values.

    Note:
    estimated_gold_value_all_eligible includes every transaction
    eligible for gold valuation.

    estimated_gold_value_for_total_eligible includes only
    transactions that also have a valid making charge.

    These are different populations and should not be compared
    as though they have identical eligibility.
    """

    completed = completed_transactions(transactions_df)
    valued = add_transaction_value_columns(completed)

    summary = (
        valued
        .agg(
            F.count("*").alias("completed_transaction_count"),

            F.sum(
                F.col("gold_value_eligible").cast("int")
            ).alias("gold_value_eligible_count"),

            F.sum(
                F.col("making_charge_eligible").cast("int")
            ).alias("estimated_total_eligible_count"),

            F.sum("estimated_gold_value").alias(
                "estimated_gold_value_all_eligible"
            ),

            F.sum("gold_value_for_total_eligible").alias(
                "estimated_gold_value_for_total_eligible"
            ),

            F.sum("estimated_making_charge").alias(
                "estimated_making_charge"
            ),

            F.sum("estimated_total_value").alias(
                "estimated_total_value"
            )
        )
    )

    return valued, summary



# 3.REGIONAL COMPARISONS

def analyze_regional_performance(transactions_df, regional_df):
    """
    Compare completed transaction activity by region.

    Regional performance uses valid region metadata.
    A region with an invalid indicative making-charge reference
    can still participate in regional sales analysis.
    """

    completed = completed_transactions(transactions_df)

    eligible = completed.filter(
        F.col("data_quality_flag").isNull()
    )

    region_columns = regional_df.select(
        "region_id",
        "region_name",
        "state",
        "making_charge_pct"
    )

    joined = eligible.join(
        region_columns,
        on="region_id",
        how="inner"
    )

    result = (
        joined
        .groupBy(
            "region_id",
            "region_name",
            "state"
        )
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams"),
            F.avg("weight_grams").alias("average_weight_grams"),
            F.avg("quoted_rate_per_10g").alias(
                "average_quoted_rate"
            )
        )
    )

    rank_window = Window.orderBy(
        F.col("completed_transactions").desc()
    )

    return result.withColumn(
        "transaction_volume_rank",
        F.dense_rank().over(rank_window)
    )


# MONTHLY TRANSACTION TRENDS

def analyze_monthly_sales(transactions_df):
    """Summarize monthly completed transaction activity and value."""

    completed = completed_transactions(transactions_df)
    valued = add_transaction_value_columns(completed)

    return (
        valued
        .withColumn(
            "year_month",
            F.date_format("sale_date", "yyyy-MM")
        )
        .groupBy("year_month")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams"),
            F.avg("weight_grams").alias("average_weight_grams"),
            F.sum("estimated_gold_value").alias(
                "estimated_gold_value"
            ),
            F.sum("estimated_total_value").alias(
                "estimated_total_value"
            ),
            F.sum(
                F.col("making_charge_eligible").cast("int")
            ).alias("estimated_total_eligible_count")
        )
        .orderBy("year_month")
    )



# 5. MARKET EVENTS


def analyze_market_event_types(events_df):
    """Summarize event types and monthly event activity."""

    by_type = (
        events_df
        .groupBy("event_type")
        .agg(
            F.count("*").alias("event_count")
        )
        .orderBy(F.col("event_count").desc())
    )

    by_month = (
        events_df
        .withColumn(
            "year_month",
            F.date_format("event_date", "yyyy-MM")
        )
        .groupBy("year_month", "event_type")
        .agg(
            F.count("*").alias("event_count")
        )
        .orderBy("year_month", "event_type")
    )

    return by_type, by_month


def analyze_event_price_windows(events_df, price_trends_df):
    """
    Compare India-wide price movement in the seven calendar days
    before and after each event date. The event date is excluded.

    Missing price dates are not imputed.
    Available observation counts are returned for both windows.
    """

    events = events_df.filter(
        F.col("event_date").isNotNull()
    ).alias("e")

    prices = price_trends_df.select(
        "price_date",
        "price_per_10g_inr"
    ).alias("p")

    pre_join = events.join(
        prices,
        (
            F.col("p.price_date")
            >= F.date_sub(F.col("e.event_date"), 7)
        )
        & (
            F.col("p.price_date")
            <= F.date_sub(F.col("e.event_date"), 1)
        ),
        "left"
    )

    pre_summary = (
        pre_join
        .groupBy(
            F.col("e.event_id").alias("event_id")
        )
        .agg(
            F.count("p.price_date").alias(
                "pre_event_price_days"
            ),
            F.avg("p.price_per_10g_inr").alias(
                "pre_event_avg_price"
            )
        )
    )

    post_join = events.join(
        prices,
        (
            F.col("p.price_date")
            >= F.date_add(F.col("e.event_date"), 1)
        )
        & (
            F.col("p.price_date")
            <= F.date_add(F.col("e.event_date"), 7)
        ),
        "left"
    )

    post_summary = (
        post_join
        .groupBy(
            F.col("e.event_id").alias("event_id")
        )
        .agg(
            F.count("p.price_date").alias(
                "post_event_price_days"
            ),
            F.avg("p.price_per_10g_inr").alias(
                "post_event_avg_price"
            )
        )
    )

    event_windows = (
        events_df
        .select(
            "event_id",
            "event_date",
            "event_type",
            "region_id"
        )
        .join(pre_summary, on="event_id", how="left")
        .join(post_summary, on="event_id", how="left")
        .withColumn(
            "pre_post_price_difference",
            F.col("post_event_avg_price")
            - F.col("pre_event_avg_price")
        )
        .withColumn(
            "pre_post_price_change_pct",
            F.when(
                F.col("pre_event_avg_price") > 0,
                F.col("pre_post_price_difference")
                / F.col("pre_event_avg_price") * 100
            )
        )
    )

    by_event_type = (
        event_windows
        .groupBy("event_type")
        .agg(
            F.count("*").alias("events_analysed"),
            F.avg("pre_post_price_change_pct").alias(
                "average_pre_post_price_change_pct"
            ),
            F.avg("pre_post_price_difference").alias(
                "average_pre_post_price_difference"
            ),
            F.sum("pre_event_price_days").alias(
                "pre_event_observations"
            ),
            F.sum("post_event_price_days").alias(
                "post_event_observations"
            )
        )
    )

    return event_windows, by_event_type



# 6. PRODUCT CONTRIBUTION TO ESTIMATED VALUE

def analyze_product_value_contribution(transactions_df):
    """Calculate each product's share of estimated total sales value."""

    completed = completed_transactions(transactions_df)
    valued = add_transaction_value_columns(completed)

    product_summary = (
        valued
        .groupBy("product_type")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams"),
            F.sum("estimated_gold_value").alias(
                "estimated_gold_value"
            ),
            F.sum("estimated_making_charge").alias(
                "estimated_making_charge"
            ),
            F.sum("estimated_total_value").alias(
                "estimated_total_value"
            ),
            F.sum(
                F.col("making_charge_eligible").cast("int")
            ).alias("eligible_value_transactions")
        )
    )

    total_value = (
        product_summary
        .agg(
            F.sum("estimated_total_value").alias("grand_total")
        )
    )

    return (
        product_summary
        .crossJoin(total_value)
        .withColumn(
            "estimated_value_contribution_pct",
            F.when(
                F.col("grand_total") > 0,
                F.col("estimated_total_value")
                / F.col("grand_total") * 100
            )
        )
        .drop("grand_total")
        .orderBy(
            F.col("estimated_total_value").desc_nulls_last()
        )
    )



# 7.REGIONAL COMPLETION RATES


def analyze_regional_completion_rates(transactions_df, regional_df):
    """
    Completion rate = completed transactions / all transactions.

    Uses regions with a matching curated regional reference.
    """

    valid_regions = regional_df.select(
        "region_id"
    ).distinct()

    region_transactions = transactions_df.join(
        valid_regions,
        on="region_id",
        how="inner"
    )

    summary = (
        region_transactions
        .groupBy("region_id")
        .agg(
            F.count("*").alias("total_transactions"),

            F.sum(
                (F.col("transaction_status") == "completed").cast("int")
            ).alias("completed_transactions"),

            F.sum(
                (F.col("transaction_status") == "cancelled").cast("int")
            ).alias("cancelled_transactions"),

            F.sum(
                (F.col("transaction_status") == "returned").cast("int")
            ).alias("returned_transactions"),

            F.sum(
                (F.col("transaction_status") == "pending").cast("int")
            ).alias("pending_transactions")
        )
    )

    return (
        summary
        .join(
            regional_df.select(
                "region_id",
                "region_name",
                "state"
            ),
            on="region_id",
            how="left"
        )
        .withColumn(
            "completion_rate_pct",
            F.when(
                F.col("total_transactions") > 0,
                F.col("completed_transactions")
                / F.col("total_transactions") * 100
            )
        )
        .withColumn(
            "cancellation_rate_pct",
            F.when(
                F.col("total_transactions") > 0,
                F.col("cancelled_transactions")
                / F.col("total_transactions") * 100
            )
        )
        .withColumn(
            "return_rate_pct",
            F.when(
                F.col("total_transactions") > 0,
                F.col("returned_transactions")
                / F.col("total_transactions") * 100
            )
        )
        .orderBy(F.col("completion_rate_pct").desc())
    )



# 8.HIGH-VALUE TRANSACTION CONCENTRATION

def analyze_high_value_transactions(transactions_df):
    """
    Define high-value transactions as those at or above the
    95th percentile of eligible estimated total transaction values.
    """

    valued = add_transaction_value_columns(
        completed_transactions(transactions_df)
    )

    eligible = valued.filter(
        F.col("estimated_total_value").isNotNull()
    )

    threshold = eligible.agg(
        F.expr(
            "percentile_approx(estimated_total_value, 0.95, 10000)"
        ).alias("p95_threshold")
    )

    high_value = (
        eligible
        .crossJoin(threshold)
        .filter(
            F.col("estimated_total_value")
            >= F.col("p95_threshold")
        )
        .withColumn(
            "high_value_flag",
            F.lit(True)
        )
    )

    concentration = (
        high_value
        .groupBy(
            "region_id",
            "product_type",
            "purity"
        )
        .agg(
            F.count("*").alias(
                "high_value_transaction_count"
            ),
            F.sum("estimated_total_value").alias(
                "high_value_estimated_total"
            ),
            F.avg("estimated_total_value").alias(
                "average_high_value_transaction"
            )
        )
        .orderBy(
            F.col("high_value_estimated_total").desc_nulls_last()
        )
    )

    return threshold, high_value, concentration



# 9.QUOTED RATE VS PURITY-ADJUSTED BENCHMARK

def analyze_quoted_vs_market_price(transactions_df, prices_df):
    """
    Assumption:
    Daily India reference price is treated as a 24K benchmark.

    Purity-adjusted benchmark = daily price * purity / 24.

    This is an approximation for comparison, not an official
    valuation rule.
    """

    completed = completed_transactions(transactions_df)

    purity_factor = (
        F.when(F.col("purity") == "24K", F.lit(1.0))
        .when(F.col("purity") == "22K", F.lit(22.0 / 24.0))
        .when(F.col("purity") == "18K", F.lit(18.0 / 24.0))
    )

    transactions = (
        completed
        .withColumn("purity_factor", purity_factor)
        .filter(
            F.col("sale_date").isNotNull()
            & F.col("quoted_rate_per_10g").isNotNull()
            & F.col("purity_factor").isNotNull()
        )
    )

    price_reference = prices_df.select(
        F.col("price_date").alias("reference_date"),
        F.col("price_per_10g_inr").alias(
            "daily_24k_reference_per_10g"
        )
    )

    joined = transactions.join(
        price_reference,
        transactions.sale_date == price_reference.reference_date,
        "inner"
    )

    result = (
        joined
        .withColumn(
            "purity_adjusted_reference_per_10g",
            F.col("daily_24k_reference_per_10g")
            * F.col("purity_factor")
        )
        .withColumn(
            "quoted_minus_reference",
            F.col("quoted_rate_per_10g")
            - F.col("purity_adjusted_reference_per_10g")
        )
        .withColumn(
            "quoted_vs_reference_pct",
            F.when(
                F.col("purity_adjusted_reference_per_10g") > 0,
                F.col("quoted_minus_reference")
                / F.col("purity_adjusted_reference_per_10g") * 100
            )
        )
    )

    summary = (
        result
        .groupBy("purity")
        .agg(
            F.count("*").alias("matched_transactions"),
            F.avg("quoted_rate_per_10g").alias(
                "average_quoted_rate"
            ),
            F.avg("purity_adjusted_reference_per_10g").alias(
                "average_adjusted_reference"
            ),
            F.avg("quoted_minus_reference").alias(
                "average_rate_difference"
            ),
            F.avg("quoted_vs_reference_pct").alias(
                "average_difference_pct"
            )
        )
    )

    return result, summary



# 10.TRANSACTION VS INDICATIVE MAKING CHARGE


def analyze_making_charge_comparison(transactions_df, regional_df):
    """
    Compare transaction-level and regional indicative making charges.

    Transaction values are decimal fractions:
        0.08 = 8%

    Regional values are percentage values:
        8 = 8%

    Transaction values are multiplied by 100 before comparison.

    Difference is expressed in percentage points.
    """

    completed = completed_transactions(transactions_df)

    eligible = completed.filter(
        F.col("making_charge_pct").isNotNull()
        & (F.col("making_charge_pct") >= 0)
    )

    reference = (
        regional_df
        .select(
            "region_id",
            F.col("making_charge_pct").alias(
                "indicative_making_charge_pct"
            )
        )
        .filter(
            F.col("indicative_making_charge_pct").isNotNull()
            & (F.col("indicative_making_charge_pct") >= 0)
        )
    )

    joined = eligible.join(
        reference,
        on="region_id",
        how="inner"
    )

    compared = (
        joined
        .withColumn(
            "transaction_making_charge_pct",
            F.col("making_charge_pct") * 100
        )
        .withColumn(
            "making_charge_difference_pp",
            F.col("transaction_making_charge_pct")
            - F.col("indicative_making_charge_pct")
        )
    )

    summary = (
        compared
        .groupBy("region_id")
        .agg(
            F.count("*").alias("compared_transactions"),

            F.avg("transaction_making_charge_pct").alias(
                "average_transaction_making_charge_pct"
            ),

            F.avg("indicative_making_charge_pct").alias(
                "indicative_making_charge_pct"
            ),

            F.avg("making_charge_difference_pp").alias(
                "average_difference_percentage_points"
            )
        )
        .orderBy("region_id")
    )

    return compared, summary


# 11. GOLD PRICE VS TRANSACTION ACTIVITY

def analyze_price_vs_sales_activity(transactions_df, prices_df):
    """
    Compare monthly India-wide average gold price with completed
    transaction count and total transaction weight.
    """

    completed = completed_transactions(transactions_df)

    monthly_sales = (
        completed
        .withColumn(
            "year_month",
            F.date_format("sale_date", "yyyy-MM")
        )
        .groupBy("year_month")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams")
        )
    )

    monthly_prices = (
        prices_df
        .withColumn(
            "year_month",
            F.date_format("price_date", "yyyy-MM")
        )
        .groupBy("year_month")
        .agg(
            F.count("*").alias("available_price_days"),
            F.avg("price_per_10g_inr").alias(
                "monthly_average_gold_price"
            )
        )
    )

    return (
        monthly_prices
        .join(
            monthly_sales,
            on="year_month",
            how="full"
        )
        .orderBy("year_month")
    )



# 12.EVENT CATEGORIES VS PRICE MOVEMENT

def analyze_event_category_price_movement(events_df, price_trends_df):
    """Aggregate event-level pre/post price movement by event type."""

    event_windows, event_type_summary = analyze_event_price_windows(
        events_df,
        price_trends_df
    )

    return event_windows, event_type_summary



# 13.PRODUCT MIX AND SEASONAL ACTIVITY

def analyze_monthly_product_mix(transactions_df):
    """Calculate each product's share of completed transactions per month."""

    completed = completed_transactions(transactions_df)

    monthly_product = (
        completed
        .withColumn(
            "year_month",
            F.date_format("sale_date", "yyyy-MM")
        )
        .groupBy("year_month", "product_type")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams")
        )
    )

    month_window = Window.partitionBy("year_month")

    return (
        monthly_product
        .withColumn(
            "monthly_transaction_total",
            F.sum("completed_transactions").over(month_window)
        )
        .withColumn(
            "product_mix_pct",
            F.when(
                F.col("monthly_transaction_total") > 0,
                F.col("completed_transactions")
                / F.col("monthly_transaction_total") * 100
            )
        )
        .orderBy("year_month", "product_type")
    )



# SPARK SQL ANALYSES + DATAFRAME VALIDATION


def run_spark_sql_analytics(spark, curated_dfs):
    """
    Run SQL analyses and compare each with its
    equivalent DataFrame implementation.
    """

    transactions = curated_dfs["gold_transactions"]
    prices = curated_dfs["gold_daily_prices"]
    regional = curated_dfs["regional_market"]

    transactions.createOrReplaceTempView("gold_transactions")
    prices.createOrReplaceTempView("gold_daily_prices")
    regional.createOrReplaceTempView("regional_market")

    # SQL 1: Product sales

    sql_product = spark.sql("""
        SELECT
            product_type,
            COUNT(*) AS completed_transactions,
            SUM(weight_grams) AS total_weight_grams
        FROM gold_transactions
        WHERE LOWER(TRIM(transaction_status)) = 'completed'
        GROUP BY product_type
    """)

    df_product = (
        completed_transactions(transactions)
        .groupBy("product_type")
        .agg(
            F.count("*").alias("completed_transactions"),
            F.sum("weight_grams").alias("total_weight_grams")
        )
    )

    # SQL 2: Monthly price statistics

    sql_monthly_price = spark.sql("""
        SELECT
            DATE_FORMAT(price_date, 'yyyy-MM') AS year_month,
            COUNT(*) AS available_price_days,
            AVG(price_per_10g_inr) AS average_price,
            MAX(price_per_10g_inr) AS highest_price,
            MIN(price_per_10g_inr) AS lowest_price
        FROM gold_daily_prices
        GROUP BY DATE_FORMAT(price_date, 'yyyy-MM')
    """)

    df_monthly_price = (
        prices
        .withColumn(
            "year_month",
            F.date_format("price_date", "yyyy-MM")
        )
        .groupBy("year_month")
        .agg(
            F.count("*").alias("available_price_days"),
            F.avg("price_per_10g_inr").alias("average_price"),
            F.max("price_per_10g_inr").alias("highest_price"),
            F.min("price_per_10g_inr").alias("lowest_price")
        )
    )


    # SQL 3: Regional transaction status

    sql_regional = spark.sql("""
        SELECT
            t.region_id,
            r.region_name,
            COUNT(*) AS total_transactions,
            SUM(
                CASE
                    WHEN LOWER(TRIM(t.transaction_status)) = 'completed'
                    THEN 1 ELSE 0
                END
            ) AS completed_transactions
        FROM gold_transactions t
        INNER JOIN regional_market r
            ON t.region_id = r.region_id
        GROUP BY t.region_id, r.region_name
    """)

    df_regional = (
        transactions
        .join(
            regional.select("region_id", "region_name"),
            on="region_id",
            how="inner"
        )
        .groupBy("region_id", "region_name")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(
                (
                    F.lower(F.trim(F.col("transaction_status")))
                    == "completed"
                ).cast("int")
            ).alias("completed_transactions")
        )
    )

    sql_results = {
        "sql_product_sales": sql_product,
        "sql_monthly_price": sql_monthly_price,
        "sql_regional_status": sql_regional
    }

    dataframe_results = {
        "df_product_sales": df_product,
        "df_monthly_price": df_monthly_price,
        "df_regional_status": df_regional
    }

    comparisons = {
        "product_sales_match": (
            sql_product.exceptAll(df_product).limit(1).count() == 0
            and df_product.exceptAll(sql_product).limit(1).count() == 0
        ),

        "monthly_price_match": (
            sql_monthly_price.exceptAll(df_monthly_price).limit(1).count() == 0
            and df_monthly_price.exceptAll(sql_monthly_price).limit(1).count() == 0
        ),

        "regional_status_match": (
            sql_regional.exceptAll(df_regional).limit(1).count() == 0
            and df_regional.exceptAll(sql_regional).limit(1).count() == 0
        )
    }

    print("\nSQL vs DataFrame validation:")

    for name, matched in comparisons.items():
        print(f"{name}: {matched}")

    return sql_results, comparisons



# EXECUTION PLAN INSPECTION

def inspect_execution_plans(
    regional_performance_df,
    price_trends_df
):
    """Printing formatted execution plans for two analytical operations."""

    print(
        "\n========== REGIONAL PERFORMANCE EXECUTION PLAN =========="
    )

    regional_performance_df.explain(mode="formatted")

    print(
        "\n========== PRICE TRENDS EXECUTION PLAN =========="
    )

    price_trends_df.explain(mode="formatted")



# ORCHESTRATOR

def run_analytics(spark, curated_dfs):

    transactions = curated_dfs["gold_transactions"]
    prices = curated_dfs["gold_daily_prices"]
    regional = curated_dfs["regional_market"]
    events = curated_dfs["market_events"]

    analytics_dfs = {}

   

    print("\n1. Daily gold price trends")

    price_trends = analyze_gold_price_trends(prices)

    analytics_dfs["price_daily_trends"] = price_trends

    print("\n2. Monthly price summary")

    monthly_prices = analyze_monthly_price_summary(price_trends)

    analytics_dfs["price_monthly_summary"] = monthly_prices

    print("\n3. Price movement rankings")

    movement_rankings = analyze_price_movement_rankings(
        price_trends
    )

    analytics_dfs["price_movement_rankings"] = movement_rankings

    print("\n4. Monthly high and low prices")

    price_extremes = analyze_price_period_extremes(price_trends)

    analytics_dfs["price_period_extremes"] = price_extremes

    print("\n5. Transaction status")

    status_overall, status_region = analyze_transaction_status(
        transactions
    )

    analytics_dfs["transaction_status_summary"] = status_overall
    analytics_dfs["regional_transaction_status"] = status_region

    print("\n6. Completed sales by region, product and purity")

    sales_region, sales_product, sales_purity = analyze_completed_sales(
        transactions
    )

    analytics_dfs["completed_sales_by_region"] = sales_region
    analytics_dfs["completed_sales_by_product"] = sales_product
    analytics_dfs["completed_sales_by_purity"] = sales_purity

    print("\n7. Estimated transaction values")

    valued_transactions, value_summary = analyze_transaction_value(
        transactions
    )

    analytics_dfs[
        "completed_transactions_with_estimated_values"
    ] = valued_transactions

    analytics_dfs["transaction_value_summary"] = value_summary

    print("\n8. Regional performance")

    regional_performance = analyze_regional_performance(
        transactions,
        regional
    )

    analytics_dfs["regional_performance"] = regional_performance

    print("\n9. Monthly sales trends")

    monthly_sales = analyze_monthly_sales(transactions)

    analytics_dfs["monthly_sales_trends"] = monthly_sales

    print("\n10. Market event summaries")

    event_types, event_months = analyze_market_event_types(events)

    analytics_dfs["market_event_type_summary"] = event_types
    analytics_dfs["market_events_by_month"] = event_months

    print("\n11. Event price windows")

    event_windows, event_window_summary = analyze_event_price_windows(
        events,
        price_trends
    )

    analytics_dfs["event_price_windows"] = event_windows

    analytics_dfs[
        "event_category_price_movement"
    ] = event_window_summary

    # --------------------------------------------------------
    # ADDITIONAL BUSINESS ANALYSES
    # --------------------------------------------------------

    print("\n12. Product contribution to estimated sales value")

    product_value = analyze_product_value_contribution(
        transactions
    )

    analytics_dfs["product_value_contribution"] = product_value

    print("\n13. Regional transaction completion rates")

    regional_completion = analyze_regional_completion_rates(
        transactions,
        regional
    )

    analytics_dfs["regional_completion_rates"] = regional_completion

    print("\n14. High-value transaction concentration")

    (
        high_value_threshold,
        high_value_transactions,
        high_value_concentration
    ) = analyze_high_value_transactions(transactions)

    analytics_dfs["high_value_threshold"] = high_value_threshold
    analytics_dfs["high_value_transactions"] = high_value_transactions
    analytics_dfs["high_value_concentration"] = high_value_concentration

    print("\n15. Quoted rate vs purity-adjusted market benchmark")

    quoted_market_detail, quoted_market_summary = (
        analyze_quoted_vs_market_price(
            transactions,
            prices
        )
    )

    analytics_dfs[
        "quoted_vs_market_price_detail"
    ] = quoted_market_detail

    analytics_dfs[
        "quoted_vs_market_price_summary"
    ] = quoted_market_summary

    print("\n16. Transaction vs indicative making charges")

    making_charge_detail, making_charge_summary = (
        analyze_making_charge_comparison(
            transactions,
            regional
        )
    )

    analytics_dfs[
        "making_charge_comparison_detail"
    ] = making_charge_detail

    analytics_dfs[
        "making_charge_comparison_summary"
    ] = making_charge_summary

    print("\n17. Gold price vs transaction activity")

    price_sales = analyze_price_vs_sales_activity(
        transactions,
        prices
    )

    analytics_dfs["price_vs_sales_activity"] = price_sales

    print("\n18. Monthly product mix")

    product_mix = analyze_monthly_product_mix(transactions)

    analytics_dfs["monthly_product_mix"] = product_mix

    
    # SPARK SQL ANALYSES

    print("\n19. Spark SQL analyses")

    sql_results, sql_comparisons = run_spark_sql_analytics(
        spark,
        curated_dfs
    )

    analytics_dfs.update(sql_results)

    # EXECUTION PLAN INSPECTION

    print("\n20. Execution plan inspection")

    inspect_execution_plans(
        regional_performance,
        price_trends
    )

    # DISPLAY KEY RESULTS

    display_outputs = [
        ("Monthly price summary", monthly_prices),
        ("Price movement rankings", movement_rankings),
        ("Transaction value summary", value_summary),
        ("Regional performance", regional_performance),
        ("Product value contribution", product_value),
        ("Regional completion rates", regional_completion),
        ("High-value concentration", high_value_concentration),
        ("Quoted vs market benchmark", quoted_market_summary),
        ("Making-charge comparison", making_charge_summary),
        ("Price vs sales activity", price_sales),
        ("Event category price movement", event_window_summary),
        ("Monthly product mix", product_mix)
    ]

    for title, df in display_outputs:
        print(f"\n========== {title.upper()} ==========")
        df.show(30, truncate=False)


    # SAVE ANALYTICAL OUTPUTS

    print("\n21. Saving analytical CSV and Parquet outputs")

    save_analytics_outputs(analytics_dfs)

    print("\nAnalytics pipeline completed.")

    return analytics_dfs, sql_comparisons
