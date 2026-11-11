## 1. Project Overview

Gold Market Explorer processes four synthetic datasets related to the gold market to derive meaningful insights into price trends, retail sales, regional performance, and market events.
---


## 2. Dataset Overview

The project uses four input datasets:

| Dataset | Description |
|---|---|
| `regional_market.csv` | Contains regional information, including region names, states, and indicative making charges. |
| `gold_daily_prices.csv` | Contains daily gold price information in INR and USD. |
| `gold_transactions.csv` | Contains retail gold transaction details, including product type, purity, weight, quoted rate, and transaction status. |
| `market_events.csv` | Contains market-related events associated with different regions and dates. |


## 3. Project Structure

```text
gold-market-explorer/
│
├── data/
│   ├── raw/
│   │   └── Original input CSV files
│   │
│   └── processed/
│       └── Curated datasets
│
├── notebooks/
│   └── Data exploration and analysis
│
├── src/
│   ├── __init__.py
│   ├── ingestion.py
│   ├── validation.py
│   ├── transformations.py
│   ├── analytics.py
│   └── main.py
│
├── tests/
│   ├── __init__.py
│   ├── test_ingestion.py
│   └── test_validation.py
│
├── outputs/
│   └── analytics/
│       └── Generated analytical outputs
│
|
│
├── README.md
├── requirements.txt
└── .gitignore
```

### Module Responsibilities

| Module | Responsibility |
|---|---|
| ingestion.py | Reads the raw datasets using explicitly defined schemas |
| validation.py | Performs data quality checks and identifies invalid records |
| transformations.py | Cleans, standardizes, and prepares curated datasets |
| analytics.py | Performs business analytics and exports analytical results |
| main.py | Orchestrates the complete pipeline |

---

## 4. Data Ingestion and Profiling

The raw CSV files are ingested using PySpark DataFrames with explicitly defined schemas.

Initial profiling was performed to understand:

- Dataset dimensions and column structures.
- Data types and null counts.
- Duplicate records and candidate key uniqueness.
- Empty and whitespace-only values.
- Invalid numeric values and date formats.
- Distinct categorical values.
- Referential integrity between datasets.


## 5. Data Quality Validation and Handling Decisions

Data quality checks were performed on all four datasets. Issues were handled based on their impact on data reliability and downstream analysis.

### 5.1 Regional Market

- Validated region IDs, NULL values, and duplicate records.
- Trimmed inconsistent whitespace from region names.
- Converted the invalid negative making charge for RG07 to NULL.
- Retained the region for other regional analyses.

### 5.2 Daily Gold Prices

- Checked duplicate records, conflicting prices, invalid dates, and numeric values.
- Removed excess exact duplicates.
- Quarantined conflicting records and records with invalid INR prices.
- Standardized market and source-label text.
- Retained NULL source labels and did not impute missing prices.

### 5.3 Gold Transactions

- Validated transaction IDs, duplicates, dates, numeric fields, purity, status, and region references.
- Removed excess exact duplicates and quarantined conflicting records.
- Quarantined records with invalid dates or negative weights.
- Converted malformed numeric values to NULL.
- Standardized purity and transaction status.
- Retained transactions with missing purity but excluded them from purity-dependent analysis.
- Flagged unmatched region references and excluded them from region-dependent analysis.
- Did not use regional indicative making charges to fill missing transaction-level charges.

### 5.4 Market Events

- Validated event IDs, dates, and region references.
- Quarantined events with invalid dates.
- Retained unmatched region references with a quality flag.
- Retained events without matching price observations but excluded them from price comparisons where applicable.
- Retained NULL event notes and did not impute missing prices.

### 5.5 Data Quality Summary

| Dataset | Validation and handling |
|---|---|
| Regional market | Standardized text; invalid making charge set to NULL. |
| Daily prices | Removed exact duplicates; quarantined conflicting and invalid price records. |
| Transactions | Removed duplicates; quarantined invalid records; standardized values and flagged unmatched references. |
| Market events | Quarantined invalid dates; flagged unmatched references; excluded unavailable price comparisons. |

---


## 6. Analytics Overview

The analytics module uses curated datasets to analyse gold price trends, retail transactions, regional performance, and market events using PySpark and Spark SQL.

### 6.1 Gold Price Analysis

- Analysed daily and monthly gold price movements, including percentage changes, rolling averages, volatility, and cumulative changes.
- Identified daily price increases/decreases and monthly price highs and lows using window functions.

### 6.2 Retail Transaction Analysis

- Analysed transaction statuses and completed sales by region, product type, and purity.
- Estimated gold value, making charges, and total transaction value using valid transaction inputs.
- Examined monthly sales trends and transaction volumes.

### 6.3 Regional Market Analysis

- Compared regional sales performance, transaction volumes, and completion, cancellation, and return rates.
- Compared actual transaction making charges with regional indicative charges.

### 6.4 Additional Business Analytics

- Analysed product contribution to estimated sales value and monthly product mix.
- Identified high-value transactions using the 95th percentile.
- Compared quoted transaction rates with purity-adjusted daily gold price benchmarks.
- Examined monthly gold prices against sales activity.
- Analysed gold price movements around market events using seven-day pre-event and post-event windows.

**Note:** Market price comparisons use available observations. Event-related price movements indicate associations, not causation.

---


## 7. Spark SQL and DataFrame API Comparison

Implemented three analytical queries using Spark SQL:

- Product sales summary.
- Monthly gold price statistics.
- Regional transaction status.

Created temporary views from the curated DataFrames and executed SQL queries. The same analyses were also implemented using the PySpark DataFrame API.

**Validation:** Used bidirectional `exceptAll()` comparisons to check whether both implementations produced identical results.

All three comparisons passed (`True`).


## 8. Spark Execution Plan Analysis

Inspected the formatted execution plans of regional performance and daily price trend analyses using `explain(mode="formatted")`.

Reviewed Spark operations such as joins, aggregations, window processing, and shuffle boundaries.

---

## 9. Output Generation

Every analytical result is saved in both CSV and Parquet formats.

Output structure:

```text
outputs/
└── analytics/
    ├── price_daily_trends/
    │   ├── csv/
    │   │   └── data.csv
    │   └── parquet/
    │       └── data.parquet
    │
    ├── price_monthly_summary/
    ├── price_movement_rankings/
    ├── price_period_extremes/
    ├── transaction_status_summary/
    ├── regional_transaction_status/
    ├── completed_sales_by_region/
    ├── completed_sales_by_product/
    ├── completed_sales_by_purity/
    ├── transaction_value_summary/
    ├── regional_performance/
    ├── monthly_sales_trends/
    ├── market_event_type_summary/
    ├── market_events_by_month/
    ├── event_price_windows/
    ├── event_category_price_movement/
    ├── product_value_contribution/
    ├── regional_completion_rates/
    ├── high_value_transactions/
    ├── high_value_concentration/
    ├── quoted_vs_market_price_summary/
    ├── making_charge_comparison_summary/
    ├── price_vs_sales_activity/
    ├── monthly_product_mix/
    └── ...
```
## 10. Testing and Validation

The project includes automated tests to verify important ingestion and data-quality validation functionality.

Tests were executed using Python's unittest framework.

```powershell
python -m tests.test_ingestion
python -m tests.test_validation

---

## 11. How to Run the Project

### Step 1: Clone the Repository

```powershell
git clone <repository-url>
```

Navigate into the project:

```powershell
cd gold-market-explorer
```

### Step 2: Create a Virtual Environment

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

### Step 3: Run the Pipeline

From the project root:

```powershell
python -m src.main
```

The pipeline performs ingestion, data preparation, analytical processing, SQL validation, execution-plan inspection, and output generation.

---
## 12. Assumptions and Limitations

- The project uses synthetic gold market and retail transaction datasets.
- Invalid and conflicting records are handled according to explicitly implemented data-quality rules.
- Missing business values are not replaced with arbitrary defaults.
- Regional indicative making charges are reference values and are not used to replace missing transaction-level charges.
- Estimated transaction values are calculated only when the required input values are valid.
- Purity-adjusted market references are comparison benchmarks, not actual fair selling prices.
- Event-related price movements indicate associations rather than causation.
- Monthly sales activity and gold price movements do not independently establish a causal relationship.
- Analytical results depend on the availability and quality of the supplied datasets.
