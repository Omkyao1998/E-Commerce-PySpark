# Retail & Warehouse Sales ETL Pipeline (PySpark)

An end-to-end PySpark pipeline that takes raw retail and warehouse sales data, cleans it, and produces 16 ready-to-use report tables: sales trends, top suppliers, best and worst items, month-over-month growth, and a retail vs warehouse channel comparison.

## Business problem

A distributor sells the same products through two channels: its own retail stores and wholesale (warehouse) customers. The sales team needs simple answers to questions like:

- Which suppliers and items bring in most of the sales?
- How are sales moving month to month and year to year?
- Which products sell mainly through retail and which through warehouse?
- Which items are underperforming and could be cut?

This pipeline answers those questions in one run and exports each answer as a CSV that can go straight into Power BI, Tableau or Excel.

## Data

Monthly sales records per item and supplier. The script expects a CSV with these columns:

| Column | Meaning |
|---|---|
| `YEAR`, `MONTH` | Sales period |
| `SUPPLIER` | Supplier name |
| `ITEM CODE`, `ITEM DESCRIPTION`, `ITEM TYPE` | Product details |
| `RETAIL SALES` | Sales through retail stores |
| `RETAIL TRANSFERS` | Stock moved to retail stores |
| `WAREHOUSE SALES` | Sales to wholesale customers |

The data file is not stored in this repo. Place it in a `data/` folder before running.

## How the pipeline works

```
Raw CSV
  │
  ├─ 1. Ingest     read with explicit CSV options, print schema, row count and null report
  ├─ 2. Clean      standardise column names, drop rows missing item/year/month, fill missing sales with 0
  ├─ 3. Transform  add TOTAL_SALES, NET_RETAIL_SALES and YEAR_MONTH; cast sales to double
  ├─ 4. Cache      keep the clean DataFrame in memory for the many aggregations that follow
  ├─ 5. Analyse    metrics, time trends, supplier/item rankings, window functions, channel split
  └─ 6. Export     write each result as a single CSV into output/
```

## What it produces

| Output table | Business question it answers |
|---|---|
| `Overall_Metrics` | Total retail, warehouse and combined sales |
| `Yearly_Sales`, `Monthly_Sales`, `Year_Monthly_Sales` | How do sales change over time? |
| `Month_Growth` | Month-over-month growth (uses `lag`) |
| `Cummulative_Sales` | Running total of sales within each year |
| `Top_Suppliers` | Which suppliers lead, and their market share |
| `Top_Items`, `Bottom_Items` | Best and worst selling products |
| `Top_Items_Per_Year` | Item rankings per year (uses `rank` / `row_number`) |
| `Item_Type_Sales` | Sales by product category |
| `Sales_Comparison`, `Channel_By_Item_Type` | Retail vs warehouse performance |
| `Transfer_Analysis` | Stock transfers vs actual retail sales |
| `Item_Per_Supplier`, `Top_Supplier_Item_Combinations` | Which supplier–product pairs drive revenue |

## Engineering choices

- **Explicit CSV read options** (permissive mode, whitespace trimming, null handling) so messy rows don't crash the job.
- **Caching** the cleaned DataFrame because more than 15 aggregations reuse it.
- **Adaptive Query Execution** enabled and shuffle partitions set to 8, which suits a single-machine run.
- **Window functions** (`rank`, `row_number`, `lag`) for rankings and growth instead of self-joins.
- **Single-file CSV outputs** (`coalesce(1)`) so business users get one file per report.
- **Windows fix:** converts a `JAVA_HOME` path containing spaces (e.g. `C:\Program Files\...`) to its short form, a common reason PySpark fails to start on Windows.

## Tech stack

Python · PySpark (Spark SQL, DataFrame API, window functions)

## How to run

Requirements: Python 3.9+, Java 11 or 17.

```bash
git clone https://github.com/Omkyao1998/E-Commerce-PySpark.git
cd E-Commerce-PySpark
pip install -r requirements.txt

# put your CSV in data/, then:
python E-Commerce_PySpark.py data/Warehouse_and_Retail_Sales.csv
```

Optional settings (environment variables):

| Variable | Default | Purpose |
|---|---|---|
| `JAVA_HOME` | `C:\Program Files\Java\jdk-17` | Location of your JDK |
| `DATA_FILE` | `data/Warehouse_and_Retail_Sales.csv` | Input file (if not passed as an argument) |
| `OUTPUT_PATH` | `output` | Where result CSVs are written |

## What I would improve next

- Split the script into modules (`ingest`, `transform`, `reports`) and add unit tests with `pytest`.
- Write outputs as Parquet or Delta Lake instead of CSV for faster downstream reads.
- Schedule the pipeline with Airflow and add data quality checks (row counts, negative sales).
- Load the outputs into a Power BI dashboard.

## Author

**Omkar Gokhale** — Data Engineer & BI Analyst · [GitHub](https://github.com/Omkyao1998)
