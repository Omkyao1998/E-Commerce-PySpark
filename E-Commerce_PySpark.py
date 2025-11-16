import os
import subprocess
import sys
import pyspark

print(f"Python version: {sys.version}")
print(f"PySpark version: {pyspark.__version__}")
# Get short path name for Java (fixes space issue)
def get_short_path(long_path):
    """Convert long path to short path (removes spaces)"""
    try:
        import ctypes
        from ctypes import wintypes
        _GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
        _GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        _GetShortPathNameW.restype = wintypes.DWORD
        
        output_buf_size = 0
        while True:
            output_buf = ctypes.create_unicode_buffer(output_buf_size)
            needed = _GetShortPathNameW(long_path, output_buf, output_buf_size)
            if output_buf_size >= needed:
                return output_buf.value
            else:
                output_buf_size = needed
    except:
        return long_path

# Set Java path - CRITICAL: Must be BEFORE PySpark imports
java_home = r"JAVA Home path"

if os.path.exists(java_home):
    # Convert to short path (C:\PROGRA~1\Java\jdk-17)
    java_home_short = get_short_path(java_home)
    os.environ['JAVA_HOME'] = java_home_short
    os.environ['PATH'] = os.path.join(java_home_short, 'bin') + os.pathsep + os.environ.get('PATH', '')
    print(f" Using Java from: {java_home_short}")
else:
    print(f" Java not found at: {java_home}")
    print("Please update the java_home path in the script")

# NOW import PySpark (after setting JAVA_HOME)
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    round as spark_round, when, lit, concat_ws, countDistinct,
    row_number, rank, dense_rank, lag, lead,
    collect_list, collect_set, explode, trim, upper
)
from pyspark.sql.types import *

# Rest of your code...
#importing necessary libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, max as spark_max, min as spark_min,
    round as spark_round, when, lit, datediff, current_date, to_date,
    year, month, dayofmonth, hour, date_format, concat_ws,
    row_number, rank, dense_rank, lag, lead,
    explode, split, array_contains, size, collect_list, collect_set,
    udf, pandas_udf, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType, ArrayType
)
import random
from datetime import datetime, timedelta

# Function to create and configure Spark Session
def create_spark_session():
    spark = SparkSession.builder \
        .appName("ECommerce_ETL_Pipeline")\
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("Warn")

    print("="*80)
    print("Spark Session Created Successfully")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    print("="*80)

    return spark

# Configuration dictionary
CONFIG = {
    'data_file':'Data File Path',

    'csv_options':{
        'header': True,
        'inferSchema': True,
        'sep': ',',
        'quote':'"',
        'escape':'\\',
        'mode':'PERMISSIVE',
        'nullValue': '',
        'emptyValue': '',
        'nanValue': '',
        'ignoreLeadingWhiteSpace': True,
        'ignoreTrailingWhiteSpace': True,
        'dateFormat': 'yyyy-MM-dd',
    },
}

#Loading data and exploring it

def load_and_explore_data(spark, config):
    try:
        # Load data
        df = spark.read.csv(config['data_file'], **config['csv_options'])
        
        if df is None:
            print("Error: DataFrame is None after loading data.")
            return None
        
        # Show schema
        df.printSchema()
        
        # Show sample data
        df.show(5)
        
        # Count total records
        total_records = df.count()
        print(f"Total Records: {total_records}")
        
        #Check for null values
        null_counts = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_counts.append((column, null_count))
        
        null_df = spark.createDataFrame(null_counts, ["Column", "NullCount"])
        null_df.show(truncate=False)
        
        return df
    
    except Exception as e:
        print(f"\nError loading or exploring data: {e}")

# Cleaning and Transforming data
def clean_and_transform_data(df):
    
    if df is None:
        print("Error:DataFrame is None.")
        return None
    try:
        for col_name in df.columns:
            new_name = col_name.strip().replace(" ", "_").upper()
            df = df.withColumnRenamed(col_name, new_name)

        #Remove rows with null critical values
        df_clean = df.filter(
            col('ITEM_CODE').isNotNull() &
            col('YEAR').isNotNull() &
            col('MONTH').isNotNull()
        )

        #Fill nulls in sales columns with 0
        sales_columns = ['RETAIL_SALES','RETAIL_TRANSFERS','WAREHOUSE_SALES']
        for col_name in sales_columns:
            if col_name in df_clean.columns:
                df_clean = df_clean.withColumn(
                    col_name,
                    when(col(col_name).isNull(), 0).otherwise(col(col_name))
                )

        #Add Calculated Columns
        df_clean = df_clean \
            .withColumn(
                'TOTAL_SALES',
                col('RETAIL_SALES') + col('WAREHOUSE_SALES')
            ) \
            .withColumn('YEAR_MONTH',
                concat_ws('-', col('YEAR'), col('MONTH'))) \
            .withColumn('NET_RETAIL_SALES',
                col('RETAIL_SALES') - col('RETAIL_TRANSFERS'))
        
        #Check on numerical Columns
        numerical_columns = ['RETAIL_SALES','RETAIL_TRANSFERS','WAREHOUSE_SALES','TOTAL_SALES','NET_RETAIL_SALES']
        for col_name in numerical_columns:
            if col_name in df_clean.columns:
                df_clean = df_clean.withColumn(
                    col_name,col(col_name).cast('double'))
                
        return df_clean
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return None

#OVERALL BUSINESS METRICS
def calculated_overall_metrics(df):
    print("\n" + "="*80)
    print("Overall Business Metrics:")
    print("="*80)

    overall = df.agg(
        spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
        spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
        spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
        spark_round(spark_sum('RETAIL_TRANSFERS'),2).alias('Total_Retail_Transfers'),
        countDistinct('ITEM_CODE').alias('Distinct_Items_Sold'),
        countDistinct('SUPPLIER').alias('Distinct_Suppliers'),
        countDistinct('ITEM_TYPE').alias('Distinct_Item_Types'),
        count('*').alias('Total_Records')
    )
    overall.show(truncate=False)

    #Sales Distribution
    print("\nSales Distribution:")
    sales_distribution = df.agg(
        spark_round(avg('RETAIL_SALES'),2).alias('Avg_Retail_Sales'),
        spark_round(spark_max('RETAIL_SALES'),2).alias('Max_Retail_Sales'),
        spark_round(spark_min('RETAIL_SALES'),2).alias('Min_Retail_Sales'),
        spark_round(avg('WAREHOUSE_SALES'),2).alias('Avg_Warehouse_Sales'),
        spark_round(spark_max('WAREHOUSE_SALES'),2).alias('Max_Warehouse_Sales'),
        spark_round(spark_min('WAREHOUSE_SALES'),2).alias('Min_Warehouse')
    )
    sales_distribution.show(truncate=False)

    return overall

# SALES BY TIME PERIOD
def sales_by_time_period(df):
    print("\n" + "="*80)
    print("Sales by Time Period:")
    print("="*80)

    #Yearly Sales
    print("\nYearly Sales:")
    yearly_sales = df.groupBy('YEAR')\
    .agg(
        spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
        spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
        spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
        countDistinct('ITEM_CODE').alias('Distinct_Items_Sold'),
        count('*').alias('Total_Records')
    )\
    .orderBy('YEAR')
    yearly_sales.show()

    #Monthly Sales
    print("\nMonthly Sales:")
    monthly_sales = df.groupBy('MONTH')\
    .agg(
        spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
        spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
        spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
        countDistinct('ITEM_CODE').alias('Distinct_Items_Sold'),
        count('*').alias('Total_Records')
    )\
    .orderBy('MONTH')
    monthly_sales.show()

    #Year-Monthly Sales
    print("\nYear-Monthly Sales:")
    year_monthly_sales = df.groupBy('YEAR_MONTH','YEAR','MONTH')\
    .agg(
        spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
        spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
        spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
        countDistinct('ITEM_CODE').alias('Distinct_Items_Sold'),
        count('*').alias('Total_Records')
    )
    year_monthly_sales.show(24) #2 Years data

    return yearly_sales, monthly_sales, year_monthly_sales

# SUPPLIER ANALYSIS
def supplier_analysis(df):
    print("\n" + "="*80)
    print("Supplier Analysis:")
    print("="*80)

    #Top Suppliers by Total Sales
    print("\nTop Suppliers by Total Sales:")
    top_suppliers = df.groupBy('SUPPLIER')\
    .agg(
        spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
        spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
        spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
        countDistinct('ITEM_CODE').alias('Distinct_Items_Sold'),
        count('*').alias('Total_Records')
    )\
    .orderBy(col('Total_Sales').desc())\
    .limit(20)
    top_suppliers.show(truncate=False)

    print("\n Supplier Market Share")
    total_sales = df.agg(spark_sum('TOTAL_SALES').alias('Overall_Sales')).collect()[0]['Overall_Sales']
    supplier_share = df.groupby('SUPPLIER')\
        .agg(spark_round(spark_sum('TOTAL_SALES'),2).alias('Supplier_Sales'))\
        .withColumn('Market_Share_Percent', spark_round((col('Supplier_Sales') / lit(total_sales)) * 100, 2))\
        .orderBy(col('Market_Share_Percent').desc())\
        .limit(20)
    supplier_share.show(truncate=False)

    return top_suppliers

# Product/Item Analysis
def item_analysis(df):
    print("\n" + "="*80)
    print("Product/Item Analysis:")
    print("="*80)

    #Top Items by Total Sales
    print("\nTop Items by Total Sales:")
    top_items = df.groupBy('ITEM_CODE','ITEM_DESCRIPTION','ITEM_TYPE','SUPPLIER')\
        .agg(
            spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
            spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
            spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
            spark_round(avg('RETAIL_SALES'),2).alias('Avg_Retail_Sales')
        ) \
        .orderBy(col('Total_Sales').desc())\
        .limit(20)
    
    top_items.show(truncate=False)

    #Item Type Analysis
    print("\nItem Type Analysis")
    item_type_sales = df.groupBy('ITEM_TYPE')\
        .agg(
            spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
            spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
            spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
            countDistinct('ITEM_CODE').alias('Distinct_Items'),
            countDistinct('SUPPLIER').alias('Distinct_Suppliers')
        )\
        .orderBy(col('Total_Sales').desc())
    item_type_sales.show(truncate=False)

    #Bottom Performers
    print("\nBottom 10 Items by Total Sales:")
    bottom_items = df.groupBy('ITEM_CODE','ITEM_DESCRIPTION','ITEM_TYPE','SUPPLIER')\
        .agg( 
            spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales')
        )\
        .orderBy(col('Total_Sales').asc())\
        .limit(10)
    
    bottom_items.show(truncate=False)

    return top_items, item_type_sales, bottom_items

#Advance Analytics with Window Functions
def advanced_analytics(df):
    print("\n" + "="*80)
    print("Advanced Analytics with Window Functions:")
    print("="*80)

    window_spec = Window.partitionBy('YEAR').orderBy(col('TOTAL_SALES').desc())

    df_window = df.withColumn(
        'Sales_Rank',
        rank().over(window_spec)
    ).withColumn(
        'Sales_Dense_Rank',
        dense_rank().over(window_spec)
    ).withColumn(
        'Sales_Row_Number',
        row_number().over(window_spec)
    )

    print("\nTop 10 Items per Year by Total Sales with Ranks:")
    top_items_per_year = df_window.filter(col('Sales_Rank') <= 10)

    print("\n Top 5 Supplier per year (Ranked)")
    supplier_window = Window.partitionBy('YEAR').orderBy(col('TOTAL_SALES').desc())
    top_suppliers_per_year = df.groupby('YEAR','SUPPLIER')\
        .agg(spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'))\
        .withColumn('Supplier_Rank', rank().over(supplier_window))\
        .filter(col('Rank')<=5)\
        .orderBy('YEAR','Supplier_Rank')
    
    top_suppliers_per_year.show(25,truncate=False)

    print("\n Month-Over-Month Sales Growth")
    time_window = Window.orderBy('YEAR','MONTH')
    month_growth = df.groupby('YEAR','MONTH','YEAR_MONTH')\
        .agg(spark_round(spark_sum('TOTAL_SALES'),2).alias('Monthly_Sales'))\
        .withColumn('Prev_Month_Sales', lag('Monthly_Sales').over(time_window))\
        .withColumn('MoM_Growth',
                    spark_round(
                        ((col('Monthly_Sales') - col('Prev_Month_Sales')) / col('Prev_Month_Sales')) * 100,2
                    ))\
        .orderBy('YEAR','MONTH')
    month_growth.show(24)

    print("\nCummulative Sales By Year")
    cummulative_window = Window.partitionBy('YEAR').orderBy('MONTH').rowsBetween(Window.unboundedPreceding, 0)

    cummulative_sales = df.groupby('YEAR','MONTH')\
        .agg(spark_round(spark_sum('TOTAL_SALES'),2).alias('Monthly_Sales'))\
        .withColumn('Cummulative_Sales',spark_sum('Monthly_Sales').over(cummulative_window))\
        .orderBy('YEAR','MONTH')

    cummulative_sales.show(24)   

    return top_items_per_year, month_growth, cummulative_sales

#RETAIL VS WAREHOUSE SALES ANALYSIS
def retail_vs_warehouse_analysis(df):
    print("\n" + "="*80)
    print("Retail vs Warehouse Sales Analysis:")
    print("="*80)

    #Overall Sales Comparison
    print("\nSales Comparison between Retail and Warehouse:")
    sales_comparison = df.agg(
        spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
        spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales'),
        spark_round(spark_sum('RETAIL_TRANSFERS'),2).alias('Total_Transfers')
    ).withColumn('Retail_Percentage',
        spark_round((col('Total_Retail_Sales') / (col('Total_Retail_Sales') + col('Total_Warehouse_Sales'))) * 100,2)
    )
    sales_comparison.show(truncate=False)

    #By Item Type
    print("\nSales Comparison by Item Type:")
    channel_by_item_type = df.groupBy('ITEM_TYPE')\
        .agg(
            spark_round(spark_sum('RETAIL_SALES'),2).alias('Total_Retail_Sales'),
            spark_round(spark_sum('WAREHOUSE_SALES'),2).alias('Total_Warehouse_Sales')
        )\
        .withColumn('Retail_Percentage', 
            spark_round((col('Total_Retail_Sales') / (col('Total_Retail_Sales') + col('Total_Warehouse_SALES'))) * 100,2)
        ).orderBy(col('Retail_Sales').desc())
    
    channel_by_item_type.show(truncate=False)

    #Transfer Analysis
    print("\nRetail Transfer Analysis:")
    transfer_analysis = df.filter(col('RETAIL_TRANSFERS') > 0)\
        .groupBy('ITEM_TYPE')\
        .agg(
            spark_round(spark_sum('RETAIL_TRANSFERS'),2).alias('Total_Retail_Transfers'),
            count('*').alias('Transfer_Records'),
            spark_round(avg('RETAIL_TRANSFERS'),2).alias('Avg_Transfers_Per_Record')
        ).orderBy(col('Total_Retail_Transfers').desc())
    
    transfer_analysis.show(truncate=False)

    return sales_comparison, channel_by_item_type, transfer_analysis

#Supplier-Item Combination Analysis
def supplier_item_combination_analysis(df):
    print("\n" + "="*80)
    print("Supplier-Item Combination Analysis:")
    print("="*80)

    #Item per supplier
    print("\nNumber of Distinct Items and Item Types per Supplier:")
    item_per_supplier = df.groupby('SUPPLIER')\
        .agg(
            countDistinct('ITEM_CODE').alias('Distinct_Items_Supplied'),
            countDistinct('ITEM_TYPE').alias('Distinct_Item_Types'),
            spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales_By_Supplier')
        ).orderBy(col('Distinct_Items_Supplied').desc())
    item_per_supplier.show(20,truncate=False)

    #Top Supplier-Item Combinations
    print("\nTop Supplier-Item Combinations by Total Sales:")
    top_supplier_item_combinations = df.groupby('SUPPLIER','ITEM_TYPE')\
        .agg(
            spark_round(spark_sum('TOTAL_SALES'),2).alias('Total_Sales'),
            countDistinct('ITEM_CODE').alias('Distinct_Items'),
            spark_round(avg('TOTAL_SALES'),2).alias('Avg_Sales_Per_Item')
        ).orderBy(col('Total_Sales').desc())\
        .limit(20)
    
    top_supplier_item_combinations.show(truncate=False)

    return item_per_supplier, top_supplier_item_combinations

def cache_dataframe(df,name):
    print(f"\nCaching DataFrame: {name} for better performance..")
    try:
        df_cached = df.cache()
        count = df_cached.count()
        partitions = df_cached.rdd.getNumPartitions()
        print(f"DataFrame '{name}' cached with {count} records across {partitions} partitions.")
        return df_cached
    except Exception as e:
        print("Error caching :{e}")
        return df

def export_results(df,output_path,name):
    try:
        df.coalesce(1)\
        .write.mode('overwrite')\
        .option('header',True)\
        .csv(f"{output_path}/{name}")
      
    except Exception as e:
        print(f"\nError exporting results for {name}: {e}")


#Main Pipeline
def main():
    spark = create_spark_session()
    config = CONFIG

    try:

        #Load and Explore Data
        df_raw = load_and_explore_data(spark, config)

        if df_raw is None:
            print("Data Loading Failed.")
            return

        #Clean and Transform Data
        df_clean = clean_and_transform_data(df_raw)
        if df_clean is None:
            print("Data Cleaning Failed.")
            return

        #Cache cleaned DataFrame
        df_cached = cache_dataframe(df_clean, "Cleaned_Data")

        #Calculate Overall Metrics
        overall_metrics = calculated_overall_metrics(df_cached)

        #Sales by Time Period
        yearly_sales, monthly_sales, year_monthly_sales = sales_by_time_period(df_cached)

        #Supplier Analysis
        top_suppliers = supplier_analysis(df_cached)

        #Item Analysis
        top_items, item_type_sales, bottom_items = item_analysis(df_cached)

        #Advanced Analytics
        top_items_per_year, month_growth, cummulative_sales = advanced_analytics(df_cached)

        #Retail vs Warehouse Analysis
        sales_comparison, channel_by_item_type, transfer_analysis = retail_vs_warehouse_analysis(df_cached)

        #Supplier-Item Combination Analysis
        item_per_supplier, top_supplier_item_combinations = supplier_item_combination_analysis(df_cached)

        #Get Key Metrics for summary
        total_sales = df_clean.agg(spark_sum('TOTAL_SALES').alias('Overall_Sales')).collect()[0][0]
        total_items = df_clean.select(countDistinct('ITEM_CODE')).collect()[0][0]
        total_suppliers = df_clean.select(countDistinct('SUPPLIER')).collect()[0][0]

        print(f"\n Total Sales: {total_sales}")
        print(f" Unique Items Sold: {total_items}")
        print(f" Unique Suppliers: {total_suppliers}")

        #Export Results (Specify output path)
        output_path = "Output Path"
        export_results(overall_metrics, output_path, "Overall_Metrics")
        export_results(yearly_sales, output_path, "Yearly_Sales")
        export_results(monthly_sales, output_path, "Monthly_Sales")
        export_results(year_monthly_sales, output_path, "Year_Monthly_Sales")
        export_results(top_suppliers, output_path, "Top_Suppliers")
        export_results(top_items, output_path, "Top_Items")
        export_results(item_type_sales, output_path, "Item_Type_Sales")
        export_results(bottom_items, output_path, "Bottom_Items")
        export_results(top_items_per_year, output_path, "Top_Items_Per_Year")
        export_results(month_growth, output_path, "Month_Growth")
        export_results(cummulative_sales, output_path, "Cummulative_Sales")
        export_results(sales_comparison, output_path, "Sales_Comparison")
        export_results(channel_by_item_type, output_path, "Channel_By_Item_Type")
        export_results(transfer_analysis, output_path, "Transfer_Analysis")
        export_results(item_per_supplier, output_path, "Item_Per_Supplier")
        export_results(top_supplier_item_combinations, output_path, "Top_Supplier_Item_Combinations")

    except Exception as e:
        print(f"\nError in ETL Pipeline: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        print("\nSpark Session Stopped. ETL Pipeline Completed.")

if __name__ == "__main__":
    main()