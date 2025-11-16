A complete, production-style PySpark ETL + Analytics Pipeline built for large-scale retail and warehouse sales data. The project covers the full data lifecycle — from raw CSV ingestion to cleaning, transformation, caching, advanced analytics, and exporting summarized results.
This pipeline is designed for anyone who wants a fully working, modular PySpark project that demonstrates real-world ETL, performance optimization, and analytical reporting.

Key Features
Automatic Java Path Fix for Windows
Handles the common “Java path with spaces” issue by converting to Windows short-path notation before starting Spark.
Configurable Spark Session
Optimized Spark setup with adaptive execution, tuned memory, and partition handling for local development.
End-to-End ETL Workflow
Reads CSV using robust schema + whitespace options
Cleans column names, handles nulls, casts types
Builds calculated fields (total sales, net retail sales, year-month)
Caches data for faster downstream operations
Business Metrics & Analysis
Total retail/warehouse/combined sales
Sales distribution summaries
Distinct item, supplier, and item-type counts
Time-Series Sales Insights
Yearly, monthly, and year-month aggregated sales
Month-over-month sales growth
Cumulative sales by year
Supplier & Item Deep-Dive
Top suppliers by sales and market share
Top and bottom performing items
Item-type level sales analytics
Advanced Window-Function Analytics
Ranking items by year
Ranking suppliers by year
Lag/lead-based growth calculations
Retail vs Warehouse Channel Comparison
Channel performance summary
Per item-type channel split
Transfer activity analysis
Export Ready
Writes all result tables to CSV
Organized output folder structure
Coalesced to single-file outputs for easier reporting
