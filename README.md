Trade Data Analysis & Visualization Tool
========================================

Overview
--------
This Python project extracts, processes, stores, and visualizes COVID-19-related trade data sourced from the New Zealand Statistics Department. It provides analytical insights into trade trends across countries, transport modes, commodities, and more.

Features
--------
- Connects to a MySQL database and creates relevant tables.
- Downloads and processes a CSV file from an official government source.
- Cleans and normalizes the dataset for analysis.
- Generates visualizations using matplotlib.
- Stores analysis results in MySQL tables.
- Exports MySQL tables as CSV files.

Technologies Used
-----------------
- Python 3.x
- MySQL
- Pandas
- Matplotlib
- Tkinter (for GUI)
- CSV
- Requests

Project Structure
-----------------
.
├── main.py               -> Main script for extraction, processing, visualization, and storage  
├── data.csv              -> Source dataset downloaded from stats.govt.nz  
├── *.csv                 -> Output files exported from MySQL tables  
└── README.txt            -> Project documentation (this file)

Requirements
------------
Install dependencies with pip:

    pip install pandas matplotlib mysql-connector-python requests

Ensure the following before running:
- MySQL server is running on localhost.
- Database named `data_analysis_ergasia` is already created.
- MySQL user `root` with password `28022002` is available.

Setup Instructions
------------------
1. Clone the repository:

    git clone https://github.com/your-repo/trade-analysis.git
    cd trade-analysis

2. Create the required database in MySQL:

    CREATE DATABASE data_analysis_ergasia;

3. Run the main script:

    python main.py

Key Functions
-------------
Function                    | Purpose
---------------------------|---------------------------------------------------------
gather_data()              | Downloads the dataset from the New Zealand Stats website
preprocess_data(df)        | Cleans and formats the raw dataset
store_data_in_table_X()    | Stores different types of grouped data into MySQL tables
plot_total_*()             | Creates visualizations for specific analytical metrics
extract_table_data_to_csv()| Exports MySQL tables to individual CSV files

Data Insights Captured
----------------------
- Monthly Profits
- Country-wise Profits
- Transport Mode Profits
- Weekday Trends
- Commodity-based Profits
- Top 5 Performing Months/Commodities
- Best Weekday per Commodity

Notes
-----
- Some countries and categories (e.g., "All", "Total (excluding China)") are filtered out during analysis.
- All MySQL tables are dropped and recreated each time the script is run — data is not persistent unless exported.
