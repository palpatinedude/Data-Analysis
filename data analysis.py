import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import requests
import calendar
import tkinter as tk
from tkinter import ttk
import mysql.connector
import csv


###################### SQL #########################

def create_database_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="28022002",
        database = 'data_analysis_ergasia'
    )   
    return connection

db_connection = create_database_connection()
cursor = db_connection.cursor()

create_tables_query = """
    DROP TABLE IF EXISTS total_earnings_per_month;
    CREATE TABLE  total_earnings_per_month (
    id INT NOT NULL AUTO_INCREMENT,
    year INT,
    month INT,
    total_profits FLOAT(18, 4),
    PRIMARY KEY (id)
  );

   DROP TABLE IF EXISTS total_profit_per_country;
   CREATE TABLE  IF NOT EXISTS total_profit_per_country (
            id INT NOT NULL AUTO_INCREMENT,
            country VARCHAR(255),
            year INT,
            total_profits FLOAT(18, 4),
            PRIMARY KEY (id)
   );

   DROP TABLE IF EXISTS total_profit_per_transport_mode;
   CREATE TABLE IF NOT EXISTS total_profit_per_transport_mode (
            id INT NOT NULL AUTO_INCREMENT,
            transport_mode VARCHAR(255),
            year INT,
            total_profits FLOAT(18, 4),
            PRIMARY KEY (id)
   );

   DROP TABLE IF EXISTS total_profit_per_weekday;
   CREATE TABLE IF NOT EXISTS total_profit_per_weekday (
            id INT NOT NULL AUTO_INCREMENT,
            weekday VARCHAR(255),
            month VARCHAR(255),
            year INT,
            total_profits FLOAT(18, 4),
            PRIMARY KEY (id)
   );

   DROP TABLE IF EXISTS total_profit_per_commodity;
   CREATE TABLE IF NOT EXISTS total_profit_per_commodity (
        id INT NOT NULL AUTO_INCREMENT,
        commodity VARCHAR(255),
        year INT,
        total_profits FLOAT(18, 4),
        PRIMARY KEY (id)
    );

   DROP TABLE IF EXISTS  top_months_per_year;
   CREATE TABLE IF NOT EXISTS top_months_per_year (
    id INT NOT NULL AUTO_INCREMENT,
    year INT,
    month INT,
    total_profits FLOAT(18, 4),
    PRIMARY KEY (id)
   );

 DROP TABLE IF EXISTS top_categories_per_country;
 CREATE TABLE IF NOT EXISTS top_categories_per_country (
        id INT NOT NULL AUTO_INCREMENT,
        year INT,
        country VARCHAR(255),
        commodity VARCHAR(255),
        total_profits FLOAT(18, 4),
        PRIMARY KEY (id)
    
);
 DROP TABLE IF  EXISTS top_weekday_per_category;
 CREATE TABLE IF NOT EXISTS top_weekday_per_category (
        id INT NOT NULL AUTO_INCREMENT,
        year INT,
        commodity VARCHAR(255),
        weekday VARCHAR(255),
        total_profits FLOAT(18, 4),
        PRIMARY KEY (id)
    
);

"""
# execute the query to create tables
cursor.execute(create_tables_query)
cursor.close()
db_connection.close()
    



################# EXTRACT CSV FILES #####################
def extract_table_data_to_csv(table_name):
    connection = create_database_connection()
    cursor = connection.cursor()

    # select all rows from the table
    select_query = f"SELECT * FROM `{table_name}`"
    cursor.execute(select_query)
    rows = cursor.fetchall()

    # get the column names 
    column_names = [description[0] for description in cursor.description]

    # write the data to a csv file
    file_name = f"{table_name}.csv"
    with open(file_name, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        writer.writerows(rows)

    cursor.close()
    connection.close()

################# PREPROCESSING #########################

#  gather data from the website
def gather_data():
    url = "https://www.stats.govt.nz/assets/Uploads/Effects-of-COVID-19-on-trade/Effects-of-COVID-19-on-trade-At-15-December-2021-provisional/Download-data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"

    response = requests.get(url)
    with open("data.csv", "wb") as file:
        file.write(response.content)

    df = pd.read_csv("data.csv")
    return df

# function to preprocess the data
def preprocess_data(df):
    repetitive_element = 'East Asia (excluding China)'
    new_name = 'East Asia'
    element = 'United States'
    new_element = 'America'
    element1 = 'European Union (27)'
    new_element1 = 'Europe'
    element2 = 'United Kingdom'
    new_element2 = 'Britain'

    df['Country'] = df['Country'].replace(repetitive_element, new_name)
    df['Country'] = df['Country'].replace(element, new_element)
    df['Country'] = df['Country'].replace(element1, new_element1)
    df['Country'] = df['Country'].replace(element2, new_element2)

    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    df['Year'] = pd.DatetimeIndex(df['Date']).year
    df['Month'] = pd.DatetimeIndex(df['Date']).month
    df_req = df[df['Measure'] == '$']
    return df_req

# function to format the y-axis scale based on the scale
def format_yaxis_scale(max_value):
    if isinstance(max_value, str):
        return 1, '{:.1f}'
    if max_value >= 1e9:
        scale = 1e9
        label_format = '{:.1f}B'
    elif max_value >= 1e6:
        scale = 1e6
        label_format = '{:.1f}M'
    else:
        scale = 1e3
        label_format = '{:.1f}K'
    return scale, label_format


################# FUNCTIONS TO STORE AND PLOT THE DATA  ##############################

def store_data_in_table_1(data1, table_name):
   
    connection = create_database_connection()
    cursor = connection.cursor()
   
    # insert the data into the table
    for year, data in data1.groupby('Year'):
        months = data.index.get_level_values('Month')  # get the months for the current year
        profits = data.values  # get the total profits for the current year
        profits_list = profits.tolist()  # convert the  array to a  list
    
        
        for month, profit in zip(months, profits_list):
            insert_query = f"INSERT INTO {table_name} (year, month, total_profits) VALUES ('{year}', '{month}', {profit})"
            cursor.execute(insert_query)

    connection.commit()
    cursor.close()
    connection.close()
        

def plot_total_monthly_profit(data1):
    for year, data in data1.groupby('Year'):
        months = data.index.get_level_values('Month')  # get the months
        profits = data.values  # get the total profits
        

        max_profit = data.max()
        scale, label_format = format_yaxis_scale(max_profit)

        plt.plot(months, profits, label=year)

    plt.xlabel('Month')
    plt.ylabel('Total Profit')
    plt.title('Total Monthly Profit for Each Year')
    plt.legend()
    plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x/scale)))
    plt.show()



def store_data_in_table_2(data1, table_name):
   
    connection = create_database_connection()
    cursor = connection.cursor()


    for country, data in data1.groupby('Country'):
        years = data.index.get_level_values('Year')  # get the months
        if country not in ['All', 'Total (excluding China)']:
            profits = data.values
            profits_list = profits.tolist()


            for year, profit in zip(years, profits_list):
             insert_query = f"INSERT INTO {table_name} (country,year , total_profits) VALUES ('{country}', '{year}', {profit})"
             cursor.execute(insert_query)

    connection.commit()
    cursor.close()
    connection.close()



def plot_total_profit_per_country(data1):
    for country, data in data1.groupby('Country'):
        if country not in ['All', 'Total (excluding China)']:
            years = data.index.get_level_values('Year')
            profits = data.values

            # determine the y-axis scale and format
            max_profit = profits.max()
            scale, label_format = format_yaxis_scale(max_profit)

            # plot the data for the current country
            plt.figure()
            plt.bar(years, profits)
            plt.xlabel('Year')
            plt.ylabel('Total Profit')
            plt.title(f'Total Profit per Year - {country}')
            plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x/scale)))

            plt.tight_layout()
            plt.show()

def store_data_in_table_3 (data1,table_name) :

    connection = create_database_connection()
    cursor = connection.cursor()
  

    for transport_mode, data in data1.groupby('Transport_Mode'):
        if transport_mode == 'All':
            continue  # skip the iteration for transport mode 'All'
        data = data[~(data.index.get_level_values('Transport_Mode') == 'All')]
        years = data.index.get_level_values('Year')  # get the years
        profits = data.values  # get the total profits
        profits_list = profits.tolist()


        for year, profit in zip(years, profits_list):
            insert_query = f"INSERT INTO {table_name} (transport_mode,year , total_profits) VALUES ('{transport_mode}', '{year}', {profit})"
            cursor.execute(insert_query)

    connection.commit()
    cursor.close()
    connection.close()

def plot_total_profit_per_transport_mode(data1):
    for transport_mode, data in data1.groupby('Transport_Mode'):
        if transport_mode == 'All':
            continue  # skip the iteration for transport mode 'All'

        data = data[~(data.index.get_level_values('Transport_Mode') == 'All')]
        years = data.index.get_level_values('Year')  # get the years
        profits = data.values  # get the total profits

        # determine the y-axis scale and format
        max_profit = profits.max()
        scale, label_format = format_yaxis_scale(max_profit)

        # plot the data for the current transport mode
        plt.figure()
        plt.bar(years, profits)
        plt.xlabel('Year')
        plt.ylabel('Total Profit')
        plt.title(f'Total Profit per Year - {transport_mode}')
        plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x/scale)))

        plt.tight_layout()
        plt.show()

      

def store_data_in_table_4 (data1,table_name) :

    connection = create_database_connection()
    cursor = connection.cursor()
  

    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    for weekday in weekdays:
            for year, data in data1.groupby('Year'):
              data = data[data.index.get_level_values('Weekday') == weekday]
              months = data.index.get_level_values('Month')
              profits = data.values
              profits_list = profits.tolist()


            for month, profit in zip(months, profits_list):
              insert_query = f"INSERT INTO {table_name} (weekday, month, year, total_profits) VALUES ('{weekday}', '{month}', {year}, {profit})"
              cursor.execute(insert_query)

    connection.commit()
    cursor.close()
    connection.close()



def plot_total_profit_per_weekday(data1):
   
  weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
  for weekday in weekdays:
    plt.figure()
    
    for year, data in data1.groupby('Year'):
        data = data[data.index.get_level_values('Weekday') == weekday]
        months = data.index.get_level_values('Month')
        profits = data.values
        
       
        max_profit = profits.max()
        scale, label_format = format_yaxis_scale(max_profit)

       
        plt.plot(months, profits, label=str(year))
    
    plt.xlabel('Month')
    plt.ylabel('Total Profit')
    plt.title(f'Total Profit per Month - {weekday}')
    plt.legend()
    plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x/scale)))

    plt.tight_layout()
    plt.show()

def store_data_in_table_5 (data1,table_name) :

    connection = create_database_connection()
    cursor = connection.cursor()
 

    for commodity, data in data1.groupby('Commodity'):
        if commodity == 'All':
            continue  # skip the iteration for commodity 'All'
        data = data[~(data.index.get_level_values('Commodity') == 'All')]
        years = data.index.get_level_values('Year')  # get the years
        profits = data.values  # get the total profits
        profits_list = profits.tolist()


        for year, profit in zip(years, profits_list):
            query = f"INSERT INTO {table_name} (commodity, year, total_profits) VALUES ('{commodity}', {year}, {profit})"
            cursor.execute(query)
      

    connection.commit()
    cursor.close()
    connection.close()




# do some changes in thr third part of the code

def plot_total_profit_per_commodity(data1):
    for commodity, data in data1.groupby('Commodity'):
        if commodity == 'All':
            continue  # skip the iteration for commodity 'All'

        data = data[~(data.index.get_level_values('Commodity') == 'All')]
        years = data.index.get_level_values('Year')  # get the years
        profits = data.values  # get the total profits

        # determine the y-axis scale and format
        max_profit = profits.max()
        scale, label_format = format_yaxis_scale(max_profit)

        # plot the data for the current commodity
        plt.figure()
        plt.bar(years, profits)
        plt.xlabel('Year')
        plt.ylabel('Total Profit')
        plt.title(f'Total Profit for Commodity - {commodity}')
        plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x/scale)))
        plt.tight_layout()
        plt.show()

def store_data_in_table_6(data1,table_name) :

    connection = create_database_connection()
    cursor = connection.cursor()
   
    
    for year, data in data1.groupby('Year'):
        top_5_months = data.nlargest(5)  # select the top 5 months with highest profit
        months = data.index.get_level_values('Month')  # get the months
        profits = top_5_months.values  # get the profits
        profits_list = profits.tolist()
   

        for month, profit in zip(months, profits_list):
            query = f"INSERT INTO {table_name} (year, month, total_profits) VALUES ({year}, {month}, {profit})"
            cursor.execute(query)

    connection.commit()
    cursor.close()
    connection.close()
 


def plot_top_months_per_year(data1):
    for year, data in data1.groupby('Year'):
     top_5_months = data.nlargest(5)  # Select the top 5 months with highest profit
     months = top_5_months.index.get_level_values('Month')  # get the months
     profits = top_5_months.values  # get the profits
     month_names = [calendar.month_name[month] for month in months]  # map month numbers to names

     # Determine the y-axis scale and format
     max_profit = profits.max()
     scale, label_format = format_yaxis_scale(max_profit)

     # Plot the data for the current year
     plt.figure()
     plt.bar(month_names, profits)
     plt.xlabel('Month')
     plt.ylabel('Profit')
     plt.title(f'Top 5 Months with Highest Profit - {year}')
     plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x/scale)))

     plt.tight_layout()
     plt.show()


def store_data_in_table_7(data1,table_name) :

    connection = create_database_connection()
    cursor = connection.cursor()
    
    for (year, country), data in data1.groupby(['Year', 'Country']):
       if country == 'All' or 'Total (excluding China)':
            continue  # skip the iteration for those countries type
       if  commodity == 'All' : 
            continue # skip the iteration for ALl
       top_commodities = data.nlargest(5)

      # select the top 5 commodities with the highest profit
       profits = top_commodities.values  # get the total profits
       profits_list = profits.tolist()

       if top_commodities.empty:
            continue  # skip the plot if there are no  commodities

       for commodity, profit in zip(commodities, profits_list):
            query = f"INSERT INTO {table_name} (year, country, commodity, total_profits) VALUES ({year}, {country}, {commodity},{profit})"
            cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close() 



def plot_top_commodities_per_country(data1):
    for (year, country), data in data1.groupby(['Year', 'Country']):
      if country in ['All', 'Total (excluding China)']:
            continue  # skip the iteration for those countries

      filtered_data = data[~(data.index.get_level_values('Commodity') == 'All')]
      if filtered_data.empty:
        continue  # skip the iteration if there are no commodities
        
      # calculate the sum of values for each commodity
      top_commodities = filtered_data.nlargest(5)
      commodities = top_commodities.index.get_level_values('Commodity')
      profits = top_commodities.values  # get the total profits
       
    # determine the y-axis scale and format
      max_profit = profits.max()
      scale, label_format = format_yaxis_scale(max_profit)
       
       # plot the data for the current year and country
      plt.figure()
      plt.bar(commodities, profits)
      plt.xlabel('Commodity')
      plt.ylabel('Total Profit')
      plt.title(f'Top Commodities with Highest Profit - {year} ({country})')
      plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x / scale)))
      plt.xticks(rotation=90)
      plt.tight_layout()
      plt.show()



def store_data_in_table_8(data1,table_name) :
    connection = create_database_connection()
    cursor = connection.cursor()

    for (year, commodity), data in data1.groupby(['Year', 'Commodity']):
        
        if commodity == 'All':
          continue  # skip the iteration if there are no commodities

        profits = data.values  # get the total profits
        max_profit_index = profits.argmax()  # get the index of the maximum profit
        max_profit_weekday = data.index[max_profit_index][2]  # get the weekday with maximum profit
        max_profit = profits[max_profit_index]  # get the maximum profit
        
        query = f"INSERT INTO {table_name} (year, commodity, weekday, total_profits) VALUES ({year}, '{commodity}', '{max_profit_weekday}', {max_profit})"
        cursor.execute(query)

    connection.commit()
    cursor.close()
    connection.close()



def plot_top_weekday_per_category(data1):
    for (year, commodity), data in data1.groupby(['Year', 'Commodity']):
        
        if commodity == 'All':
            continue  # skip the iteration if there are no commodities
        profits = data.values  # get the total profits
        max_profit_index = profits.argmax()  # get the index of the maximum profit
        max_profit_weekday = data.index[max_profit_index][2]  # get the weekday with maximum profit
        max_profit = profits[max_profit_index]  # get the maximum profit

        # determine the y-axis scale and format
        max_profit = profits.max()
        scale, label_format = format_yaxis_scale(max_profit)

       # plot the data for the current year and commodity
        plt.bar(max_profit_weekday, max_profit, label=commodity)

        plt.xlabel('Weekday')
        plt.ylabel('Total Profit')
        plt.title(f'Weekday with Highest Profit - {year}')
        plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: label_format.format(x / scale)))
        plt.legend()
        plt.tight_layout()
        plt.show()
     

       
########################## MAIN FUNCTION #############################

# main function
def main():
    df = gather_data()
    df = preprocess_data(df)
   
    # group the filtered data
    total_monthly_profit = df.groupby(['Year', 'Month'])['Value'].sum()
    total_country_profit = df.groupby(['Year', 'Country'])['Value'].sum()
    total_transport_profit_yearly = df.groupby(['Year', 'Transport_Mode'])['Value'].sum()
    total_weekday_profit_monthly_yearly = df.groupby(['Year', 'Month', 'Weekday'])['Value'].sum()
    total_goods_profit_yearly = df.groupby(['Year', 'Commodity'])['Value'].sum()
    top_months_per_year = df.groupby(['Year', 'Month'])['Value'].sum()
    top_categories_per_country = df.groupby(['Year', 'Country', 'Commodity'])['Value'].sum()
    highest_profit_day_per_category = df.groupby(['Year', 'Commodity', 'Weekday'])['Value'].sum()


    store_data_in_table_1(total_monthly_profit, 'total_earnings_per_month')
    store_data_in_table_2(total_country_profit,'total_profit_per_country')
    store_data_in_table_3(total_transport_profit_yearly,'total_profit_per_transport_mode')
    store_data_in_table_4( total_weekday_profit_monthly_yearly,'total_profit_per_weekday')
    store_data_in_table_5(total_goods_profit_yearly,'total_profit_per_commodity')
    store_data_in_table_6(top_months_per_year,'top_months_per_year')
    store_data_in_table_7(top_categories_per_country ,'top_categories_per_country')
    store_data_in_table_8(highest_profit_day_per_category , 'top_weekday_per_category')
    
    table_names = ['total_earnings_per_month', 'total_profit_per_country', 'total_profit_per_transport_mode','total_profit_per_weekday','total_profit_per_commodity','top_months_per_year','top_categories_per_country','top_weekday_per_category']
    for table_name in table_names:
        extract_table_data_to_csv(table_name)

    def menu(): # menu in order to plot the options of the user
        selected_option = option_combobox.get()
        # group the filtered data

       
        if selected_option == 'Total Earnings Per Month Each Year':
            plot_total_monthly_profit(total_monthly_profit)
        elif selected_option == 'Total Profit Per Country Per Year':
            plot_total_profit_per_country(total_country_profit)
        elif selected_option == 'Overall Profit by Means of Transport Per Year':
            plot_total_profit_per_transport_mode(total_transport_profit_yearly)
        elif selected_option == 'Total Profit by Day of the Week Per Month Each Year':
            plot_total_profit_per_weekday(total_weekday_profit_monthly_yearly)
        elif selected_option == 'Total Profit by Category of Goods Per Year':
            plot_total_profit_per_commodity(total_goods_profit_yearly)
        elif selected_option == 'Top 5 Months with Highest Profit Per Year':
            plot_top_months_per_year(top_months_per_year)
        elif selected_option == 'Top 5 Categories of Goods with Largest Profit per Country Every Year':
            plot_top_commodities_per_country(top_categories_per_country)
        elif selected_option == 'Top Weekday with Largest Profit per Commodity Every Year':
            plot_top_weekday_per_category (highest_profit_day_per_category)

   

    window = tk.Tk()
    window.title("Data Analysis")
    window.geometry("300x300")

    # create a label selecting the analysis options
    option_label = ttk.Label(window, text="Welcome User!\nSelect Plot Option:")
    option_label.pack(pady=8)

    option_combobox = ttk.Combobox(window, values=[
        'Total Earnings Per Month Each Year',
        'Total Profit Per Country Per Year',
        'Overall Profit by Means of Transport Per Year',
        'Total Profit by Day of the Week Per Month Each Year',
        'Total Profit by Category of Goods Per Year',
        'Top 5 Months with Highest Profit Per Year',
        'Top 5 Categories of Goods with Largest Profit per Country Every Year',
        'Top Weekday with Largest Profit per Commodity Every Year'
    ])
    option_combobox.pack(pady=8)

    # create a button 
    button = ttk.Button(window, text="Plot", command=menu)
    button.pack(pady=10)

    window.mainloop()

    

main()















