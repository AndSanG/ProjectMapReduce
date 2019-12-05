import pandas as pd

retail_year_2009 = 'retail0910.csv'
retail_year_2010 = 'retail1011.csv'

# read all data from raw file
df_2009 = pd.read_csv(retail_year_2009, encoding='latin-1', sep=",", header=0)
df_2010 = pd.read_csv(retail_year_2010, encoding='latin-1', sep=",", header=0)

# choose only necessary data columns
df1 = df_2009[['StockCode', 'Quantity', 'Price']]
df2 = df_2010[['StockCode', 'Quantity', 'Price']]

# select the data types for columns
df1 = df1.astype({'StockCode': str, 'Quantity': int, 'Price': float}, errors='ignore')
df2 = df2.astype({'StockCode': str, 'Quantity': int, 'Price': float}, errors='ignore')

print(df1[:20])
# create a csv with selected columns
df1.to_csv('2.2_input_data_2009.csv', header=None, index=False, sep=',')
df2.to_csv('2.2_input_data_2010.csv', header=None, index=False, sep=',')
