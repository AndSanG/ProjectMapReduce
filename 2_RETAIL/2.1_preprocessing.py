import pandas as pd

retail_year_2009 = 'retail0910.csv'
retail_year_2010 = 'retail1011.csv'

# read all data from raw file
df_2009 = pd.read_csv(retail_year_2009, encoding='latin-1', sep=",", header=0)
df_2010 = pd.read_csv(retail_year_2010, encoding='latin-1', sep=",", header=0)

# choose only necessary data columns, and drop the rows with missing customer information
# missing rows could be filled using df.fillna(0) but then, it enters the top charts. It shouldn't
df1 = df_2009[['Quantity', 'Price', 'Customer ID']].dropna(0)
df2 = df_2010[['Quantity', 'Price', 'Customer ID']].dropna(0)

# select the data types for columns
df1 = df1.astype({'Quantity': float, 'Price': float, 'Customer ID': int}, errors='ignore')
df2 = df2.astype({'Quantity': float, 'Price': float, 'Customer ID': int}, errors='ignore')

print(df1[:20])
# create a csv with selected columns
df1.to_csv('2.1_input_data_2009.csv', header=None, index=False, sep=',')
df2.to_csv('2.1_input_data_2010.csv', header=None, index=False, sep=',')
