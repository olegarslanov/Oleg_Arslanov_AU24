import pandas as pd
from ydata_profiling import ProfileReport

# Read(download) parquet data
df = pd.read_parquet("car_prices.parquet")


#1. Missing Values in Key Columns

# Check for None value in main columns
missing_values = df[['make', 'model', 'sellingprice']].isnull().sum()

print("Missing values in key columns:")
print(missing_values)

# Check for all missed values in DataFrame
#total_missing = df.isnull().sum()
#print("Missing values in all columns:")
#print(total_missing)



#2. Negative or Zero Prices

#find rows where sellingprice <= 0
invalid_prices = df[df['sellingprice']<= 0]

# print quantity invalid rows
print(f"! Found {len(invalid_prices)} rows with non-positive selling price.")



#3. Sale Date Formatting Issues

# Try to transform to datetime-format, invalid values get value: NaT
df['saledate_parsed'] = pd.to_datetime(df['saledate'], errors='coerce', utc=True)

# Select rows where the date could not be parsed
invalid_dates = df[df['saledate_parsed'].isnull()]

# Print rows will invalid dates
print(f"! Found {len(invalid_dates)} rows with invalid or unparseable dates in 'saledate'.")






