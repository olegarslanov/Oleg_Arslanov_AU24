
import pandas as pd

# Read parquet file in DataFrame
df = pd.read_parquet("car_prices.parquet")

# Save report to HTML file
df.to_csv("car_prices.csv", index=False)

print("File transform is finished. car_prices.csv created")





# Check .parquet and .csv is same

# Read both files
df_parquet = pd.read_parquet("car_prices.parquet")
df_csv = pd.read_csv("car_prices.csv")

# Check data is same
print("Same quantity of rows?", len(df_parquet) == len(df_csv))
print("Same columns?", set(df_parquet.columns) == set(df_csv.columns))



