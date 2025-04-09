
import pandas as pd
from ydata_profiling import ProfileReport

# Read parquet file in DataFrame
df = pd.read_parquet("car_prices.parquet")

# Create report about data with ydata-profiling
profile = ProfileReport(df, title="Car Prices Profiling Report")

# Save report to HTML file
profile.to_file("car_prices_report.html")





