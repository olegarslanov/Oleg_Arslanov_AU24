import pandas as pd
#from datetime import datetime

# Загрузка данных
df = pd.read_csv('train2.csv')

# Преобразование столбцов в формат datetime
df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])


df['pickup_day_week'] = df['pickup_datetime'].dt.day_name()
df['pickup_day'] = df['pickup_datetime'].dt.day
df['pickup_month'] = df['pickup_datetime'].dt.month
df['pickup_year'] = df['pickup_datetime'].dt.year
df['pickup_hour'] = df['pickup_datetime'].dt.hour
df['pickup_minute'] = df['pickup_datetime'].dt.minute

df['dropoff_day_week'] = df['dropoff_datetime'].dt.day_name()
df['dropoff_day'] = df['pickup_datetime'].dt.day
df['dropoff_month'] = df['dropoff_datetime'].dt.month
df['dropoff_year'] = df['dropoff_datetime'].dt.year
df['dropoff_hour'] = df['dropoff_datetime'].dt.hour
df['dropoff_minute'] = df['dropoff_datetime'].dt.minute


# Сохранение скорректированного набора данных
df.to_csv('output_dat_time.csv', index=False)
