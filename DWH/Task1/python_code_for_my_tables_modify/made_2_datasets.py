import pandas as pd
import numpy as np

# Чтение данных из первой системы источников
yellow_taxi_data = pd.read_csv('train2.csv')

# Создание копии для второй системы источников
green_taxi_data = yellow_taxi_data.copy()

# Изменение атрибутов и добавление новых столбцов

green_taxi_data['vendor_id'] += 1  # Уникальные идентификаторы для второй системы
green_taxi_data['customer_type'] = np.random.choice(['Individual', 'Business'], size=len(green_taxi_data))
green_taxi_data['promo_code'] = np.random.choice(["Promo10", "Promo20", "None"], size=len(green_taxi_data))
green_taxi_data.drop(columns=['store_and_fwd_flag'], inplace=True)

# Сохранение двух систем источников в CSV
yellow_taxi_data.to_csv('yellow_taxi_data.csv', index=False)
green_taxi_data.to_csv('green_taxi_data.csv', index=False)

print("Данные двух систем источников созданы и сохранены в CSV файлы")
