import pandas as pd
from datetime import datetime
import numpy as np


# Добавление недостающих столбцов

# Функция для расчета расстояния между двумя точками (Haversine formula)
def haversine(lat1, lon1, lat2, lon2):
    R = 3959  # Радиус Земли в милях
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c

# Чтение данных из CSV файла
df = pd.read_csv('train2.csv')

# Определение базовой ставки и стоимости за милю
base_fare = 2.50
cost_per_mile = 1.50


# Расчет расстояния для каждой поездки
df['distance_miles'] = round(df.apply(lambda row: haversine(row['pickup_latitude'], row['pickup_longitude'],
                                                      row['dropoff_latitude'], row['dropoff_longitude']), axis=1), 2)

# Расчет общей стоимости поездки
df['trip_cost'] = round(base_fare + (cost_per_mile * df['distance_miles']), 2)

# Сохранение обновленного DataFrame обратно в CSV файл
df.to_csv('output_trip_costs.csv', index=False)

print("Обработка завершена. Данные сохранены в 'output_trip_costs.csv'")

#df['fare_amount'] = 0.0  # Добавьте информацию о тарифах, если доступно
#df['extra'] = 0.0  # Добавьте информацию о дополнительных сборах, если доступно
#df['mta_tax'] = 0.0  # Добавьте информацию о налогах, если доступно
#df['tip_amount'] = 0.0  # Добавьте информацию о чаевых, если доступно
#df['tolls_amount'] = 0.0  # Добавьте информацию о сборах за проезд, если доступно
#df['total_amount'] = 0.0  # Добавьте информацию о общей сумме, если доступно


