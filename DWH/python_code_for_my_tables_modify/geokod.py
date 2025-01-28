import pandas as pd
import requests

# Твой API ключ Google Maps
API_KEY = 'AIzaSyBm0DT-mX42fM3_LkLJOUlaQrhD5fRPCjY'

# Функция для выполнения обратного геокодирования
def get_neighborhood(lat, lon, api_key):
    url = f'https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={api_key}'
    response = requests.get(url)
    data = response.json()

    if data['status'] == 'OK':
        for component in data['results'][0]['address_components']:
            if 'neighborhood' in component['types']:
                return component['long_name']
    return 'Ne naideno'


# Чтение данных из CSV файла
df = pd.read_csv('train_podacha_python_geolokacija.csv')

# Применение функции к каждой строке и добавление новых столбцов
df['pickup_neighborhood'] = df.apply(lambda row: get_neighborhood(row['pickup_latitude'], row['pickup_longitude'], API_KEY),
                                     axis=1)
df['dropoff_neighborhood'] = df.apply(lambda row: get_neighborhood(row['dropoff_latitude'], row['dropoff_longitude'], API_KEY),
                                      axis=1)

# Сохранение обновленного DataFrame обратно в CSV файл
df.to_csv('data_with_neighborhoods.csv', index=False)

print("Обработка завершена. Данные сохранены в 'data_with_neighborhoods.csv'")
