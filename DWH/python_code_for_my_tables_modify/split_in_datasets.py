import csv
import random

# Чтение данных из CSV файла с использованием with
with open('train2.csv', mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Обработка данных
header = data[0]

for row in data[1:]:  # Пропускаем заголовок
    vendor_name = row[2]
    if vendor_name == "Green Taxi":
        data.remove(row)


# Запись данных обратно в CSV файл с использованием with
with open('dataset1.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print("Обработка завершена. Данные сохранены в 'dataset1.csv'")