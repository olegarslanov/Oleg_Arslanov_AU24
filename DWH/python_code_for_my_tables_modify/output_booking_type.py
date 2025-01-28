import csv
import random

# Чтение данных из CSV файла с использованием with
with open('train2.csv', mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Обработка данных (пример: добавление нового столбца booking_type)
header = data[0]
header.append('booking_type')
for row in data[1:]:  # Пропускаем заголовок
    vendor_name = row[2]
    if vendor_name == "Green Taxi":
        row.append("Phone")
    elif vendor_name == "Yellow Taxi":
        row.append(random.choice(["Street", "Phone"]))
    else:
        row.append('unknown')

# Запись данных обратно в CSV файл с использованием with
with open('output_booking_type.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print("Обработка завершена. Данные сохранены в 'output_booking_type.csv'")
