import csv
import random

# Чтение данных из CSV файла с использованием with
with open('green_taxi_data2.csv', mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Обработка данных
header = data[0]
header.append('customer_type')
for row in data[1:]:  # Пропускаем заголовок
    booking_type = row[29]
    if booking_type == "Street":
        row.append("Individual")
    else:
        row.append(random.choice(["Individual", "Business"]))


# Запись данных обратно в CSV файл с использованием with
with open('output_customer_type.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print("Обработка завершена.")