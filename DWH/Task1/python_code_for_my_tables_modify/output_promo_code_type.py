import csv
import random

# Чтение данных из CSV файла с использованием with
with open('green_taxi_data.csv', mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Обработка данных
header = data[0]
header.append('promo_code')
for row in data[1:]:  # Пропускаем заголовок
    customer_type = row[35]
    booking_type = row[29]
    if customer_type == "Individual" and booking_type == "Phone":
        row.append(random.choice(["Promo20", "Promo10", "NULL"]))
    else:
        row.append("NULL")


# Запись данных обратно в CSV файл с использованием with
with open('output_promo_code_type.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print("Обработка завершена")