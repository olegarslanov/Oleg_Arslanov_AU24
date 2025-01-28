import csv
import random

# Чтение данных из CSV файла с использованием with
with open('../green_taxi_data2.csv', mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Функция для генерации случайного телефона в формате +1 (XXX) XXX-XXXX
def generate_random_phone():
    area_code = random.randint(200, 999)  # Код региона
    first_part = random.randint(200, 999)  # Первая часть номера
    second_part = random.randint(1000, 9999)  # Вторая часть номера
    return f"+1 ({area_code}) {first_part}-{second_part}"

# Обработка данных
header = data[0]
header.append('customer_telephone')
for row in data[1:]:  # Пропускаем заголовок
    booking_type = row[17]
    if booking_type == "Street":
        row.append("NULL")
    else:
        row.append(generate_random_phone())


# Запись данных обратно в CSV файл с использованием with
with open('output_customer_telephone.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print("Обработка завершена.")