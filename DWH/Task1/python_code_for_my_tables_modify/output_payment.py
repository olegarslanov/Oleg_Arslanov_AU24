import csv
import random

# Чтение данных из CSV файла с использованием with
with open('train2.csv', mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Обработка данных
header = data[0]
header.append('payment_type')
for row in data[1:]:  # Пропускаем заголовок
    store_and_fwd_flag = row[23]
    if store_and_fwd_flag == "Y":
        row.append("Card")
    elif store_and_fwd_flag == "N":
        row.append(random.choice(["Card", "Cash"]))
    else:
        row.append('unknown')

# Запись данных обратно в CSV файл с использованием with
with open('output_payment_type.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print("Обработка завершена. Данные сохранены в 'output_payment_type.csv'")