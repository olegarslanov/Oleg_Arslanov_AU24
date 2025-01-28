import csv

# Чтение данных из CSV файла с использованием with
with open('green_taxi_data.csv', mode='r') as file:
    reader = csv.reader(file)
    data = list(reader)

# Обработка данных
unique_rows = set()

for row in data:
    unique_rows.add(row[0])

if len(unique_rows) == len(data):
    print("Dublikaty est")
else:
    print("Dublikatov netu")


