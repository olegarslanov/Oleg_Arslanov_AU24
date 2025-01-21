def divide(a, b):
    if b == 0:
        raise ZeroDivisionError("Деление на ноль недопустимо")
    return a / b


try:
    result = divide(10, 0)
    print(result)
except ZeroDivisionError as e:
    print(f"Ошибка: {e}")
finally:
    print("Завершение операции деления")



def safe_divide(a, b):
    return a / b

# Основная часть программы
print("Начало программы")

try:
    # Код, который может вызвать исключение
    result = safe_divide(10, 0)
    print(f"Результат: {result}")
except ZeroDivisionError:
    # Обработка исключения
    print("Ошибка: Деление на ноль недопустимо")

print("Конец программы")
