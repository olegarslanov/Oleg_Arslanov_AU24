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


def task_5():
    a, b = input("Vvedite dve cifry oi duhi cerez probel:").split()
    try:
        x = int(a)/int(b)
    except ZeroDivisionError:
        print("Can't divide by zero")
    except ValueError:
        print("Entered value is wrong")
    else:
        print(x)
    finally:
        print("Vsio")

task_5()