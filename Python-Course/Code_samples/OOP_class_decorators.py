class Person:

    lifespan = 65

    def __init__(self, name):
        self.name = name

    @classmethod
    def increment_lifespan(cls):
        cls.lifespan += 1


person1 = Person('Alason')

#print(person1)

print(person1.lifespan)

Person.increment_lifespan()

print(person1.lifespan)
print(person1.name)
print(Person.lifespan)



class MyContextManager:
    def __enter__(self):
        print("Вход в контекстный менеджер")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Выход из контекстного менеджера")
        # Возврат True подавляет исключение, если оно возникло
        return False

# Использование собственного контекстного менеджера
with MyContextManager() as manager:
    print("Внутри блока контекстного менеджера")


