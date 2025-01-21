def get_dict (string):
    dict1 = {}
    for letter in string:
        if letter not in dict1:
            dict1[letter] = 0
        dict1[letter] += 1
    return dict1

print(get_dict('abrakadabra'))


def get_intersect(lst1, lst2):
    lst3 = set()
    for num1 in lst1:
        for num2 in lst2:
            if num1 == num2:
                lst3.add(num1)
    lst3 = list(lst3)
    return lst3

print(get_intersect([1,2,3,4], [2,5,6]))





def outer_function(msg):
    return msg

my_closure = outer_function("Hello, World1!")
print(my_closure)  # Вывод: Hello, World!


def outer_function(msg):
    b=2
    def inner_function():
        a=3
        for name, value in locals().items():
            print(f"//////{name}: {value}")
        print(msg)
    return inner_function

my_closure = outer_function("Hello, World2!")

my_closure()  # Вывод: Hello, World!

print(dir(__builtins__))
print(dir(globals()))

import numpy
print(numpy)

print(dir(globals()))

# Пример глобального пространства имен
def show_global_namespace():
    print("Глобальное пространство имен:")
    for name, value in globals().items():
        print(f"{name}: {value}")

a=3222
# Пример локального пространства имен
def show_local_namespace():
    local_var = 10
    print("Локальное пространство имен:")
    for name, value in locals().items():
        print(f"{name}: {value}")



def main():
    global_var = "I am global"
    show_global_namespace()
    show_local_namespace()

if __name__ == "__main__":
    main()


def outer_function():
    x = "outer"

    def middle_function():
        x = "middle"

        def inner_function():
            nonlocal x
            x = "inner"
            print("inner_function:", x)  # Вывод: inner

        inner_function()
        print("middle_function:", x)  # Вывод: inner

    middle_function()
    print("outer_function:", x)  # Вывод: outer


outer_function()




