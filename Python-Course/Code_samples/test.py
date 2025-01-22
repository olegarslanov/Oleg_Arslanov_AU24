









a = 'global'


def outer_function():
    a = 'local'
    print("Внешняя функция")
    print(locals())
    print(globals())

    def inner_function():
        a = 'podlocal'
        print("Вложенная функция")
        print(locals())
        print(globals())

    inner_function()

print(locals())
print(globals())

# Вызов внешней функции
outer_function()






