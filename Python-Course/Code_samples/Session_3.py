
# LOCAL, GLOBAL, NONLOCAL

x = 'Hi'
def func():
    x = 'Ni'

    def nested():
        nonlocal x
        x = 'Spam'
        print(x)
    nested()
    print(x)

func()
print(x)


# CLOSURES

def maker(n):
    def action(x):
        return x * n
    return action

#sohraniaem v globale v peremenuju vse zavisimosti funkcij maker, daze posle ee otrabotki
f = maker(2)
print(f)
print(maker(3))

print(f(3))

g = maker(3)
print(g(3))



# DECORATORS

def f1(func):
    def wrapper():
        print("Started")
        func()
        print("Ended")
    return wrapper

@f1
def f():
    print("Hello")
f()



def null_decorator(func):
  return func

def greet():
  return 'hello'
greet2 = null_decorator(greet)
print(greet2())

@null_decorator
def greet():
    return 'hello'
print(greet())


def logger(f):
    def wrapper(*args, **kwargs):
        print("Function execution started")
        res = f(*args, **kwargs)
        print("Function execution ended")
        return res
    return wrapper

def func(x, y):
    return x + y
logger2 = logger(func)
print(logger2(1,2))

@logger
def func(x, y):
    return x + y
print(logger(func(1, 3)))



#OPEN files

f = open("test.txt")
while len(f.readline()) > 0:
    print (f.readline())


with open("module_2_Egor.py") as my_file:
    print(my_file.read())


# MODULES

import greeter
greeter.greet("Oleg")


import greeter as hello
hello.greet("Student")


from greeter import greet
greet("Me-me-eee")


import math
math_contents = dir(math)
print(math_contents)

import importlib
import greeter  # Предположим, что greeter — это ваш модуль

# Внесли изменения в my_module...

# Перезагружаем модуль
importlib.reload(greeter)


#PACKAGES

import pandas







