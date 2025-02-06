import time
from typing import List

Matrix = List[List[int]]


def task_1(exp: int):
    def power (num):
        return num ** exp
    return power

a = task_1(3)
print(a(4))


def task_2(*args, **kwargs):
    for i in args:
        print(i)
    for key, value in kwargs.items():
        print(value)


task_2 (1, 2, 3, moment=4, cap="arkadiy")



def helper(func):
    def wrapper(*args, **kwargs):
        print ("Hi, friend! What's your name?")
        func(*args,**kwargs)
        print("See you soon!")
    return wrapper

@helper
def task_3(name: str):
    print(f"Hello! My name is {name}.")

task_3("John")



def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        run_time = end_time - start_time

        print(f"Finished {func.__name__} in {run_time:.4f} secs")

    return wrapper

@timer
def task_4():
    return len([1 for _ in range(0, 10**8)])

task_4()


def task_5(matrix: Matrix) -> Matrix:
    copy_matrix = matrix.copy()
    for idx in range(len(matrix)):
        for jdx in range(len(matrix[idx])):
            if jdx != idx:
                copy_matrix[idx][jdx] = matrix[jdx][idx]
    return copy_matrix

matrix = [
    [1,2,3],
    [4,5,6],
    [7,8,9]
]

print(task_5(matrix))



def task_6(queue: str):
    pass
