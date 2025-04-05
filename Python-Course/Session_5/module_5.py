# from collections import Counter
import os
from pathlib import Path
from random import seed

from random import choice
from typing import List, Union

# import requests
# from requests.exceptions import ConnectionError
# from gensim.utils import simple_preprocess


S5_PATH = Path(os.path.realpath(__file__)).parent

PATH_TO_NAMES = S5_PATH / "names.txt"
PATH_TO_SURNAMES = S5_PATH / "last_names.txt"
PATH_TO_OUTPUT = S5_PATH / "sorted_names_and_surnames.txt"
PATH_TO_TEXT = S5_PATH / "random_text.txt"
PATH_TO_STOP_WORDS = S5_PATH / "stop_words.txt"


def task_1():
    seed(1)
    new_lst = []

    with open("names.txt", "r", encoding="utf-8") as file:
        names =  file.readlines()                                   # chtenije vseh strok v spisok

    names = [name.strip().lower() for name in names]
    sorted_names = sorted(names)                                    # poluchaju list

    with open("last_names.txt", "r", encoding="utf-8") as file:
        value = file.readlines()
    last_names = [name.strip().lower() for name in value]

    for x in sorted_names:
        new_lst.append(x +" " + choice(last_names))

    with open("sorted_names_and_surnames.txt", "w", encoding="utf-8") as file:
        for i in new_lst:
            file.write(i + "\n")

task_1()
        



def task_2(top_k: int):
    pass


def task_3(url: str):
    pass


def task_4(data: List[Union[int, str, float]]):
    pass


def task_5():
    a, b = input("Vvedite dve cifry cherez probel:").split()
    try:
        x = int(a) / int(b)
    except ZeroDivisionError:
        print("Can't divide by zero")
    except ValueError:
        print("Entered value is wrong")
    else:
        print(x)
    finally:
        print("Game Over")


task_5()