# from collections import defaultdict as dd
# from itertools import product
from typing import Any, Dict, List, Tuple


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]):
    dict_copy = data_1.copy()
    for key, value in data_2.items():
        if key in dict_copy:
            dict_copy[key] += value
        else:
            dict_copy[key] = value
    return dict_copy

data_1 = {'a': 123, 'b': 23, 'c': 0}
data_2 = {'a': 1, 'b': 11, 'd': 99}
#print(task_1(data_1, data_2))

def task_2():
    dict1 ={}
    n=15
    for i in range(1, n+1):
        dict1[i] = i * i
    return dict1

#print(task_2(n))


def task_3(data) -> List[str]:
    def backtrack(keys_idx, arr):
        if len(arr) == len(data):
            combinations.append("".join(arr))
            return

        key = keys[keys_idx]
        for value in data[key]:
            arr.append(value)
            backtrack(keys_idx + 1, arr)
            arr.pop()

    combinations = []
    keys = list(data.keys())
    backtrack(0, [])
    return combinations

# here Egor implementation
dict1 = {'1': ['a', 'b'], '2': ['c', 'd'], '3': ['d', 'e']}
#print(task_3(dict1))

def task_4(data: Dict[str, int]):
    dict2 = []
    copy_dict1 = dict1.copy()
    while len(dict2) < 3 and copy_dict1:
        max_v = list(copy_dict1.values())[0]
        max_k = list(copy_dict1.keys())[0]
        for key, value in copy_dict1.items():
            if value > max_v:
                max_v = value
                max_k = key
        del copy_dict1[max_k]
        dict2.append(max_v)

    return dict2

input_dict = {'a': 500, 'b': 5874, 'c': 560,'d': 400, 'e': 5874, 'f': 20}
print(task_4(input_dict))


def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
    dict1 = {}
    for key, value in lst10:
        if key not in dict1:
            dict1[key] = [value]
        else:
            dict1[key].append(value)
    return dict1

lst10 = [('yellow', 1), ('blue', 2), ('yellow', 3), ('blue', 4), ('red', 1)]
#print(task_5(lst10))


def task_6(data: List[Any]):
    lst1 =[]
    for i in data:
        if i not in lst1:
            lst1.append(i)
    return lst1

data = [1, 1, 3, "3"]
#print(task_6(data))


def task_7(words: [List[str]]) -> str:
    min_len_word = min(words, key = lambda x: len(x))
    lst2 = []

    for num in range(len(min_len_word)):
        if all([word[num] == min_len_word[num] for word in words]):
            lst2.append(min_len_word[num])
        else:
            break
    return "".join(lst2)

lst1 = ["flower", "flows"]
#print(task_7(lst1))


def task_8(haystack: str, needle: str) -> int:
    pass
