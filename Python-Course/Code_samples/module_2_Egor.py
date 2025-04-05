from collections import defaultdict as dd
from typing import Any, Dict, List, Tuple


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]):
    for key, value in data_2.items():
        data_1[key] = data_1.get(key, 0) + value
    return data_1


def task_2() -> Dict[int, int]:
    return {num: num ** 2 for num in range(1, 16)}


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


def task_4(data: Dict[str, int]) -> List[int]:
    def sorting(values, key_val):
        for idx in range(len(values)):
            if values[idx] is None:
                values[idx] = key_val
                break
            if values[idx][1] < key_val[1]:
                values[idx], key_val = key_val, values[idx]

    values = [None] * 3
    for key, value in data.items():
        sorting(values, (key, value))

    return [key_val[0] for key_val in values if key_val is not None]


def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
    output = dd(list)

    for key, value in data:
        output[key].append(value)
    return output


def task_6(data: List[Any]) -> List[Any]:
    return list(set(data))


def task_7(words: [List[str]]) -> str:
    min_len_word = min(words, key=lambda x: len(x))
    output = []

    for idx in range(len(min_len_word)):
        if all([word[idx] == min_len_word[idx] for word in words]):
            output.append(min_len_word[idx])
        else:
            break

    return "".join(output)


def task_8(haystack: str, needle: str) -> int:
    def compute_lps(pattern):
        lps = [0] * len(pattern)
        length = 0
        idx = 1

        while idx < len(pattern):
            if pattern[idx] == pattern[length]:
                length += 1
                lps[idx] = length
                idx += 1
            else:
                if length != 0:
                    length = lps[length - 1]
                else:
                    lps[idx] = 0
                    idx += 1
        return lps

    if not needle:
        return 0

    lps = compute_lps(needle)
    i = j = 0

    while i < len(haystack):
        if haystack[i] == needle[j]:
            i += 1
            j += 1

        if j == len(needle):
            return i - j

        elif i < len(haystack) and haystack[i] != needle[j]:
            if j != 0:
                j = lps[j - 1]
            else:
                i += 1

    return -1
