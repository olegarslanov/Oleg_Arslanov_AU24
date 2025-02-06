from typing import List


def task_1(array: List[int], target: int) -> List[int]:
    """
        We get here list of two numbers that added got target value
        """
    result = []
    seen = set()
    for y in array:
        x = target - y
        if x in seen:
            return [x, y]
        seen.add(y)
    return result


#sample = [2, 7, 11, 15]
#target = 9
#print(task_1(sample, target))


def task_2(number: int) -> int:
    """
        We get here mirror number
        """
    return_number = 0
    while number != 0:
        last_digit = number % 10
        return_number = return_number * 10 + last_digit
        number = number // 10
    return return_number


# sample = 130
# print(task_2(sample))


def task_3(array: List[int]) -> int:
    """
    We get the same number from list
    """
    len1 = len(array)
    for i in range(len1-1):
        for j in range(i+1, len1):
            if array[i] == array[j]:
                return array[i]
    return -1

#sample = [2, 1, 3, 4, 2]
#print(task_3(sample))


def task_4(string: str) -> int:
    """
       Convert Roman numeral to Arabic integer.
       """
    int1 = 0
    dict1 = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}

    i = 0
    for i in range(len(string)):
        if i < len(string) - 1:
            s1 = string[i]
            s2 = string[i + 1]
            if s1 == "I" and s2 in ["V", "X"]:
                int1 += dict1[s2] - dict1[s1]
                i += 1
            elif s1 == "X" and s2 in ["L", "C"]:
                int1 += dict1[s2] - dict1[s1]
                i += 1
            elif s1 == "C" and s2 in ["D", "M"]:
                int1 += dict1[s2] - dict1[s1]
                i += 1
            else:
                int1 += dict1[s1]
        else:
            int1 += dict1[string[i]]
    return int1


# sample = "XIX"
# print(task_4(sample))


def task_5(array: List[int]) -> int:
    """
    Find min integer
    """

    level = array[0]
    for y in array:
        if y < level:
            level = y
    return level

#sample = [3, 4, -1, 10, 12]
#print(task_5(sample))
