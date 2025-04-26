import pytest
import time

# fixture for whole suite execution
@pytest.fixture(scope="session", autouse=True)  # here choose session so fixture call one time only
def suite_timer():
    start = time.time()  # start execution test time
    yield # here execute all tests
    end = time.time()  # all test finish time
    print(f"\n[Suite duration] Total time: {end - start:.2f} seconds") # print used time of all tests

# fixture for time of each test execution
@pytest.fixture
def test_timer():
    start = time.time()
    yield
    end = time.time()
    print(f"[Test duration] Time: {end - start:.2f} seconds")

# declare logic that we use in our unit tests
def add_numbers(a, b):
    return a + b


def test_add_two_positive_numbers(test_timer):
    a, b = 3, 5
    result = add_numbers(a, b)
    time.sleep(2)
    assert result == 8


def test_add_two_negative_numbers(test_timer):
    a, b = -3, -5
    result = add_numbers(a, b)
    time.sleep(3)
    assert result == -8


def test_add_negative_and_positive_numbers():
    a, b = -3, 5
    result = add_numbers(a, b)
    time.sleep(10)
    assert result == 2