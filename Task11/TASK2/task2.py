import pytest
import time
import yaml


def get_numbers_data(config_name="config.yaml"):
    with open(config_name, 'r') as stream:
        config = yaml.safe_load(stream)
    return config['cases']


def add_numbers(a, b, c):
    try:
        return a + b + c
    except TypeError:
        raise 'Please check the parameters. All of them must be numeric'


# Parametrize test with mark smoke
@pytest.mark.smoke # custom mark for smoke tests filter
@pytest.mark.parametrize("case", get_numbers_data()) # parametrize for several execute with different input values
def test_add_numbers(case):
    a, b, c = case["input"]
    expected = case["expected"]
    result = add_numbers(a, b, c)
    assert result == expected


# Test for incorrect data types with mark critical
@pytest.mark.critical #custom mark for critical tests filter
def test_add_floats():
    with pytest.raises(TypeError):
        add_numbers('a', 1, 2)




