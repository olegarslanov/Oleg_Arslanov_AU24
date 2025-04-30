import pytest
import yaml
import allure

def get_sql_queries(mark_type):
    with open("config_sql.yaml", 'r') as f:
        return yaml.safe_load(f)[mark_type]

@pytest.mark.smoke  #this code only for smoke code
@pytest.mark.parametrize("case", get_sql_queries("smoke")) #reexecute code with different values
@allure.feature("Database Checks")
@allure.story("Smoke Tests for Database Queries")
def test_smoke_queries(case, db_connection):
    cursor = db_connection.cursor()
    cursor.execute(case["sql"])
    result = cursor.fetchone()[0]
    assert result == case["expected"], f"{case['name']} failed"


@pytest.mark.critical
@pytest.mark.parametrize("case", get_sql_queries("critical"))
@allure.feature("Database Checks")
@allure.story("Check if the 'dim_customers_scd' table has at least one row")
def test_critical_queries(case, db_connection):
    cursor = db_connection.cursor()
    cursor.execute(case["sql"])
    result = cursor.fetchone()[0]

    expected = case["expected"]

    if isinstance(expected, str) and expected.startswith(">"):
        threshold = int(expected[1:].strip())
        assert result > threshold, f"{case['name']} failed: expected > {threshold}, got {result}"
    else:
        assert result == expected, f"{case['name']} failed: expected {expected}, got {result}"


