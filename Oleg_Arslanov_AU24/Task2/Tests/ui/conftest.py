#conftest.py Page Object Model
import os
import sys
import yaml
from selenium import webdriver
#from webdriver_manager.chrome import ChromeDriverManager
import pytest
from Pages.incomeStatementsReportPage import IncomeStatementsReportPage
#from selenium.webdriver.chrome.service import Service
#from selenium.webdriver.chrome.options import Options


def get_selenium_config(config_name):
    """Load configuration from YAML file"""
    module_dir = os.path.dirname(os.path.abspath(sys.modules[__name__].__file__))
    parent_dir = os.path.dirname(module_dir)

    config_path = os.path.join(parent_dir, '..', 'Configs', config_name)

    with open(config_path, 'r') as stream:
        config = yaml.safe_load(stream)

    return config['global']

@pytest.fixture(scope="session") # fixture that run before each test
def config():
    """configuration Fixture"""
    return get_selenium_config('config_selenium.yaml')

@pytest.fixture(scope="function") # fixture that run before each test
def open_income_statements_report_webpage(config):
    report_uri = get_selenium_config('config_selenium.yaml')['report_uri'] # web site from configuration file
    delay = get_selenium_config('config_selenium.yaml')['delay']

    # here use webdriver with path on environments (because problems with automatically download)
    driver = webdriver.Chrome()

    driver.set_window_size(1024, 600)
    driver.maximize_window()
    driver.get(report_uri) # download page with URL

    income_report = IncomeStatementsReportPage(driver, delay) # make object web page with working after
    income_report.open_capture_report_views() #with method open another page

    print("1. We are opened image with link 'Capture report views'")

    # Switch to iframe
    income_report.switch_to_report_frame()

    print("2. We are inside iframe")

    yield income_report # here fixture passes control to test
    driver.quit() # after test driver will be closed



