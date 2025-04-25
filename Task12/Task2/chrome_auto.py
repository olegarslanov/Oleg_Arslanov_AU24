
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
#from selenium.webdriver.common.by import By
import time

def test_open_google_chrome():
    # Selenium Manager will auto-download ChromeDriver if needed
    driver = webdriver.Chrome(service=ChromeService())
    driver.get("https://www.google.com")
    print("Chrome page title:", driver.title)
    time.sleep(2)
    driver.quit()
