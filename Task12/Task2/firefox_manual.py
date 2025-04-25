from selenium import webdriver
import time

def test_open_google_firefox():
    # I download geckodriver.exe and added to environment Path, so automatically webdriver.Firefox can find him
    driver = webdriver.Firefox()
    driver.get("https://www.google.com")
    print("Firefox page title:", driver.title)
    time.sleep(2)
    driver.quit()
