from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# 🔹 Start browser
driver = webdriver.Chrome()

# 🔹 Open Google
driver.get("https://www.google.com")

# 🔹 Find search box and type query
search_box = driver.find_element(By.NAME, "q")
search_box.send_keys("Selenium Python")
search_box.send_keys(Keys.RETURN)

# 🔹 Wait for results
wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.ID, "search")))

# 🔹 Make screenshot
driver.save_screenshot("google_search_result.png")
print("📸 Screenshot saved!")

# 🔹 Optional: for user can see that browser is opened
time.sleep(3)

# 🔹 Close browser
driver.quit()
