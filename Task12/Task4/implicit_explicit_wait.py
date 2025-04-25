from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.chrome.service import Service as ChromeService

# Initialize the WebDriver with Service object
#service = Service(ChromeDriverManager().install())
#driver = webdriver.Chrome(service=service)

driver = webdriver.Chrome(service=ChromeService())

# Set implicit wait
driver.implicitly_wait(10)  # Wait for elements to be available for 10 seconds

# Open google.com
driver.get("https://www.google.com")

# Search and click on button "Priimti viska"
accept_button = driver.find_element(By.ID, "L2AGLb")
accept_button.click()

print("Clicked on accept button")

# Time for close
time.sleep(2)

# Find the search input field and enter "Selenium"
search_box = driver.find_element(By.NAME, "q")

time.sleep(5)

search_box.send_keys("Selenium")

time.sleep(5)

search_box.send_keys(Keys.RETURN)  # Press Enter to search

# Get list of all iframe
iframes = driver.find_elements(By.TAG_NAME, "iframe")

# Check iframe that have in recaptcha
for iframe in iframes:
    src = iframe.get_attribute("src")
    if "recaptcha" in src:
        print("reCAPTCHA URL найден:", src)
        break

# Pereiti po src v tom zhe driver
recaptcha_url = iframe.get_attribute("src")
driver.get(recaptcha_url)


# Found checkbox
checkbox = driver.find_element(By.ID, "recaptcha-anchor")

print("Found checkbox")

checkbox.click()

# Check checkbox is on or off
checked = checkbox.get_attribute("aria-checked")
if checked == "true":
    print("Checkbox is on!")
else:
    print("Checkbox off.")


# Give time for execute
time.sleep(2)

# Use explicit wait to wait for search results to appear
wait = WebDriverWait(driver, 10)


first_result = wait.until(EC.presence_of_element_located((By.XPATH, "(//h3)[1]")))

# Click on the first result link
first_result.click()

# Take a screenshot of the result
driver.save_screenshot("selenium_first_result.png")
print("Screenshot saved as 'selenium_first_result.png'")

# Wait for a few seconds to see the result
time.sleep(5)

# Close the driver
driver.quit()

