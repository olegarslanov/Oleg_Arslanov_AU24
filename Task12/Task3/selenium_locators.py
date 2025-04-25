from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import time

# path to the driver
service = Service(ChromeDriverManager().install())

# Initialize the WebDriver
driver = webdriver.Chrome(service=service)

# Open the website
driver.get("https://www.phptravels.com/demo/")
# use only this address because second https://phptravels.org/register.php is not exists
# https://phptravels.com/blog/ visualy it changed, but I found similar item and it have references to another address ... I probe but can not do this

# Example 1: Locator by class name (search 'Submit' button name)
element_by_class = driver.find_element(By.CLASS_NAME, "btn-lg")
print("Element found by CLASS_NAME:", element_by_class.text)

# Example 2: Locator by class name
element_by_class2 = driver.find_element(By.CLASS_NAME, "bg-light")
print("Element found by CLASS_NAME:", element_by_class2.text)


# Example 3: Locator by ID (search 'Submit' button name)
element_by_id = driver.find_element(By.ID, "demo")
print("Element found by ID:", element_by_id.text)

# Example 4: Another locator by ID
element_by_id2 = driver.find_element(By.ID, "number")
print("Found by ID:", element_by_id2.get_attribute("placeholder"))


# Example 5: Locator by name (search fo default value in email field)
element_by_name = driver.find_element(By.NAME, "email")
print("Email input found by NAME:", element_by_name.get_attribute("placeholder"))


# Example 6: Another locator by name (search for email field class name)
element_by_name_2 = driver.find_element(By.NAME, "email")
print("Email input found by NAME:", element_by_name_2.get_attribute("class"))

# Example 7: Locator by CSS selector
element_by_css = driver.find_element(By.CSS_SELECTOR, ".p-md-4 h5 strong")
print("Element found by CSS_SELECTOR:", element_by_css.text)

# Example 8: Another locator by CSS selector

# Find the dropdown list using the CSS selector
select_element = driver.find_element(By.CSS_SELECTOR, "select#country_id")
# Wrap it in Select object
select = Select(select_element)
# Select the country by its visible text
country_name = "Azerbaijan +994"
select.select_by_visible_text(country_name)
# Print the selected country
selected_option = select.first_selected_option
print(f"Selected country: {selected_option.text}")


# Example 9: Locator by XPath
element_by_xpath = driver.find_element(By.XPATH, "//input[@placeholder='Result ?']")
print("Element found by XPATH:", element_by_xpath.get_attribute("placeholder"))

# Example 10: Another locator by XPath
element_by_xpath_2 = driver.find_element(By.XPATH, "//div[@class='form-floating mb-3']//input[@name='first_name']")
print("Element found by XPATH (2nd example):", element_by_xpath_2.text)


# Example 11: Relative Locator (above)
#element_relative = driver.find_element(
    #By.TAG_NAME, "input"
#).above(driver.find_element(By.ID, "password"))

#print("Element found by Relative Locator (Above):", element_relative)


# Allow time to see the results
time.sleep(5)

# Quit the driver
driver.quit()








