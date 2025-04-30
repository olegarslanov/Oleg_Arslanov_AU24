#incomeStatementsReportPage.py
from selenium.webdriver.support.ui import WebDriverWait as WDW
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import *
import allure

# here create connection to url pages (objects) from class
class IncomeStatementsReportPage:
    def __init__(self, driver, delay):
        self.driver = driver
        self.delay = delay

        # locators
        self.capture_report_views_button_xpath = "//div[contains(text(), 'Capture report views')]"

    #def open_capture_report_views(self):
        #capture_button = WDW(self.driver, self.delay).until(EC.element_to_be_clickable((By.XPATH,
                                                                    #self.capture_report_views_button_xpath)))
        #capture_button.click()

    def open_capture_report_views(self):
        # 1. Waiting for download button "Capture report views"
        capture_button = WDW(self.driver, self.delay).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "img[alt='Capture report views']"))
        )
        # 2. Scrolling to button
        self.driver.execute_script("arguments[0].scrollIntoView(true);", capture_button)
        # 3. Click button
        capture_button.click()

    def switch_to_report_frame(self):
        # wait for iframe start and switch
        iframe = WDW(self.driver, self.delay).until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
        self.driver.switch_to.frame(iframe)

    def get_report_title(self):
        # search element inside report
        report_header = WDW(self.driver, self.delay).until(
            EC.presence_of_element_located((By.XPATH, "//*[text()='Q&A']"))
        )
        return report_header.text

    def select_slicer_value(self, start_date_value):
        # Waiting downloaded all input fields with date
        slicer_inputs = WDW(self.driver, self.delay).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "date-slicer-input"))
        )
        # Check how many found
        if not slicer_inputs:
            raise Exception("No date slicer input fields found.")

        print(f"Found {len(slicer_inputs)} date input fields.")

        # Insert value in first field (Start Date)
        start_date_input = slicer_inputs[0]

        start_date_input.clear()  # first clear field
        start_date_input.send_keys(start_date_value)

        print(f"Entered start date: {start_date_value}")

    @allure.feature('VanArsdel Button')
    @allure.story('Click VanArsdel button')
    def click_vanarsdel_button(self):
        with allure.step("Wait for 'Total Unit of VanArsdel' button to be clickable"):
            # Ожидаем, пока кнопка станет кликабельной
            WDW(self.driver, self.delay).until(
                EC.element_to_be_clickable((By.XPATH, "//*[text()='Total Unit of VanArsdel']"))
            )




