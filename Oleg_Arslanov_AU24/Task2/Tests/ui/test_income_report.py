import allure
#import os
#import subprocess


@allure.feature('Decomposition Tree')
@allure.story('Open decomposition tree visualization')
def test_01_open_decomposition_tree_visualization(open_income_statements_report_webpage):
    with allure.step("Open Income Statements report webpage"):
        print("Test 1: Open decomposition tree visualization")
        report_page = open_income_statements_report_webpage
        report_page.switch_to_report_frame()

    with allure.step("Get report title"):
        report_title = report_page.get_report_title()

    with allure.step(f"Verify report title is 'Q&A'"):
        assert report_title == "Q&A"

    allure.attach(f"Report title: {report_title}", name="Report Title", attachment_type=allure.attachment_type.TEXT)
    print(f"Test 1: Got '{report_title}'. Completed successfully!")


@allure.feature('Income Statements')
@allure.story('Input slicer value')
def test_02_input_slicer_1value(open_income_statements_report_webpage):
    with allure.step("Open Income Statements report page"):
        print("Test 2.1: Open Income Statements report page")
        report_page = open_income_statements_report_webpage
        report_page.switch_to_report_frame()

    with allure.step("Select slicer value '1/1/2014'"):
        print("Test 2.2: Selecting slicer value '1/1/2014'")
        report_page.select_slicer_value('1/1/2014')

    allure.attach("Slicer value '1/1/2014' selected", name="Slicer Value", attachment_type=allure.attachment_type.TEXT)
    print("Test 2.3: Completed successfully!")


@allure.feature('VanArsdel Button')
@allure.story('Click VanArsdel button')
def test_03_click_button(open_income_statements_report_webpage):
    with allure.step("Click 'Total Unit of VanArsdel' button"):
        print("Test 3: Click 'Total Unit of VanArsdel' button")
        report_page = open_income_statements_report_webpage
        report_page.switch_to_report_frame()
        report_page.click_vanarsdel_button()

    allure.attach("VanArsdel button clicked", name="VanArsdel Button", attachment_type=allure.attachment_type.TEXT)
    print("Test 3: Completed successfully!")


#def run_tests():
    #"""Run tests with auto generation report Allure"""
    # Run pytest with  flag for save result
    #result = os.system('pytest --alluredir=allure-results')

    # If test successfully execute Allure
    #if result == 0:
        #print("Generating Allure report...")
        #subprocess.run(["allure", "serve", "allure-results"], check=True)
    #else:
        #print("Tests failed. Allure report will not be generated.")

#if __name__ == '__main__':
    #run_tests()