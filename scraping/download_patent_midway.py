import time
import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def init_driver(download_path):
    '''
    Initialize the FirefoxDriver with specified download preferences.
    '''
    driver_path = '/home/guankun/geckodriver'
    service = Service(executable_path=driver_path)
    
    # Set up options for Firefox webdriver
    options = Options()
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    options.headless = True  # Uncomment to run Firefox in headless mode
    
    # Create a Firefox profile with specific preferences for downloading files without dialog
    options.set_preference("browser.download.folderList", 2)  # 0: desktop, 1: default download folder, 2: custom location
    options.set_preference("browser.download.dir", download_path)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/csv, text/csv, application/vnd.ms-excel, application/octet-stream")  # MIME types
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("pdfjs.disabled", True)

    if not os.path.exists(download_path):
        os.makedirs(download_path)
    
    # Initialize and return the WebDriver
    driver = webdriver.Firefox(service=service, options=options)

    return driver

if __name__ == "__main__":
    # Set up argparse
    parser = argparse.ArgumentParser(description='Download csv files of patent records over a given range of years')
    parser.add_argument('--download_path', type=str, default='/home/guankun/MACS_30123/final-project-les-deux-mousquetaires', help='download default directory')
    parser.add_argument('--start_year', type=int, default=2019, help='Starting year for patents to be downloaded')
    parser.add_argument('--end_year', type=int, default=2023, help='Ending year for patents to be downloaded')
    args = parser.parse_args()

    # Define start date and end date
    start_date = datetime(args.start_year, 1, 1)
    end_date = datetime(args.end_year, 1, 1)
    current_date = start_date

    # Initialize the dataframe that stores the patent details
    columns = ['id', 'title', 'assignee', 'inventor/author', 'priority date',
       'filing/creation date', 'publication date', 'grant date', 'result link',
       'representative figure link', 'period']
    patent_data = pd.DataFrame(columns=columns)

    # Begin the loop to download patent data from start year to end year
    while current_date < end_date:
        driver = init_driver(args.download_path)
        try:
            next_month = current_date + timedelta(days=32)  # Ensure moving to the next month
            first_day_next_month = datetime(next_month.year, next_month.month, 1)
            after_date = current_date.strftime('%Y%m%d')
            before_date = (first_day_next_month - timedelta(days=1)).strftime('%Y%m%d')

            url = f'https://patents.google.com/?q=(artificial+intelligence)&country=US&before=publication:{before_date}&after=publication:{after_date}&language=ENGLISH&type=PATENT'
            print(f'Begin downloading patent records from {after_date} to {before_date}.')
            driver.get(url)
            time.sleep(10)

            # Download csv files of patent data for each month
            download_css_selector = "a[href*='download=true'] iron-icon[icon='icons:file-download']"
            # Wait for the link to be clickable and click it
            link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, download_css_selector))
            )
            link.click()
            # Wait for files to be downloaded
            time.sleep(15)
        finally:
            driver.quit()
        
        # Read the downloaded csv file (there should only be one file since 
        # newly downloaded files will be deleted after concatenating to the
        # final big data frame)
        csv_path = os.path.join(args.download_path, os.listdir(args.download_path)[0])
        df = pd.read_csv(csv_path, skiprows=[0], header=[0])
        df['period'] = [after_date + '-' + before_date] * len(df)

        # Concatenate the records to patent_data
        patent_data = pd.concat([patent_data, df], ignore_index=True)
        print(f'Added patent records from {after_date} to {before_date} to patent_data dataframe.')

        # Delete the temporarily downloaded csv file
        os.remove(csv_path)
        
        # Update current_date for scraping
        current_date = first_day_next_month
