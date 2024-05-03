from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
import time
from datetime import datetime, timedelta


# Set start and end dates for the loop
start_date = datetime(2019, 1, 1)
end_date = datetime(2024, 1, 1)

current_date = start_date

while current_date < end_date:
    # Initialize the Firefox WebDriver with options each time in the loop
    firefox_options = Options()
    # Uncomment below line to run Firefox in headless mode (no GUI)
    firefox_options.add_argument('--headless')
    
    driver = webdriver.Firefox(options=firefox_options)

    try:
        # Calculate the first day of the next month
        next_month = current_date + timedelta(days=32)  # Add enough days to ensure moving to the next month
        first_day_next_month = datetime(next_month.year, next_month.month, 1)
        
        # Format dates for the URL
        after_date = current_date.strftime('%Y%m%d')
        before_date = (first_day_next_month - timedelta(days=1)).strftime('%Y%m%d')
        print(after_date, before_date)  # Optional: Print the dates to monitor progress
        
        # Construct the URL with the formatted dates
        url = f'https://patents.google.com/?q=(artificial+intelligence)&country=US&before=publication:{before_date}&after=publication:{after_date}&language=ENGLISH&type=PATENT&oq=(artificial+intelligence)+country:US+before=publication:{before_date}+after=publication:{after_date}+language=ENGLISH+type=PATENT&dups=language'
        
        # Visit the web page
        driver.get(url)
        
        # Wait for the page to load
        time.sleep(15)  # Adjust the waiting time based on actual conditions

        # Locate and click the download CSV link
        download_link = driver.find_element(By.CSS_SELECTOR, '#count > div.layout.horizontal.style-scope.search-results > span.headerButton.style-scope.search-results > a')
        download_link.click()

        # Additional waiting time to complete download
        time.sleep(10)
        
    finally:
        # Close the browser after each download
        driver.quit()

    # Update the date to the first day of the next month
    current_date = first_day_next_month
