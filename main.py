import pandas as pd
import schedule
import time
import random
from datetime import datetime
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup

# List of proxies
proxies = [
    "http://proxy1_ip:proxy1_port",
    "http://proxy2_ip:proxy2_port",
    # Add more proxies as needed
]

# Function to initialize the Selenium driver with randomized settings
def initialize_driver():
    # Randomize user agent
    ua = UserAgent()
    user_agent = ua.random

    # Randomly select a proxy
    proxy = random.choice(proxies)

    # Set up Firefox options with user-agent and proxy
    options = webdriver.FirefoxOptions()
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument(f"--proxy-server={proxy}")
    options.add_argument("start-maximized")

    # Initialize FirefoxDriver
    service = Service('geckodriver-2')
    driver = webdriver.Firefox(service=service, options=options)
    return driver

# Simulate human-like interaction with scrolling
def simulate_human_behavior(driver):
    # Scroll down to the bottom of the page
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(1, 3))  # Random pause between 1-3 seconds

    # Scroll back up a little
    driver.execute_script("window.scrollBy(0, -300);")
    time.sleep(random.uniform(1, 2))  # Random pause between 1-2 seconds

# Main function to scrape the data from the table
def scrape_table():
    driver = initialize_driver()
    url = "https://www.talitywellness.ca/book-online?_mt=%2Fschedule%2Fdaily%2F48541%3Flocations%3D48717%2C48718"
    driver.get(url)

    # Simulate human-like behavior
    simulate_human_behavior(driver)

    # Debugging steps for iframe loading and switching
    try:
        print("Waiting for iframe to load...")
        iframe = WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[title='mt_integrations']"))
        )
        print("Iframe located. Attempting to switch to iframe...")
        driver.switch_to.frame(iframe)
        print("Switched to iframe successfully.")
    except Exception as iframe_error:
        print(f"Failed to load or switch to iframe: {iframe_error}")
        driver.quit()
        return  # Exit early if iframe switching fails

        # Attempt to find and click the "Accept All Cookies" button
    try:
        print("Looking for 'Accept All Cookies' button with data-test-button selector...")

        # First selector attempt with data-test-button attribute
        cookie_button = WebDriverWait(driver, 25).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-test-button='accept-all-cookies']"))
        )
        print("Cookie button located with data-test-button selector. Attempting to click...")
        cookie_button.click()
        print("Cookie consent accepted.")
    except Exception as button_error:
        print(f"Failed with data-test-button selector: {button_error}. Trying class-based selector.")

            # Try an alternative selector based on the button's classes if the first one fails
        try:
                print("Looking for 'Accept All Cookies' button with class-based selector...")
                cookie_button = WebDriverWait(driver, 25).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.StyledButton-sc-1ubu8st.StyledUIButton-sc-1q9p0gx"))
                )
                print("Cookie button located with class-based selector. Attempting to click...")
                cookie_button.click()
                print("Cookie consent accepted with class-based selector.")
        except Exception as alt_button_error:
                print(f"Both methods failed to locate or click the button: {alt_button_error}")

    finally:
            # Ensure switching back to main content to avoid iframe lock
            driver.switch_to.default_content()
            print("Switched back to main content.")

            print("Proceeding with the rest of the scraping logic...")

    # Switch to the iframe containing the schedule table
    try:
        print("Waiting for iframe with the schedule table...")
        iframe_table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[title='mt_integrations']"))
        )
        driver.switch_to.frame(iframe_table)
        print("Switched to iframe containing the schedule table.")

        # Locate the table and extract data
        print("Waiting for schedule table to load...")
        table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table[data-test-table='schedule']"))
        )
        print("Table found, starting data extraction.")
        # Initialize data storage as a list of dictionaries

        scraped_data = []

        # Loop through each row in the table
        rows = table.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            try:
                # Extract the time from the first <td>
                time_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(1) p.BoldLabel-sc-ha1dsk")
                time = time_element.text.strip()

                # Extract the session type from the second <td>
                session_type_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(2) span.ButtonLabel-sc-vvc4oq")
                session_type = session_type_element.text.strip()

                # Extract the number of open seats from the third <td>
                seats_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(3) p.StyledNoWrapLabel-sc-6b5x4x")
                seats = seats_element.text.strip()

                # Record the current timestamp
                run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Append the row data as a dictionary to our list
                scraped_data.append({
                    "Run Time": run_time,
                    "Time": time,
                    "Session Type": session_type,
                    "Open Seats": seats
                })

                print(f"Scraped row - Time: {time}, Session: {session_type}, Seats: {seats}")
            except Exception as row_error:
                # Skip rows where elements are missing (e.g., session has passed)
                print(f"Skipping row due to missing elements: {row_error}")
                continue

        # Convert the scraped data to a DataFrame and append to CSV
        df = pd.DataFrame(scraped_data)
        file_path = "tality_scrape/tality_schedule_data.csv"
        df.to_csv(file_path, mode='a', header=not pd.io.common.file_exists(file_path), index=False)

        print(f"Data saved to {file_path}")

    except Exception as e:
        print(f"Failed to locate or load schedule table: {e}")

    finally:
        driver.quit()

# Schedule the task to run every hour at the 50-minute mark
schedule.every().hour.at(":50").do(scrape_table)

print("Starting schedule. The script will run every hour at the 50-minute mark.")
while True:
    schedule.run_pending()
    time.sleep(1)  # Wait 1 second between each check