from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
prefs= {
    "download.default_directory": r"C:\Users\monassib.ma\Desktop\mark\downloaded_files",
    "download.prompt_for_download": False,  # To automatically save files to the specified directory without prompting for download
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True  # It will not open PDF files in Chrome
}

user_data_dir=r'C:\Users\monassib.ma\AppData\Local\Google\Chrome\User Data'
driver = Driver(uc=True,chromium_arg=f"profile-directory=Profile 1,prefs={prefs}",user_data_dir=user_data_dir)

driver.get('https://zefys.staatsbibliothek-berlin.de/ddr-presse/')


#//*[@class="newspaper-list-item"]


input('Press Enter to close the browser...')

#//*[@class="calendar-scroll"]//a

from datetime import datetime, timedelta
import time



# Function to generate date strings for the URLs
def generate_date_strings(start_date, end_date):
    date_format = "%Y%m%d"
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]

    date_strings = []
    for date in date_generated:
        date_strings.append(date.strftime(date_format))
    return date_strings

# Main program
#1946 - 1990
base_url = "https://content.staatsbibliothek-berlin.de/zefys/SNP2532889X-{}-0-0-0-0.xml"
start_date = '19460423'
end_date = '19910101'
urls = []

# Generate the URLs
date_strings = generate_date_strings(start_date, end_date)
for date_string in date_strings:
    # Construct the URL for the given date
    url = base_url.format(date_string)
    urls.append(url)
print(len(urls))
for url in urls :
    driver.get(url)
    time.sleep(3)
    with open('SNP2532889X.txt', 'a') as f:
        f.write(url+"\n")

    
    