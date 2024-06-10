import os
import time
import pandas as pd
import schedule
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re
from datetime import date
import hashlib

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '', filename)

# Config
BASE_URL = "https://fondswelt.hansainvest.com/de/downloads-und-formulare/download-center"  
ROOT_DIR = r"c:\Projects\FundDatabase"
DATA_SOURCE = "Hansainvest"
METADATA_CSV = os.path.join(ROOT_DIR, "fundDatabase.csv")

def setup_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

#Creating file hash value
def calculate_file_hash(file_path, hash_algorithm = 'md5'):    
    hash_func = hashlib.new(hash_algorithm)
    
    with open(file_path, 'rb') as file:
        while chunk := file.read(8192):  
            hash_func.update(chunk)
    
    return hash_func.hexdigest()

#Download files
def download_pdfs():
    print("Start")
    driver = setup_webdriver()
    driver.get(BASE_URL)

    time.sleep(10)

    global_count = 0
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div[@class='table-responsive']")))
    except Exception as e:
        print(f"Problems while loading web page: {e}")
        driver.quit()
        return
    skroll = 280 

    metadata = []
    
    if os.path.exists(METADATA_CSV):
            metadata_df = pd.read_csv(METADATA_CSV)

    while global_count < 200:
        rows = driver.find_elements(By.XPATH, ".//table/tbody/tr")
        for row in rows:
            if(global_count < 200):
                global_count += 1
                driver.execute_script(f"window.scrollBy(0, {skroll});")
                skroll = 70
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    continue  

                isin = cells[0].find_element(By.XPATH, ".//span[@class='d-block']") 
                isin_text = isin.text.strip()
                configured_isin = sanitize_filename(isin_text)
                if not configured_isin:
                    continue  
                if os.path.exists(METADATA_CSV):
                    if configured_isin in metadata_df['ISIN'].values:
                        continue

                isin_dir = os.path.join(ROOT_DIR, DATA_SOURCE, configured_isin)
                os.makedirs(isin_dir, exist_ok=True)
   
                for cell in cells[1:]:
                    pdf_links = cell.find_elements(By.XPATH, ".//a[contains(@href, '.pdf')]")
                    for link in pdf_links:
                        pdf_url = link.get_attribute('href')
                        doc_type_url = pdf_url.rsplit('/', 1)[0]
                        document_type = doc_type_url.rsplit('/', 1)[-1]
                        pdf_name = pdf_url.split('/')[-1]
                        pdf_path = os.path.join(isin_dir, pdf_name)
                        date_source = cell.find_element(By.XPATH, ".//span[@class = 'd-inline-block align-middle fs--1']")
                        effective_date = date_source.find_element(By.TAG_NAME, "span").text
                        download_date = date.today()
                        

                        try:
                            pdf_response = requests.get(pdf_url)
                            pdf_response.raise_for_status()
                            
                            with open(pdf_path, 'wb') as pdf_file:
                                pdf_file.write(pdf_response.content)

                            hash_code = calculate_file_hash(pdf_path)
                            metadata.append({
                                "ISIN": configured_isin,
                                "DocumentType": document_type,
                                "EffectiveDate": effective_date,
                                "DownloadDate": download_date,
                                "DownloadURL": pdf_url,
                                "FilePath": pdf_path,
                                "MD5Hash": hash_code,
                                "FileSize": len(pdf_response.content)
                            })

                            print(f"Downloaded {pdf_name} for ISIN {configured_isin}")

                        except requests.RequestException as e:
                            print(f"Failed to download {pdf_url}: {e}")
                time.sleep(2)

        li_button = driver.find_element(By.XPATH, "//li[@class='paginate_button page-item next']")
        next_button = li_button.find_element(By.TAG_NAME, "a")
        driver.execute_script("arguments[0].scrollIntoView();", next_button)
        driver.execute_script("arguments[0].click();", next_button)
        if "disabled" in li_button.get_attribute("class"):
            print("We reached last page.")
            break

        time.sleep(5)

    driver.quit()

    if metadata:
        print("Metadata")
        if os.path.exists(METADATA_CSV):
            existing_metadata = pd.read_csv(METADATA_CSV)
            metadata_df = pd.DataFrame(metadata)
            combined_metadata = pd.concat([existing_metadata, metadata_df], ignore_index=True)
        else:
            combined_metadata = pd.DataFrame(metadata)

        combined_metadata.to_csv(METADATA_CSV, index=False)
        print(f"Metadata saved to {METADATA_CSV}")

#Create job 
def job():
    print(f"Job started: {datetime.now()}")
    download_pdfs()

# Schedule the job to run once a day
schedule.every().day.at("00:00").do(job)

print("Service started.")
# Run the scheduler loop
while True:
    schedule.run_pending()
    time.sleep(1)
        