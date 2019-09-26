from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd 
from time import sleep

driver = webdriver.Chrome('./chromedriver')

try:
    driver.maximize_window()
    driver.get('https://www.boerse-frankfurt.de/?lang=en')
    driver.find_element_by_xpath('//button[contains(text(),"Accept")]').click()
    search_bar = driver.find_element_by_xpath('//input')
    search_bar.clear()
    search_bar.send_keys("DAX")
    sleep(3)
    search_bar.send_keys(Keys.ENTER)
    print(driver.current_url)
    dax_page = driver.find_element_by_xpath('//a[contains(text(),"DAX")]').get_attribute('href')
    driver.get(dax_page)
    sleep(3)
    driver.find_element_by_xpath('//button[contains(text(),"Constituents")]').click()
    sleep(3)
    driver.find_element_by_xpath('//button[contains(@class,"page-bar-type-button") and contains(text(),"100")]').click()
    sleep(3)
    df = pd.read_html('<table>' + driver.page_source + '</table>')[0]
    df = df[['Name','WKN']]
    dic = df.to_dict(orient='records')
    for i in dic:
        driver.get('https://www.boerse-frankfurt.de/?lang=en')
        search_bar = driver.find_element_by_xpath('//input')
        search_bar.clear()
        search_bar.send_keys(i['WKN'])
        sleep(1)
        search_bar.send_keys(Keys.ENTER)
        sleep(2)
        i['ISIN'] = driver.current_url.split('/')[-1]
    print(dic)
    driver.quit()
except Exception as e:
    print(e)
    driver.quit()