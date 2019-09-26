from time import sleep
from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.keys import Keys
from copy import copy
def indices_list(driver):
    url='https://www.investing.com/indices/major-indices'
    driver.get(url)
    sleep(5)
#        driver.find_element_by_xpath('//div[@class="ng-star-inserted"]/button[contains(text(),"100")]').click()
#        time.sleep(5)
    data={}
    table = driver.find_elements_by_tag_name('table')[0]
    rows=table.find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')
    for row in rows:
        column=row.find_elements_by_tag_name('td')[1]
        name=column.text
        if(name in data.keys()):
            continue
        link=column.find_element_by_tag_name('a').get_attribute('href')
        data[name]=link
    return data
def get_component_list(driver,indices):
    list_of_indices=list(indices.keys())
    list_of_indices.reverse()
    for name in list_of_indices:
        print("\nExtracting {}".format(name),end='')
        link=indices[name]
        driver.get(link)
        sleep(5)
        lists=driver.find_element_by_xpath('//*[@id="pairSublinksLevel2"]')
        comp_found=False
        for l in lists.text.split():
            if(l=='Components'):
                comp_found=True
        if(comp_found):
            link+='-components'
        driver.get(link)
        sleep(5)
        if(driver.find_elements_by_xpath('//*[@id="cr1"]')):
            print('   - Constituents Found',end='')
            df=pd.DataFrame()
            while(driver.find_elements_by_xpath('//*[@id="paginationWrap"]/div[3]/a')):
                print('next present')
                link=driver.find_element_by_xpath('//*[@id="paginationWrap"]/div[3]/a').get_attribute("href")
                driver.get(link)
                sleep(5)
                table=driver.find_element_by_xpath('//*[@id="cr1"]')
                df2 = pd.read_html('<table>'+table.get_attribute('innerHTML')+'</table>')[0]['Name']
                print(df2.shape)
                df=df.append(df2.T)
            table=driver.find_element_by_xpath('//*[@id="cr1"]')
            df2 = pd.read_html('<table>'+table.get_attribute('innerHTML')+'</table>')[0]['Name']
            df=df.append(df2.T)
            print('\n',df)
            df.to_excel(writer,sheet_name=name)
writer = pd.ExcelWriter('major_indices.xlsx', engine='xlsxwriter')
options  = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome('/home/user/Downloads/chromedriver_linux64/chromedriver',options=options)
indices=indices_list(driver)
get_component_list(driver,indices)
writer.save()
