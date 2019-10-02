import re
import pandas as pd
from time import sleep
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException

def getpath(driver,t):
    path=driver.execute_script("""gPt=function(c){
                                 if(c.id!==''){
                                     return'id("'+c.id+'")'
                                 }
                                 if(c===document.body){
                                     return c.tagName
                                 }
                                 var a=0;
                                 var e=c.parentNode.childNodes;
                                 for(var b=0;b<e.length;b++){
                                     var d=e[b];
                                     if(d===c){
                                         return gPt(c.parentNode)+'/'+c.tagName+'['+(a+1)+']'
                                     }
                                     if(d.nodeType===1&&d.tagName===c.tagName){
                                         a++
                                     }
                                 }
                             };
                             return gPt(arguments[0]).toLowerCase();""", t)
    if('/html/' not in path):
        return '/html/'+path
    else:
        return path
        
def get_tables(args,driver,all_tables={},no_parents=3):
    tables=driver.find_elements_by_tag_name("table")
    print("{} table(s) found".format(len(tables)))
    for table in tables:
            xpath=getpath(driver,table)
            header=''
            parent=xpath+'/..'
            for i in range(no_parents):
                #Loop finds if any header the is present in any parent of the table.
                #Upto 3 parents are searched. If no header is found, then
                try:
                    header=driver.find_element_by_xpath(parent).find_element_by_tag_name('h2' or 'h1')
                    break
                except NoSuchElementException as e:
                    #if header is not found, go one more level up
                    parent+='/..'
            if(header):
                #Find or create table headers
                if(table.find_elements_by_tag_name('thead')):
                    column_headings=[x.text for x in table.find_element_by_tag_name('thead').find_elements_by_tag_name('th')]
                else:
                    no_columns=len(table.find_element_by_tag_name('tr').find_elements_by_tag_name('td'))
                    column_headings=["Column "+str(i) for i in range(no_columns)]

                new_table=pd.DataFrame()
                rows=table.find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')

                for row in rows:
                    new_row={}
                    columns=row.find_elements_by_tag_name('td')
                    for i in range(len(columns)):
                        new_row[column_headings[i]]=columns[i].text
                        if(columns[i].find_elements_by_tag_name('a')):
                            #check if column has any hyperlink
                            new_row[column_headings[i]+" link"]=columns[i].find_element_by_tag_name('a').get_attribute("href")
                    new_table=new_table.append(new_row,ignore_index=True)
                final_header=re.sub(r'([^\s\w]|_)+', '', header.text)

                if(final_header in all_tables.keys()):
                    all_tables[final_header]=all_tables[final_header].append(new_table,ignore_index=True).drop_duplicates()
                else:
                    all_tables[final_header]=new_table
                print('\tExtracted {}'.format(final_header))
    return all_tables
'''args.logger.error(str(e), extra={"script_type": "housekeeping",
                        "operation": "collecting table's name ,xpath and number from DAX website and store to MySQL table",
                        "criticality": 3, "data_loss": "No data loss"})'''

def click_elements_and_get_tables(args,driver,clickable_elements,no_parents=3):
    all_tables={}
    if(isinstance(clickable_elements,list)):
       for clickable_element in clickable_elements:
           clickable_element.click()
           sleep(8)
           print('\n\nExtracting from {}'.format(clickable_element.text))
           all_tables=get_tables(args,driver,all_tables,no_parents)
    else:
        clickable_elements.click()
        print('\n\nExtracting from {}'.format(clickable_element.text))
        sleep(8)
        all_tables=get_tables(args,driver,all_tables,no_parents)
    return all_tables
