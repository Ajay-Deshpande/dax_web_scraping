from datetime import datetime
import pandas as pd

class Cleaner():
    """
    Cleans and formats the dataframe.
    """
    def __init__(self,df):
        """
        Argument - List of dataframes containing the tabular information scraped from web
        """
        self.data = df
    def clean(self):
        lst = ['News about','Performance','Master data','Trading parameters Frankfurt','Related indices','Contact','Shareholder structure','Corporate information','Dividend information','Investor relation events','Company reports','']
        ## To remove non-tabular or unnecessary data.
        self.data = {i : self.data[i] for i in self.data if i not in lst}
        for i in self.data:
            if i == 'Historical key data':
                ## The data is pivoted around the Year column
                self.data[i] = self.data[i].T.reset_index()
                self.data[i]['index'].iloc[0] = 'Year'
                self.data[i].columns = self.data[i].iloc[0]
                self.data[i] = self.data[i].iloc[1:]
            if i != 'Historical key data'and i != 'Historical prices and volumes':
                self.data[i] = self.data[i].T
                self.data[i].columns = self.data[i].iloc[0]
                self.data[i] = self.data[i].iloc[1:]        
            try:
                ## The data is standardized. Column names are converted to lower case, \
                ## non-ascii characters are removed, \
                ## the currency unit it is removed since all units are in Euro.
                self.data[i].columns = self.data[i].columns.str.replace(r'[^a-zA-Z0-9\s]*', '', regex=True).str.strip()
                self.data[i].columns = self.data[i].columns.str.strip().str.replace(r'[\s]+', '_', regex=True).str.strip()
                self.data[i].columns = self.data[i].columns.str.lower().str.strip()
                self.data[i].columns = self.data[i].columns.str.replace(r'_in$','',regex=True).str.strip()
            except:
                pass
            finally:
                for j in self.data[i].columns:
                    try:
                        if j == 'year':
                            raise
                        ## If the values in a column are numeric its converted to float.
                        self.data[i][j] = self.data[i][j].apply(float)
                    except:
                        pass
                    try:
                        ## Standardizing values and date formats.
                        self.data[i][j] = self.data[i][j].str.replace(r'm$|bn$','',regex = True).str.strip()
                        self.data[i][j] = self.data[i][j].apply(lambda x : datetime.strptime(x[:-2] + '20' + x[-2:],'%d/%m/%Y'))
                    except:
                        pass
                self.data[i] = self.data[i].apply(lambda col:pd.to_numeric(col,errors='ignore'),axis=1)

        return self.preprocessing()

    def preprocessing(self):
        ## The tabular data collected is categorized based on frequency of change or frequency o collection.
        mapper = {'daily' : ['Price information','Fundamentals','Technical key data'],'yearly':['Historical key data'],'historical':['Historical prices and volumes']}
        final_data = dict()
        for i in self.data:
            key = [x for x in mapper if i in mapper[x]][0]
            final_data[key] = pd.concat([self.data[i],final_data.get(key,pd.DataFrame())],axis = 1)
        return final_data