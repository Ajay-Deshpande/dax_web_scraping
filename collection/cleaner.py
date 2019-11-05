from datetime import datetime
import copy

class Cleaner():
    def __init__(self,df):
        self.data = df
        print('init')
    def clean(self):
        data = copy.deepcopy(temp)
        for i in data:
            if i == 'Performance':
                continue
            if i == 'Historical key data':
                data[i] = data[i].T.reset_index()
                data[i]['index'].iloc[0] = 'Year'
                data[i].columns = data[i].iloc[0]
                data[i] = data[i].iloc[1:]
            if i != 'Historical key data'and i != 'Historical prices and volumes':
                data[i] = data[i].T
                data[i].columns = data[i].iloc[0]
                data[i] = data[i].iloc[1:]        
            try:
                data[i].columns = data[i].columns.str.replace(r'[^a-zA-Z0-9\s]*', '', regex=True).str.strip()
                data[i].columns = data[i].columns.str.strip().str.replace(r'[\s]+', '_', regex=True)
                data[i].columns = data[i].columns.str.lower()
                data[i].columns = data[i].columns.str.replace(r'_in$','',regex=True)
            except:
                pass
            finally:
                for j in data[i].columns:
                    try:
                        data[i][j] = data[i][j].str.replace(r'm$|bn$','',regex = True).str.strip()
                        try:
                            data[i][j] = data[i][j].apply(float)
                        except:
                            pass
                        data[i][j] = data[i][j].apply(lambda x : datetime.strptime(x[:-2] + '20' + x[-2:],'%d/%m/%Y'))
                    except:
                        pass
        return data