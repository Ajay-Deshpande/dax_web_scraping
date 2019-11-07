from flask import Flask, render_template,jsonify
app = Flask(__name__)
import sqlite3
import pandas as pd
# import json
@app.route('/')
def profit_per_share():
    conn = sqlite3.connect('constituents.db')
    labels = pd.read_sql_query("select * from daily",conn)['constituent_name'].values
    data = pd.read_sql_query("select * from daily",conn)['profit_per_share'].values#.to_dict(orient='records')
    conn.close()
    return render_template('index.html',name="profit_per_share",labels = '-'.join(labels.tolist()),data=' '.join(map(str,data.tolist())))

@app.route('/historical')
def high_low():
    conn = sqlite3.connect('constituents.db')
    df = pd.read_sql_query("select * from historical where wkn='daimler-ag'",conn).sort_values('date')[['high','low','date']]
    labels = pd.to_datetime(df['date']).dt.strftime('%d/%m/%y').values.tolist()
    print(labels)
    labels = ' '.join(map(str,labels))
    high = ' '.join(map(str,df['high'].values.tolist()))
    low = ' '.join(map(str,df['low'].values.tolist()))
    conn.close()
    return render_template('historical.html',name="daimler-ag",labels=labels,high=high,low=low,high_name="High price",low_name="Low price")



@app.route('/yearly')
def yearly():
    conn = sqlite3.connect('constituents.db')
    # series = pd.read_sql_query('select * from yearly',conn).groupby('constituent_name')['sales_in_mio'].sum()
    # labels = series.index.tolist()
    # data = series.values.tolist()
    # print(str(test)=='{}'.format(test))
    # print(str(test))
    df = pd.read_sql_query('select * from yearly',conn).groupby('wkn')['number_of_employees','sales_in_mio'].sum()#.to_dict(orient='records')
    df.columns = ['x','y']
    wkn = ' '.join(df.index.tolist())
    test = df.to_dict(orient='records')
    conn.close()
    # return render_template('yearly.html',test=test,labels="-".join(labels),data=" ".join(map(str,data)))
    return render_template('yearly.html',test=test,wkn=wkn)

if __name__ == '__main__':
   app.run(debug = True)