import investpy
from datetime import date, timedelta, datetime
from database import connection
import pandas as pd
import numpy as np
from Model import Economic_Calendar_Data
import sys 
#today_d = date.today()
#end_date_d = today_d + timedelta(days=10)
def get_new_economic_calendar_data(today_d, end_date_d):

    today = today_d.strftime("%d/%m/%Y")
    end_date = end_date_d.strftime("%d/%m/%Y")
    
    df = investpy.economic_calendar(
        from_date=today,
        to_date  =end_date
    )
    
    
    today = today_d.strftime("%Y%m%d")
    end_date = end_date_d.strftime("%Y%m%d")
    df.to_excel(f'investopy_{today}_{end_date}.xlsx')
    return df


def tranfsorm_data(df, column):
    #clear actual column to new column
    
    #column = 'actual'
    df = df.dropna(subset=[column])
    if df.empty:
        return df
    df = df.reset_index()
    df = df.drop('index', 1)
    
    
    df = df.drop_duplicates()
    
    df[f'{column}temp'] = df[f'{column}']
    df[f'{column}_clear'] = df[f'{column}'].map(lambda x: x.rstrip('KMBLTk'))
    df[f'{column}'] = df[f'{column}temp']
    df = df.drop(f'{column}temp', 1)
    
    df[f'{column}_clear'] = df[f'{column}_clear'].str.replace(',', '')
    df.loc[~df[f'{column}_clear'].str.contains('%'), f'{column}_clear'] =  df[f'{column}_clear']
    df.loc[df[f'{column}_clear'].str.contains('%'), f'{column}_clear'] = df[f'{column}_clear'].str.rstrip('%').astype('float') / 100.0
    
    #transform data
    df.loc[df[column].str.contains('%'),'ispercentage'] = 1
    df.loc[~df[column].str.contains('%'),'ispercentage'] = 0
    
    df[f'{column}_clear'] = df[f'{column}_clear'].astype(float)
    df['forecast'] = df['forecast'].astype(str)
    df['previous'] = df['previous'].astype(str)
    df['actual'] = df['actual'].astype(str)
    
    df['event_clear'] = df['event'].str.replace(r"\(.*\)","")
    df['event_clear'] = df['event_clear'].str.rstrip()
    
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y').dt.date

    #df = df.drop('index', 1)
    return df

#engine = connection()

#s = "20220316"
#today_d = datetime.strptime(s, '%Y%m%d')
#end_date_d = today_d + timedelta(days=100)
today_d = date.today()
end_date_d = today_d + timedelta(days=1)
today_d = today_d + timedelta(days=-4)
df = get_new_economic_calendar_data(today_d, end_date_d)

    
df = tranfsorm_data(df, 'actual')

if df.empty:
    sys.exit('No Data')
    
for index, row in df.iterrows():
   
    engine = connection()
    date, zone, event, actual = row.date, row.zone, row.event, row.actual
    print ( date, zone, event, actual)
    exist = Economic_Calendar_Data.exist_row_in_db_n_update(Economic_Calendar_Data, date, zone, event, actual)
    print ('exist: ' + str(exist))
    if not exist:
        Economic_Calendar_Data.insert_new_row(Economic_Calendar_Data, row)
        print ('inserted' + row.id)
        

'''import investpy

search_result = investpy.search_quotes(text='apple', products=['stocks'],
                                       countries=['united states'], n_results=1)
print(search_result)'''