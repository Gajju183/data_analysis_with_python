import pandas as pd
import numpy as np

df = pd.read_excel('C:/Users/Gajraj Singh/Raw.xlsx')

df['Event Time Parsed'] =  pd.to_datetime(df['Event Time'])
df['just_date'] = df['Event Time Parsed'].dt.date

df['New Site Id'] = df['Site ID'].apply(lambda x:x.split("%")[0])

df[['just_date','Event Name','New Site Id' ]]

df2 = df[['just_date','Event Name','New Site Id' ]]

df2.groupby(['just_date', 'New Site Id'])['Event Name'].apply(lambda y: y.count())

df2 = Df2.pivot_table( values='Event Name', index=['just_date','New Site Id' ],columns=['Event Name'], aggfunc=lambda x : len(x))

df3 = df2.fillna(0)

df3.to_excel('C:/Users/Gajraj Singh/output11.xlsx')
