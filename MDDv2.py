#!/usr/bin/env python
# coding: utf-8

# In[230]:


import numpy as np
import pandas as pd
from datetime import datetime
import datetime as dt
import math
import json


# In[231]:


df = pd.read_csv(r'C:\Users\Anup\Documents\Python Scripts\MDD\TIC.csv')


# In[232]:


df.head()


# In[233]:


df.dtypes


# In[234]:


test = '12/31/2019'
test2 = datetime.strptime(test,'%m/%d/%Y').date()
test3 = test2 - dt.timedelta(days=31)
print(test3)


# In[235]:


df['AsOfDate'] = df['AsOfDate'].apply(lambda x: datetime.strptime(x,'%m/%d/%Y').date())


# In[236]:


df['MaxDate'] = df['RSSD'].apply(lambda x: df['AsOfDate'].max())


# In[237]:


df#.head()


# In[238]:


df['MaxDateFlag'] = df['AsOfDate'] == df['MaxDate']


# In[239]:


df = df.sort_values(by=['RSSD','AsOfDate'],ascending=[True,False])


# In[240]:


df.head()


# In[241]:


dfpv = df.pivot_table(values='Cell_Value',columns='AsOfDate',index=['RSSD','Country','Product'])


# In[242]:


dfpv


# In[243]:


dfpv['var'] = dfpv[dfpv.columns[-1]] - dfpv[dfpv.columns[-2]]


# In[244]:


dfpv


# In[245]:


dfpv.columns


# In[246]:


dfpv4 = dfpv['var']


# In[247]:


dfpv4


# In[248]:


df2 = pd.merge(df,dfpv4,how='left',on=['RSSD','Country','Product'])


# In[249]:


df2


# In[250]:


dfpv.stack()


# In[251]:


dfpv2 = df.set_index(['RSSD','Country','Product'])


# In[252]:


dfpv3 = df.unstack(level=5)


# In[253]:


dfpv2


# In[254]:


df2.head()


# In[255]:


weights_dict = {1:0.97,2:0.9,3:0.7,4:0.5}


# In[256]:


dt_test = pd.datetime.today().date()


# In[257]:


df_test2 = dt_test - dt.timedelta(days=730)
print(df_test2)


# In[258]:


dt2 = pd.DataFrame(pd.date_range(start=df_test2,periods=24,freq='M'),columns=['Date'])


# In[259]:


dt2['Ind'] = dt2.index+1


# In[260]:


dt2['Group'] = dt2['Ind'].apply(lambda x: 'Group1' if x < 5 else 'Group2')


# In[261]:


def groups(x):
    if x < 7:
        return '1'
    elif x < 13:
        return '2'
    elif x < 19:
        return '3'
    else:
        return '4'


# In[262]:


dt2['Group2'] = dt2.apply(lambda x: groups(x['Ind']),axis=1)


# In[263]:


dt2['Date'] = dt2['Date'].apply(lambda x: x.date())
dt2


# In[264]:


dt2.dtypes


# In[265]:


df_group = pd.merge(df2,dt2,how='left',left_on='AsOfDate',right_on='Date')


# In[266]:


df_dates = pd.DataFrame(df.groupby(by='AsOfDate'),columns=['AsOfDate',''])


# In[267]:


df_dates = pd.DataFrame(df_dates['AsOfDate'])


# In[268]:


df_dates.sort_values(by='AsOfDate', ascending=False).reset_index()


# In[269]:


df_group.head(2)


# In[270]:


def cell_value_times_weight(group):
    if group==4:
        return 0.9
    elif  group==3:
        return 0.8
    else:
        return 0.7


# In[271]:


df_group['weighted_value'] = df_group.apply(lambda x: cell_value_times_weight(x['Group2']),axis=1)


# In[272]:


df_group.head()


# In[273]:


df_group['weighted_cell_value'] = df_group['Cell_Value']*df_group['weighted_value']


# In[274]:


df_group.head()


# In[275]:


df_group['curr_val_flag'] = df_group['AsOfDate']==df_group['MaxDate']


# In[276]:


df_group[df_group['curr_val_flag']==False].groupby(by=['RSSD','Country','Product'])


# In[277]:


#df_temp.rename(columns={'weighted_cell_value':'weighted_mean'})
df_temp = df_group.groupby(by=['RSSD','Country','Product']).agg({'weighted_cell_value':'mean'})


# In[278]:


df_temp


# In[279]:


df_group = pd.merge(left=df_group,right=df_temp,how='left',on=['RSSD','Country','Product'])


# In[280]:


df_group.head()


# In[281]:


df_group['variance'] = (((df_group['Cell_Value'] - df_group['weighted_cell_value_y'])**2)/24)
#df_group.dtypes


# In[282]:


df_group['stddev'] = df_group['variance'].apply(lambda x: math.sqrt(x))


# In[283]:


df_group.head()


# In[284]:


df_group['z'] = (df_group['Cell_Value'] - df_group['weighted_cell_value_y'])/df_group['stddev']


# In[285]:


df_group.head()


# In[286]:


df_curr = df[df['MaxDateFlag']==True]


# In[287]:


df_curr.head()


# In[288]:


df_curr = pd.merge(left=df_curr, right=df_temp, how='left', on=['RSSD','Country','Product'])


# In[289]:


df_curr


# In[290]:


df_curr.rename(columns={'weighted_cell_value':'weighted_mean'},inplace=True)


# In[291]:


#df_curr.drop(columns=['weighted_mean'], inplace=True)


# In[292]:


df_curr


# In[293]:


df_curr['variance'] = ((df_curr['Cell_Value'] - df_curr['weighted_mean'])**2)/24
df_curr


# In[294]:


df_curr['stddev'] = df_curr['variance'].apply(lambda x: math.sqrt(x))
df_curr


# In[295]:


df_curr['z'] = (df_curr['Cell_Value'] - df_curr['weighted_mean'])/df_curr['stddev']
df_curr


# In[296]:


df.head()


# In[297]:


df_1000 = df


# In[298]:


df_1000['Std'] = df_1000['Cell_Value'].apply(lambda x: x*4)
df_1000.head()


# In[299]:


df_1000['Cell_Value'].std()


# In[305]:


df_1000.to_csv(r'C:\Users\Anup\Documents\Python Scripts\MDD\output.csv')


# In[300]:


js = df_1000.head().to_json(orient='table', date_format='iso',index=False)
js


# In[301]:


js.find('"data"')


# In[302]:


len(js)


# In[303]:


js[js.find('"data"')+9:]


# In[ ]:




