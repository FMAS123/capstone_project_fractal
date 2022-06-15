#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from sqlalchemy import create_engine


# In[ ]:


engine = create_engine('postgresql://postgres:Krishna839#@34.122.187.243:5432/postgres')


# In[ ]:


dfc= pd.read_sql_query('select * from "customer_master"', con=engine)
dfp= pd.read_sql_query('select * from "product_master"', con=engine)
dfod= pd.read_sql_query('select * from "order_details"', con=engine)
dfoi= pd.read_sql_query('select * from "order_items"', con=engine)


# In[ ]:


dfc


# In[ ]:


dfp


# In[ ]:


dfod


# In[ ]:


dfoi.columns


# # dim_order

# In[ ]:


dim_order = dfod[['orderid', 'order_status_update_timestamp', 'order_status']]


# In[ ]:


dim_order


# In[ ]:


len(dfc)


# In[ ]:





# # dim_address

# In[ ]:


from collections import defaultdict

dim_address =  defaultdict(list)


# In[ ]:


for i in range(0,len(dfc)):
    dim_address['address_id'].append(i) 
    dim_address['address'].append(dfc['address'][i])
    dim_address['city'].append(dfc['city'][i])
    dim_address['state'].append(dfc['state'][i])
    dim_address['pincode'].append(dfc['pincode'][i])
    
    


# In[ ]:


dim_address


# In[ ]:


df_dim_address = pd.DataFrame(dim_address)


# In[ ]:


df_dim_address


# # dim_customer

# In[ ]:


dim_customer =  defaultdict(list)


# In[ ]:


for i in range(0,len(dfc)):
    dim_customer['customerid'].append(dfc['customerid'][i]) 
    dim_customer['name'].append(dfc['name'][i])
    dim_customer['address_id'].append(df_dim_address['address_id'][i])
    dim_customer['start_date'].append(dfc['update_timestamp'][i])
    dim_customer['end_date'].append('NULL')


# In[ ]:


df_dim_customer = pd.DataFrame(dim_customer)


# In[ ]:


df_dim_customer['start_date'] = pd.to_datetime(df_dim_customer['start_date']).dt.date


# In[ ]:


df_dim_customer


# In[ ]:





# # dim_product

# In[ ]:


#	productid	productcode	productname	sku	rate	isactive	start_date	end_date


# In[ ]:


dim_product = defaultdict(list)


# In[ ]:


for i in range(0,len(dfp)):
    dim_product['productid'].append(dfp['productid'][i])
    dim_product['productcode'].append(dfp['productcode'][i])
    dim_product['productname'].append(dfp['productname'][i])
    dim_product['sku'].append(dfp['sku'][i])
    dim_product['rate'].append(dfp['rate'][i])
    dim_product['isactive'].append(dfp['isactive'][i])
    dim_product['start_date'].append(dfc['update_timestamp'][i])
    dim_product['end_date'].append('NULL')
    


# In[ ]:


df_dim_product = pd.DataFrame(dim_product)


# In[ ]:


df_dim_product['start_date'] = pd.to_datetime(df_dim_customer['start_date']).dt.date


# In[ ]:


df_dim_product


# In[ ]:


# for df_dim_product['end_date'] in df_dim_product:
#     if df_dim_product['isactive'][i] == 'False':
        
#         dim_product['end_date'].append(dfc['update_timestamp'][i])
        
df_dim_product.loc[df_dim_product['isactive']== 'False', 'end_date']= df_dim_product['start_date'][i]


# In[ ]:


df_dim_product


# In[ ]:


import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes
from pandas.io import gbq


# In[ ]:


credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/googlekey.json')


# In[ ]:


dim_order.to_gbq(destination_table='cloudsql_to_bq.dim_order',project_id='capstone-9-352804',if_exists='append', credentials = credentials)


# In[ ]:


df_dim_address.to_gbq(destination_table='cloudsql_to_bq.dim_address',project_id='capstone-9-352804',if_exists='append', credentials = credentials)


# In[ ]:


df_dim_customer.to_gbq(destination_table='cloudsql_to_bq.dim_customer',project_id='capstone-9-352804',if_exists='append', credentials = credentials)


# In[ ]:


df_dim_product.to_gbq(destination_table='cloudsql_to_bq.dim_product',project_id='capstone-9-352804',if_exists='append', credentials = credentials)


# In[ ]:





# In[ ]:




