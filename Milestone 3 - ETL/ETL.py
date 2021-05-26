#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
import requests
import json
import time

# library for importing BLS statistics through the BLS.gov API: https://github.com/OliverSherouse/bls
import bls
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# import ETL functions
import etl_functions as etl


# In[2]:


start = time.time()


# In[3]:


# Connect to Google BigQuery
key_path = ''

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# In[4]:


###########
# Extract #
###########


# In[5]:


# Extract Chicago crime data
chicago_crime_data = etl.extract_chicago_crime_data()


# In[6]:


# Extract graduation data
cps_graduation_data = etl.extract_cps_graduation_data()


# In[7]:


# Extract unemployment data
chicago_unemployment_rate_data = etl.extract_chicago_unemployment_rate_data()


# In[8]:


# Extract weather data
chicago_weather_data = etl.extract_chicago_weather_data()


# In[9]:


##################
# DATA PROFILING #
##################


# In[10]:


# Profile Chicago crime data
chicago_crime_data_profile = etl.data_profiling_dataframe(chicago_crime_data)
print(chicago_crime_data_profile)


# In[11]:


# Profile graduation data
cps_graduation_data_profile = etl.data_profiling_dataframe(cps_graduation_data)
print(cps_graduation_data_profile)


# In[12]:


# Profile unemployment data
chicago_unemployment_rate_data_profile = etl.data_profiling_dataframe(chicago_unemployment_rate_data)
print(chicago_unemployment_rate_data_profile)


# In[13]:


# Profile weather data
chicago_weather_data_profile = etl.data_profiling_dataframe(chicago_weather_data)
print(chicago_weather_data_profile)


# In[14]:


#############
# TRANSFORM #
#############


# In[15]:


# Clean Chicago crime data
chicago_crime_data = etl.clean_chicago_crime_data(chicago_crime_data)


# In[16]:


# Clean graduation data
cps_graduation_data = etl.clean_cps_graduation_data(cps_graduation_data)


# In[17]:


# Clean unemployment data
chicago_unemployment_rate_data = etl.clean_chicago_unemployment_rate_data(chicago_unemployment_rate_data)


# In[18]:


# Clean weather data
chicago_weather_data = etl.clean_chicago_weather_data(chicago_weather_data)


# In[19]:


# Create date dimension
date_dim = etl.create_date_dimension()
print(date_dim.head())


# In[20]:


# Create crime_code dimension
crime_code_dim = etl.create_crime_code_dimension(chicago_crime_data)
print(crime_code_dim.head())


# In[21]:


# Create location dimension
location_dim = etl.create_location_dimension(chicago_crime_data)
print(location_dim.head())


# In[22]:


# Create CRIME_INCIDENT fact
crime_incident_fact = etl.create_crime_incident_fact(chicago_crime_data, location_dim, crime_code_dim)
print(crime_incident_fact.head())


# In[23]:


# Create CHICAGO_UNEMPLOYMENT fact
chicago_unemployment_fact = etl.create_chicago_unemployment_fact(chicago_unemployment_rate_data, date_dim)
print(chicago_unemployment_fact.head())


# In[24]:


# Create GRADUATION_RATE fact
graduation_rate_fact = etl.create_graduation_rate_fact(cps_graduation_data, date_dim)
print(graduation_rate_fact.head())


# In[25]:


# Create WEATHER fact table
weather_fact = etl.create_weather_fact(chicago_weather_data)
print(weather_fact.head())


# In[26]:


########
# LOAD #
########

# Load location dimension
etl.load_df_to_bigquery(df = location_dim, table_name = 'location_dim')
# Load crime_code dimension
etl.load_df_to_bigquery(df = crime_code_dim, table_name = 'crime_code_dim')
# Load date dimension
etl.load_df_to_bigquery(df = date_dim, table_name = 'date_dim')

# Load crime_incident fact
etl.load_df_to_bigquery(df = crime_incident_fact, table_name = 'crime_incident_fact')
# Load chicago_unemployment fact
etl.load_df_to_bigquery(df = chicago_unemployment_fact, table_name = 'chicago_unemployment_fact')
# Load graduation_rate fact
etl.load_df_to_bigquery(df = graduation_rate_fact, table_name = 'graduation_rate_fact')
# Load weather fact
etl.load_df_to_bigquery(df = weather_fact, table_name = 'weather_fact')


# In[27]:


end = time.time()
print("Total time for ETL in seconds:", end - start)

