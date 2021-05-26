from datetime import datetime
import requests
import json

# library for importing BLS statistics through the BLS.gov API: https://github.com/OliverSherouse/bls
import bls
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

# import ETL functions
import etl_functions as etl

##################################################

# Set tokens
noaa_token = ''
bls_api_key = ''

##################################################

# Connect to Google BigQuery
key_path = ''

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

##################################################

# Extract Chicago crime data
def extract_chicago_crime_data():
    query = '''
            SELECT
            
            -- crime_incident fact table
            arrest,
            domestic,
            description,

            -- crime_code dimension
            primary_type,
            fbi_code,

            -- location dimension
            location_description,
            community_area,
            district,
            ward,
            beat,
            block,
            latitude,
            longitude,
            x_coordinate,
            y_coordinate,

            -- date dimension
            date

            FROM `bigquery-public-data.chicago_crime.crime`
            WHERE date >= '2011-01-01' AND
                  date <  '2020-01-01'
            ;
            '''

    chicago_crime_df = client.query(query).to_dataframe()

    if len(chicago_crime_df) > 0:
        print(len(chicago_crime_df), 'crime records extracted from Google BigQuery public datasets: Chicago Crime Data')
        return chicago_crime_df
    else:
        print('DATA EXTRACTION FAILED: Google BigQuery public datasets: Chicago Crime Data')
        
##################################################

# Extract Chicago Public Schools graduation data
def extract_cps_graduation_data():
    try:
        # Excel file located on this site: https://www.cps.edu/about/district-data/metrics/

        url = 'https://www.cps.edu/globalassets/cps-pages/about/district-data/metrics/metrics_cohortgraduationdropoutadjusted_citywide_2011to2019.xls'
        resp = requests.get(url)

        grad_data_excel_file = pd.ExcelFile(resp.content)
        graduation_data_raw = pd.read_excel(grad_data_excel_file, 'Citywide 4 Year Cohort Rates')
        
        # rename the columns
        temp_list = []
        for i in range(graduation_data_raw.shape[1]):
            temp_list.append(i)
        graduation_data_raw.columns = temp_list

        # create a dataframe from the graduation rates by year
        year_list = []
        rate_list = []
        for i in range(11,20):
            year_list.append(graduation_data_raw[i][1])
            rate_list.append(graduation_data_raw[i][2])

        graduation_rates = pd.DataFrame(rate_list, columns=['grad_rate'], index=year_list)

        print("Chicago Public Schools graduation data successfully extracted.")
        return graduation_rates
 
    except:
        print("ERROR: Could not extract Chicago Public Schools graduation data.")
        
##################################################

# Identify the Series ID code using these resources: https://www.bls.gov/help/hlpforma.htm#LA
# Local Area Unemployment Rate for Chicago Series ID: LAUCT171400000000003

# Extract Chicago unemployment rate data from BLS.gov
def extract_chicago_unemployment_rate_data():
    try:
        chicago_unemployment_rate = bls.get_series('LAUCT171400000000003', 2011, 2019, bls_api_key)
        print('Chicago Unemployment rate data successfully extracted.')
        urate_df = pd.DataFrame(chicago_unemployment_rate)
        urate_df.columns = ['unemployment_rate']
        return urate_df
    except:
        print("ERROR: Could not extract Chicago unemployment rate data.")
        
##################################################

# Extract Chicago weather data from NOAA through their API
def extract_chicago_weather_data():
    try:
        # Weather data retrieved using the method described here:
        # https://towardsdatascience.com/getting-weather-data-in-3-easy-steps-8dc10cc5c859
        # Weather station URL: https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USC00115097/detail

        #initialize lists to store data
        dates_temp = []
        temps = []
        dates_prcp = []
        prcps = []

        # retrieve temperature observations
        for year in range(2011, 2020):
            year = str(year)

            # make the api call
            r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=TOBS&limit=1000&stationid=GHCND:USC00115097&startdate='+year+'-01-01&enddate='+year+'-12-31', headers={'token':noaa_token})

            # load the api response as a json
            d = json.loads(r.text)    

            # get all items in the response which are observed temperature readings
            obs_temps = [item for item in d['results'] if item['datatype']=='TOBS']

            # get the date field from all observed temperature readings
            dates_temp += [item['date'] for item in obs_temps]

            # get the actual observed temperature from all average temperature readings
            temps += [item['value'] for item in obs_temps]
            
            print('Extracting temperature data for ' + year + '.')
            

        # initialize dataframe
        df_temps = pd.DataFrame()

        # populate date and observed temperature fields
        # cast string date to datetime and convert temperature from tenths of Celsius to Fahrenheit
        df_temps['date'] = [datetime.strptime(d, "%Y-%m-%dT%H:%M:%S") for d in dates_temp]
        df_temps['temp'] = [float(v)/10.0*1.8 + 32 for v in temps]
        
  
        # retrieve preciptation observations
        for year in range(2011, 2020):
            year = str(year)

            # make the api call
            r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=PRCP&limit=1000&stationid=GHCND:USC00115097&startdate='+year+'-01-01&enddate='+year+'-12-31', headers={'token':noaa_token})

            # load the api response as a json
            d = json.loads(r.text)    
            
            # get all items in the response which are precipitation readings
            obs_prcps = [item for item in d['results'] if item['datatype']=='PRCP']

            # get the date field from all precipitation readings
            dates_prcp += [item['date'] for item in obs_prcps]

            # get the actual observed precipitation from all precipitation readings
            prcps += [item['value'] for item in obs_prcps]
            
            print('Extracting precipitation data for ' + year + '.')
            

        # initialize dataframe
        df_prcp = pd.DataFrame()

        # populate date and observed temperature fields
        # cast string date to datetime and convert precipitation from mm to inches
        df_prcp['date'] = [datetime.strptime(d, "%Y-%m-%dT%H:%M:%S") for d in dates_prcp]
        df_prcp['prcp'] = [float(v)/25.4 for v in prcps]
        
        # join temperature and precipitation dataframes
        df_weather = df_temps.merge(df_prcp, on='date', how='inner')
    
        # set date as the index
        df_weather.index = df_weather.date
        df_weather = df_weather.drop('date', axis=1)
    
        return df_weather
    
    except:
        print("ERROR: Could not extract Chicago weather data.")

##################################################
        
# Create data profiling function for extracted data
# Function written and provided by Professor Michael O'Donnell
def data_profiling_dataframe(df):
    # Get memory used by each column in the raw data dataset in MB
    mem_used_dtypes = pd.DataFrame(df.memory_usage(deep=True) / 1024**2)
    df_mem = df.memory_usage(deep=True).sum() / 1024**2
    file_size = float('nan')
    sz_increase = ((df_mem - file_size) / file_size)
    
    # Rename column
    mem_used_dtypes.rename(columns={ 0:'memory'}, inplace=True)
    
    # Drop index memory usage since this is not required when merging with Data Quality Dataframe
    mem_used_dtypes.drop('Index', axis=0, inplace=True)
    
    
    # define number of columns in dataframe
    no_of_rows = len(df.columns)
    # Constructing the data_qlt_df dataframe and pre-assigning and columns
    data_qlt_df = pd.DataFrame(index=np.arange(0, no_of_rows),
                                columns=('column_name', 'col_data_type',
                                         'col_memory','non_null_values',
                                         'unique_values_count', 'column_dtype')
                              )

    # Add rows to the data_qlt_df dataframe
    for ind, cols in enumerate(df.columns):
        # Count of unique values in the column
        col_unique_count = df[cols].nunique()
        
        data_qlt_df.loc[ind] = [cols,
                                df[cols].dtype,
                                mem_used_dtypes['memory'][ind],
                                df[cols].count(),
                                col_unique_count,
                                cols + '~'+ str(df[cols].dtype)
                                ]
        
    # Use describe() to get column stats of raw dataframe
    raw_num_df = df.describe().T.round(2)
    
    # Merging the df.describe() output with rest of the info to create a single Data Profile Dataframe
    data_qlt_df = pd.merge(data_qlt_df, raw_num_df, how='left',
                           left_on='column_name', right_index=True)
    
    # Calculate percentage of non-null values over total number of values
    data_qlt_df['%_of_non_nulls'] = (data_qlt_df['non_null_values']/df.shape[0])*100
    
    # Calculate null values for the column
    data_qlt_df['null_values'] = df.shape[0] - data_qlt_df['non_null_values']
    
    # Calculate percentage of null values over total number of values
    data_qlt_df['%_of_nulls'] = 100 - data_qlt_df['%_of_non_nulls']
    
    # Calculate percentage of each column memory usage compared to total memory used by raw data datframe
    data_qlt_df['%_of_total_memory'] = data_qlt_df['col_memory'] / data_qlt_df['col_memory'].sum() * 100
    
    # Calculate the total memory used by a given group of data type
    # See Notes section at the bottom of this notebook for advatages of using 'transform' function with group_by
    #data_qlt_df["dtype_total"] = data_qlt_df.groupby('col_data_type')["col_memory"].transform('sum')
    data_qlt_df["dtype_total"] = "NA"
    
    # Calculate the percentage memory used by each column data type compared to the total memory used by the group of data type
    # the above can be merged to one calculation if we do not need the total as separate column
    #data_qlt_df["%_of_dtype_mem2"] = data_qlt_df["Dtype Memory"] / (data_qlt_df.groupby('Data Type')["Dtype Memory"].transform('sum')) * 100
    data_qlt_df["%_of_dtype_mem"] = "NA"
    
    # Calculate the percentage memory used by each group of data type of the total memory used by dataset
    data_qlt_df["dtype_%_total_mem"] = "NA"
    
    # Calculate the count of each data type
    data_qlt_df["dtype_count"] = "NA"
    
    # Calculate the total count of column values
    data_qlt_df["count"] = data_qlt_df['null_values'] + data_qlt_df['non_null_values']

    # Reorder the Data Profile Dataframe columns
    data_qlt_df = data_qlt_df[
                                ['column_name', 'col_data_type', 'col_memory',
                                 '%_of_dtype_mem', '%_of_total_memory',
                                 'dtype_count', 'dtype_total', 'dtype_%_total_mem',
                                 'non_null_values', '%_of_non_nulls',
                                 'null_values', '%_of_nulls', 'unique_values_count',
                                 'count', 'mean', 'std', 'min', '25%',
                                 '50%', '75%', 'max']
                             ]
                                
    # drop unneeded columns
    for c in ['%_of_dtype_mem', 'dtype_count', 'dtype_total', 'dtype_%_total_mem']:
        data_qlt_df.drop(c, axis = 1, inplace=True)
                                
    return data_qlt_df

##################################################

# Clean Chicago crime data for the CRIME_INCIDENT fact
def clean_chicago_crime_data(df):
    print("Cleaning Chicago Crime data for the CRIME_INCIDENT fact.")   
    
    # drop rows with null values
    num_nulls = df.isnull().sum().sum()
    if num_nulls > 0:
        df = df.dropna()
        print(num_nulls, "null values dropped.")
    else:
        print("Chicago Crime data has no null values.")

    # drop duplicate values
    if len(df[df.duplicated()]) > 0:
        df.duplicated(keep = 'first')
        print(len(df[df.duplicated()])/2.0, "duplicate values dropped.")
    else:
        print("Chicago crime data has no duplicate rows.")

    print("Chicago crime data cleaning successful.")
    return df
    
##################################################

# Clean graduation data for the GRADUATION_RATE fact
def clean_cps_graduation_data(df):
    print("Cleaning Chicago Public Schools graduation data for the GRADUATION_RATE fact.")    
    
    # drop rows with null values
    num_nulls = df.isnull().sum().sum()
    if num_nulls > 0:
        df = df.dropna()
        print(num_nulls, "null values dropped.")
    else:
        print("Chicago Public Schools graduation data has no null values.")

    # drop duplicate values
    if len(df[df.index.duplicated()]) > 0:
        df.index.duplicated(keep = 'first')
        print(len(df[df.index.duplicated()])/2.0, "duplicate values dropped.")
    else:
        print("Chicago Public Schools graduation data has no duplicate rows.")

    print("Chicago Public Schools graduation data cleaning successful.")
    return df

##################################################
    
# Clean unemployment data for the CHICAGO_UNEMPLOYMENT fact
def clean_chicago_unemployment_rate_data(df):
    print("Cleaning unemployment data for the CHICAGO_UNEMPLOYMENT fact.")
        
    # drop rows with null values
    num_nulls = df.isnull().sum().sum()
    if num_nulls > 0:
        df = df.dropna()
        print(num_nulls, "null values dropped.")
    else:
        print("Chicago unemployment data has no null values.")

    # drop duplicate values
    if len(df[df.index.duplicated()]) > 0:
        df.index.duplicated(keep = 'first')
        print(len(df[df.index.duplicated()])/2.0, "duplicate values dropped.")
    else:
        print("Chicago unemployement data has no duplicate rows.")

    print("Chicago unemployement data cleaning successful.")
    return df
    
##################################################
    
# Clean weather data for the WEATHER fact
def clean_chicago_weather_data(df):
    print("Cleaning weather data for the WEATHER fact.")
        
    # drop rows with null values
    num_nulls = df.isnull().sum().sum()
    if num_nulls > 0:
        df = df.dropna()
        print(num_nulls, "null values dropped.")
    else:
        print("Chicago weather data has no null values.")

    # drop duplicate values
    if len(df[df.index.duplicated()]) > 0:
        df.index.duplicated(keep = 'first')
        print(len(df[df.index.duplicated()])/2.0, "duplicate values dropped.")
    else:
        print("Chicago weather data has no duplicate rows.")

    print("Chicago weather data cleaning successful.")
    return df

##################################################

# Create date dimension
# Function written and provided by Professor Michael O'Donnell
def create_date_dimension():
    sql_query = """
                  SELECT
                  CONCAT (FORMAT_DATE("%Y",d),FORMAT_DATE("%m",d),FORMAT_DATE("%d",d)) as date_id,
                  d AS full_date,
                  EXTRACT(YEAR FROM d) AS year,
                  EXTRACT(WEEK FROM d) AS year_week,
                  EXTRACT(DAY FROM d) AS year_day,
                  EXTRACT(YEAR FROM d) AS fiscal_year,
                  FORMAT_DATE('%Q', d) as fiscal_qtr,
                  EXTRACT(MONTH FROM d) AS month,
                  FORMAT_DATE('%B', d) as month_name,
                  FORMAT_DATE('%w', d) AS week_day,
                  FORMAT_DATE('%A', d) AS day_name,
                  (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END) AS day_is_weekday,
                FROM (
                  SELECT
                    *
                  FROM
                    UNNEST(GENERATE_DATE_ARRAY('2005-01-01', '2025-01-01', INTERVAL 1 DAY)) AS d )
                """
    
    # store extracted data in new dataframe
    date_df = client.query(sql_query).to_dataframe()
    
    # validate that the length of the dataframe is >0 and return dataframe
    if len(date_df) > 0:
        print("Date dimension created.")
        return date_df
    else:
        print("Date dimension creation failed.")

##################################################

# Create crime code dimension
def create_crime_code_dimension(chicago_crime_data):
    
    # get list of unique values in fields
    unique_codes = chicago_crime_data[['fbi_code', 'primary_type']].drop_duplicates()
    
    # reset the index and drop the index column
    unique_codes = unique_codes.reset_index().drop(columns='index').reset_index()

    # rename the columns
    unique_codes.columns=['code_id', 'fbi_code', 'primary_type']

    # change the code_id to start at 1000
    unique_codes.code_id = unique_codes.code_id + 1000

    print("Crime code dimension created.")
    return unique_codes

##################################################

# Create location dimension
def create_location_dimension(chicago_crime_data):
    
    # get list of unique values in fields
    unique_codes = chicago_crime_data[['location_description',
                                       'community_area',
                                       'district',
                                       'ward',
                                       'beat',
                                       'block',
                                       'latitude',
                                       'longitude',
                                       'x_coordinate',
                                       'y_coordinate']].drop_duplicates()
    
    # reset the index and drop the index column
    unique_codes = unique_codes.reset_index().drop(columns='index').reset_index()

    # rename the columns
    unique_codes.columns=['location_id',
                          'location_description',
                          'community_area',
                          'district',
                          'ward',
                          'beat',
                          'block',
                          'latitude',
                          'longitude',
                          'x_coordinate',
                          'y_coordinate']

    # change the code_id to start at 1000
    unique_codes.location_id = unique_codes.location_id + 1000

    print("Location dimension created.")
    return unique_codes

##################################################

# Create the CRIME_INCIDENT fact table
def create_crime_incident_fact(chicago_crime_data, location_dim, crime_code_dim):
    # merge the location dimension
    chicago_crime_data = chicago_crime_data.merge(location_dim,
                                                  on=['location_description',
                                                        'community_area',
                                                        'district',
                                                        'ward',
                                                        'beat',
                                                        'block',
                                                        'latitude',
                                                        'longitude',
                                                        'x_coordinate',
                                                        'y_coordinate'],
                                                        how='inner')

    # merge the crime_code dimension
    chicago_crime_data = chicago_crime_data.merge(crime_code_dim,
                                                 on=['fbi_code', 'primary_type'],
                                                 how='inner')

    # create the date_id column
    chicago_crime_data['date_id'] = chicago_crime_data['date'].apply(lambda x: x.strftime("%Y%m%d"))
    
    # keep only columns needed for the fact
    chicago_crime_data = chicago_crime_data[['date_id',
                                             'location_id',
                                             'code_id',
                                             'arrest',
                                             'domestic',
                                             'description']]

    print("CRIME_INCIDENT fact created.")
    return chicago_crime_data

##################################################

# Create CHICAGO_UNEMPLOYMENT fact table
def create_chicago_unemployment_fact(chicago_unemployment_rate_data, date_dim):
    # rest the index to turn date into a column
    chicago_unemployment_rate_data = chicago_unemployment_rate_data.reset_index()
    
    # create the date_id column
    chicago_unemployment_rate_data['date_id'] = chicago_unemployment_rate_data['date'].apply(lambda x: x.strftime("%Y%m%d"))
    
    # extract the month and year into their own columns
    chicago_unemployment_rate_data['month'] = chicago_unemployment_rate_data.date.apply(lambda x: datetime.strptime(str(x), "%Y-%m").month)
    chicago_unemployment_rate_data['year'] = chicago_unemployment_rate_data.date.apply(lambda x: datetime.strptime(str(x), "%Y-%m").year)
    
    # join the dataframes to apply the unemployment rate for each month and year to all days of that month and year
    chicago_unemployment_rate_data = chicago_unemployment_rate_data.merge(date_dim, on=['month', 'year'], how='inner')
    
    # select only needed columns and rename them
    chicago_unemployment_rate_data = chicago_unemployment_rate_data[['date_id_y', 'unemployment_rate']]
    chicago_unemployment_rate_data.columns=['date_id', 'unemployment_rate']
    
    print("CHICAGO_UNEMPLOYMENT fact created.")
    return chicago_unemployment_rate_data

##################################################

# Create GRADUATION_RATE fact table
def create_graduation_rate_fact(cps_graduation_data, date_dim):
    # reset the index to create the 'year' column
    cps_graduation_data = cps_graduation_data.reset_index()
    cps_graduation_data.columns=['year', 'grad_rate']

    # merge the date dimension on the year field to apply the graduation rate to all days that year
    cps_graduation_data = cps_graduation_data.merge(date_dim, on=['year'], how='inner')

    # select only needed columns
    cps_graduation_data = cps_graduation_data[['date_id', 'grad_rate']]

    print("GRADUATION_RATE fact created.")
    return cps_graduation_data

##################################################

# Create WEATHER fact table
def create_weather_fact(chicago_weather_data):
    # reset the index to create the date column
    chicago_weather_data = chicago_weather_data.reset_index()

    # create the date_id column
    chicago_weather_data['date_id'] = chicago_weather_data['date'].apply(lambda x: x.strftime("%Y%m%d"))

    # select only the columns needed
    chicago_weather_data = chicago_weather_data[['date_id', 'temp', 'prcp']]

    print("WEATHER fact created.")
    return chicago_weather_data

##################################################

# Create function for loading table to BigQuery
def load_df_to_bigquery(df, table_name):
    
    dataset_id = 'chicago-crime-project-311420'
    
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = "WRITE_TRUNCATE"
    
    upload_table_name = 'chicago_crime_project_data.'+str(table_name)
    
    load_job = client.load_table_from_dataframe(df, upload_table_name,
                                                job_config=job_config)
    
    print("Starting job {}".format(load_job))
