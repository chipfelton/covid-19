# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## COVID-19 Data and Analyses

# %% [markdown]
# ### Get global cases and deaths data. Read into dataframe and Tableau hyper extract
# Data sources: https://ourworldindata.org/coronavirus-source-data, https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide

# %%
from datetime import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
import pantab

pd.options.display.max_columns = 100
pd.options.display.max_rows = 1000
# recode infinity values to NaN
pd.set_option('use_inf_as_na', True)


# %%
def get_data():
    d = datetime.today() - timedelta(days=1)
    fn = str(d.date()) + '-ourworldindata-org-covid-ecdc-full-data.csv'
    hyper_name = 'COVID-19.hyper'
    print('- Archiving yesterday\'s dataset as {}'.format(fn))
    # !cp 'ourworldindata-org-covid-ecdc-full-data.csv' {fn}
    print('- Downloading today\'s dataset')
    # !curl 'https://covid.ourworldindata.org/data/ecdc/full_data.csv' > 'ourworldindata-org-covid-ecdc-full-data.csv'
    print('- Reading data into dataframe and adding rolling calculations')
    df = pd.read_csv('ourworldindata-org-covid-ecdc-full-data.csv', parse_dates=['date'])
    
    # add calculations
    df.set_index('date', inplace=True)
    countries = df['location'].unique()
    frames = []
    for c in countries:
        # total cases related
        temp_df = df.loc[df['location'] == c].copy()
        temp_df['total_cases_7day_avg'] = temp_df['total_cases'].rolling(7).mean()
        temp_df['total_cases_7day_pct_change'] = temp_df['total_cases_7day_avg'].pct_change()
        temp_df['total_cases_doubling_days'] = temp_df['total_cases_7day_pct_change'].apply(lambda x: np.log(2.0) / 
                                                                                        np.log(1.0 + x))
        # total deaths related
        temp_df['total_deaths_7day_avg'] = temp_df['total_deaths'].rolling(7).mean()
        temp_df['total_deaths_7day_pct_change'] = temp_df['total_deaths_7day_avg'].pct_change()
        temp_df['total_deaths_doubling_days'] = temp_df['total_deaths_7day_pct_change'].apply(lambda x: np.log(2.0) / 
                                                                                        np.log(1.0 + x))
        frames.append(temp_df)
    df = pd.concat(frames)
    df.reset_index(inplace=True)    
    
    print('- Exporting dataframe as hyper extract {}'.format(hyper_name))
    pantab.frame_to_hyper(df, hyper_name, table='ECDC Worldwide (via Our World in Data.org)')
    print('- Returning dataframe for further use\n')
    
    return df

df = get_data()
df.loc[(df['location'] == 'New Zealand')].sort_values(by=['date'], ascending=False).head()

# %%
df['location'].value_counts()

# %% [markdown]
# ### Get ECDC COVID-19 geographic distribution dataset
# Data source: https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide

# %%
import pandas as pd
url='https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/'
csv='ecdc-covid19-casedistribution.csv'
# !curl --silent {url} > {csv}
df2=pd.read_csv(csv, encoding='ISO-8859–1')
df2.loc[ df2['countriesAndTerritories'].str.contains('Zealand')]

# %%
df2.dtypes

# %%
pwd

# %%
import pandas as pd
url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
csv = 'nyt-covid19-us-counties.csv'
# !curl  {url} > {csv}
nyt_df = pd.read_csv(csv)
nyt_df.head()

# %%
nyt_df.dtypes

# %%
url = 'https://data.world/covid-19-data-resource-hub/covid-19-case-counts/workspace/file?filename=COVID-19+Cases.hyper'
hyper = 'tableau-covid19.hyper'
# !curl {url} > {hyper}
tableau_df = pantab.frame_from_hyper(hyper, table='Extract')
tableau_df.head()

# %%
pantab.frame_from_hyper(hyper, table='Extract')

# %%
# !pwd

# %% [markdown]
# ### FAO Stat database

# %%
# get database catalogue
catalog_url = 'http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json'
catalog = pd.DataFrame.from_records(pd.read_json('/Users/chip/Downloads/datasets_E.json', encoding='mac_roman').loc['Dataset'][0])
catalog

# %%
download = catalog.loc[ catalog['DatasetCode'] == 'LC', 'FileLocation'].item()

# %%
# !curl '{download}' > 'Environment_LandCover_E_All_Data_(Normalized).zip'

# %%
# !unzip 'Environment_LandCover_E_All_Data_(Normalized).zip'

# %%
data = pd.read_csv('Environment_LandCover_E_All_Data_(Normalized).csv', encoding='mac_roman')
data

# %%
data[ data['Area'] == 'New Zealand']

# %%
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

engine = create_engine(URL(
    account = 'sb87483.australia-east.azure',
    user = 'm0ntage',
    password = 'Montage1989!',
    database = 'chips_db',
    schema = 'public',
    warehouse = 'compute_wh',
    role='sysadmin',
))
# Specify that the to_sql method should use the pd_writer function
# to write the data from the DataFrame to the table named "customers"
# in the Snowflake database.

# %%
snow_table = 'ourworldindata_org_covid_ecdc_full_data'

# %%
# %%time
# fastest method
with engine.connect() as conn:
    #display(df.head())
    df.to_sql(snow_table, conn, if_exists='replace', index=False, chunksize=10000)    

# %%
# %%time
with engine.connect() as conn:
    #display(df.head())
    df.to_sql(snow_table, conn, if_exists='replace', index=False, chunksize=10000, method='multi')    

# %%
with engine.connect() as conn:
    covid = pd.read_sql(snow_table, conn)
    display(covid.loc[(covid['location'] == 'New Zealand')].sort_values(by=['date'], ascending=False))

# %%
# test of reading directly from azure blob
import requests
import gzip 
url = 'https://montageonlinestorage.blob.core.windows.net/montage-online-data-lake/nelmac/bi_hsevents.json.gz?sp=r&st=2020-10-06T07:57:53Z&se=2025-10-06T15:57:53Z&spr=https&sv=2019-12-12&sr=b&sig=bsZYYIDqn5RTLF8hEXmeen65TBxDTCLQZMqUpZSYgIE%3D'

display(pd.read_json(gzip.decompress(requests.get(url).content).decode('utf-8-sig'), lines=True))

# %%
