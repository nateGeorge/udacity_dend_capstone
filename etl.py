import os

import pandas as pd
import numpy as np

# Set up GCP API
from google.cloud import bigquery
# Construct a BigQuery client object.
client = bigquery.Client()



def load_lbnl_data(replace_nans=True):
    df1 = pd.read_csv('data/TTS_LBNL_public_file_10-Dec-2019_p1.csv', encoding='latin-1', low_memory=False)
    df2 = pd.read_csv('data/TTS_LBNL_public_file_10-Dec-2019_p2.csv', encoding='latin-1', low_memory=False)
    lbnl_df = pd.concat([df1, df2], axis=0)
    if replace_nans:
        lbnl_df.replace(-9999, np.nan, inplace=True)
        lbnl_df.replace('-9999', np.nan, inplace=True)
    
    return lbnl_df


def load_eia_iou_data():
    iou_df = pd.read_csv('data/iouzipcodes2017.csv')
    noniou_df = pd.read_csv('data/noniouzipcodes2017.csv')
    eia_zipcode_df = pd.concat([iou_df, noniou_df], axis=0)
    
    # zip codes are ints without zero padding
    eia_zipcode_df['zip'] = eia_zipcode_df['zip'].astype('str')
    eia_zipcode_df['zip'] = eia_zipcode_df['zip'].apply(lambda x: x.zfill(5))
    
    return eia_zipcode_df


def extract_lbnl_data():
    """
    Gets data from LBNL dataset for the installer table and main metrics table.
    """
    lbnl_df = load_lbnl_data(replace_nans=False)

    # get mode of module manufacturer #1 for each install company
    # doesn't seem to work when -9999 values are replaced with NaNs
    manufacturer_modes = lbnl_df[['Installer Name', 'Module Manufacturer #1']].groupby('Installer Name').agg(lambda x: x.value_counts().index[0])

    lbnl_zip_data = lbnl_df[['Battery System', 'Feed-in Tariff (Annual Payment)', 'Zip Code']].copy()

    lbnl_zip_data.replace(-9999, 0, inplace=True)
    lbnl_zip_groups = lbnl_zip_data.groupby('Zip Code').mean()
    lbnl_zip_groups = lbnl_zip_groups[~(lbnl_zip_groups.index == '-9999')]
    lbnl_zip_groups.reset_index(inplace=True)

    return manufacturer_modes, lbnl_zip_groups


def extract_eia_data():
    """
    Extracts data from EIA for main metrics table and utility table.
    """
    eia861_df = pd.read_excel('data/Sales_Ult_Cust_2018.xlsx', header=[0, 1, 2])

    # util number here is eiaia in the IOU data
    # get relevant columns from multiindex dataframe
    utility_number = eia861_df['Utility Characteristics', 'Unnamed: 1_level_1', 'Utility Number']
    utility_name = eia861_df['Utility Characteristics', 'Unnamed: 2_level_1', 'Utility Name']
    service_type = eia861_df['Utility Characteristics', 'Unnamed: 4_level_1', 'Service Type']
    ownership = eia861_df['Utility Characteristics', 'Unnamed: 7_level_1', 'Ownership']
    eia_utility_data = pd.concat([utility_number, utility_name, service_type, ownership], axis=1)
    eia_utility_data.columns = eia_utility_data.columns.droplevel(0).droplevel(0)

    # get residential cost and kwh usage data
    res_data = eia861_df['RESIDENTIAL'].copy()
    # missing data seems to be a period
    res_data.replace('.', np.nan, inplace=True)
    for c in res_data.columns:
        print(c)
        res_data[c] = res_data[c].astype('float')

    # convert revenues to yearly bill and MWh to kWh
    res_data['average_yearly_bill'] = res_data['Revenues', 'Thousand Dollars'] * 1000 / res_data['Customers', 'Count']
    res_data['average_yearly_kwh'] = (res_data['Sales', 'Megawatthours'] * 1000) / res_data['Customers', 'Count']
    res_columns = ['average_yearly_bill', 'average_yearly_kwh']
    # get rid of empty 2nd level multiindex level
    res_data.columns = res_data.columns.droplevel(1)

    # combine residential and utility info data
    eia_861_data = pd.concat([res_data[res_columns], eia_utility_data], axis=1)

    # combine zipcodes with EIA861 data
    eia_zip_df = load_eia_iou_data()
    eia_861_data_zipcode = eia_861_data.merge(eia_zip_df, left_on='Utility Number', right_on='eiaid')

    return eia_861_data_zipcode


def extract_acs_data(load_csv=True, save_csv=True):
    """
    Extracts ACS US census data from Google BigQuery.

    load_csv - boolean; if True, tries to load data from csv
    save_csv - boolean; if True, will save data to csv if downloading anew
    """
    # ACS US census data
    ACS_DB = '`bigquery-public-data`.census_bureau_acs'
    ACS_TABLE = 'zip_codes_2017_5yr'


    filename = 'data/acs_data.csv'
    if load_csv and os.path.exists(filename):
        return pd.read_csv(filename)
    
    acs_data_query = f"""SELECT   geo_id,
                        median_age,
                        housing_units,
                        median_income,
                        owner_occupied_housing_units,
                        occupied_housing_units,
                        dwellings_1_units_detached + dwellings_1_units_attached + dwellings_2_units + dwellings_3_to_4_units AS family_homes,
                        bachelors_degree_2,
                        different_house_year_ago_different_city + different_house_year_ago_same_city AS moved_recently
                        FROM {ACS_DB}.{ACS_TABLE}"""

    acs_data = pd.read_gbq(acs_data_query)

    if save_csv:
        acs_data.to_csv(filename, index=False)
    
    return acs_data


def extract_psr_data(load_csv=True, save_csv=True):
    """
    Extracts project sunroof data from Google BigQuery.

    load_csv - boolean; if True, tries to load data from csv
    save_csv - boolean; if True, will save data to csv if downloading anew
    """
    PSR_DB = '`bigquery-public-data`.sunroof_solar'
    PSR_TABLE = 'solar_potential_by_postal_code'

    filename = 'data/psr_data.csv'
    if load_csv and os.path.exists(filename):
        return pd.read_csv(filename)

    psr_query = f"""SELECT region_name,
                        percent_covered,
                        percent_qualified,
                        number_of_panels_total,
                        kw_median,
                        (count_qualified - existing_installs_count) AS potential_installs
                        FROM {PSR_DB}.{PSR_TABLE};
                        """

    psr_df = pd.read_gbq(psr_query)

    if save_csv:
        psr_df.to_csv(filename)

    return psr_df


def fill_zips(x):
    if not pd.isna(x['zip']):
        return x['zip']
    elif not pd.isna(x['Zip Code']):
        return x['Zip Code']
    elif not pd.isna(x['geo_id']):
        return x['geo_id']
    elif not pd.isna(x['region_name']):
        return x['region_name']
    else:
        return np.nan


def merge_data(psr, acs, lbnl, eia, read_csv=True, write_csv=True):
    """
    Combines EIA, ACS, project sunroof, and LBNL datasets in preparation for writing to the database.
    
    psr - pandas DataFrame with project sunroof data
    acs - pandas DataFrame with ACS US census data
    lbnl - pandas DataFrame with LBNL data
    eia - pandas DataFrame with EIA data
    read_csv - boolean; if True, tries to read csv file if exists
    write_csv - boolean; if True, writes final dataframe to csv
    """
    filename = 'data/solar_metrics_data.csv'
    if read_csv and os.path.exists(filename):
        return pd.read_csv(filename)

    psr_acs = psr_df.merge(acs_data, left_on='region_name', right_on='geo_id', how='outer')
    psr_acs_lbnl = psr_acs.merge(lbnl_zip_groups, left_on='region_name', right_on='Zip Code', how='outer')
    psr_acs_lbnl_eia = psr_acs_lbnl.merge(eia_861_data_zipcode, left_on='region_name', right_on='zip', how='outer')

    # combine different zip code columns to make one column with no missing values
    psr_acs_lbnl_eia['full_zip'] = psr_acs_lbnl_eia.apply(fill_zips, axis=1)

    # columns we'll use in the same order as the DB table
    cols_to_use = ['full_zip',
                'percent_qualified',
                'number_of_panels_total',
                'kw_median',
                'potential_installs',
                'median_income',
                'median_age',
                'occupied_housing_units',
                'owner_occupied_housing_units',
                'family_homes',
                'bachelors_degree_2',
                'moved_recently',
                'average_yearly_bill',
                'average_yearly_kwh',
                # note: installer ID has to be gotten from the installer table
                'Battery System',
                'Feed-in Tariff (Annual Payment)']

    final_df = psr_acs_lbnl_eia[cols_to_use]

    if write_csv:
        final_df.to_csv(filename, index=False)
    
    return final_df


psr_df = extract_psr_data()
acs_df = extract_acs_data()
manufacturer_df, lbnl_df = extract_lbnl_data()
eia_df = extract_eia_data()
merge_data(psr_df, acs_df, lbnl_df, eia_df)