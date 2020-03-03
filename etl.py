import os

import pandas as pd
import numpy as np

# Set up GCP API
from google.cloud import bigquery
# Construct a BigQuery client object.
client = bigquery.Client()



def load_lbnl_data(replace_nans=True, short_zips=True):
    """
    Loads LBNL solar survey data.

    replace_nans - boolean; if True, replaces -9999 missing value placeholders with np.nan
    short_zips - boolean; if True, makes sure all zip codes are 5-digit
    """
    df1 = pd.read_csv('data/TTS_LBNL_public_file_10-Dec-2019_p1.csv', encoding='latin-1', low_memory=False)
    df2 = pd.read_csv('data/TTS_LBNL_public_file_10-Dec-2019_p2.csv', encoding='latin-1', low_memory=False)
    lbnl_df = pd.concat([df1, df2], axis=0)
    if replace_nans:
        lbnl_df.replace(-9999, np.nan, inplace=True)
        lbnl_df.replace('-9999', np.nan, inplace=True)
    
    if short_zips:
        lbnl_df['Zip Code'] = lbnl_df['Zip Code'].apply(lambda x: x.strip()[:5])
    
    # a few zip codes with only 4 digits
    lbnl_df['Zip Code'] = lbnl_df['Zip Code'].apply(lambda x: x.zfill(5))
    
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
    manufacturer_modes.reset_index(inplace=True)
    # dictionary of installer name to ID
    id_install_dict = {}
    for i, r in manufacturer_modes.iterrows():
        id_install_dict[r['Installer Name']] = i

    installer_modes = lbnl_df[['Installer Name', 'Zip Code']].groupby('Zip Code').agg(lambda x: x.value_counts().index[0])
    installer_modes['Installer Name'].replace(id_install_dict)
    installer_modes.colunms = ['Installer ID']

    lbnl_zip_data = lbnl_df[['Battery System', 'Feed-in Tariff (Annual Payment)', 'Zip Code']].copy()

    lbnl_zip_data.replace(-9999, 0, inplace=True)
    lbnl_zip_groups = lbnl_zip_data.groupby('Zip Code').mean()
    # merge with most common installer by zip codes
    lbnl_zip_groups = lbnl_zip_groups.merge(installer_modes, left_index=True, right_index=True)
    lbnl_zip_groups = lbnl_zip_groups[~(lbnl_zip_groups.index == '-9999')]
    lbnl_zip_groups.reset_index(inplace=True)
    lbnl_zip_groups['Installer ID'] = lbnl_zip_groups['Installer Name'].replace(id_install_dict)

    return manufacturer_modes, lbnl_zip_groups


def extract_eia_data():
    """
    Extracts data from EIA for main metrics table and utility table.

    Note: several utilities serve the same zip codes.
    """
    # load zipcode to eiaid/util number data
    eia_zip_df = load_eia_iou_data()
    # eia861 report loading
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
    # drop uppermost level
    res_data.columns = res_data.columns.droplevel(0)

    # missing data seems to be a period
    res_data.replace('.', np.nan, inplace=True)
    for c in res_data.columns:
        res_data[c] = res_data[c].astype('float')

    util_number_data = pd.DataFrame(utility_number)
    util_number_data.columns = util_number_data.columns.droplevel(0).droplevel(0)
    res_data = pd.concat([res_data, utility_number], axis=1)
    res_data.columns = ['Thousand Dollars', 'Megawatthours', 'Count', 'Utility Number']

    # first join with zipcode data to group by zip
    res_data_zip = res_data.merge(eia_zip_df, left_on='Utility Number', right_on='eiaid')
    # group by zip and get sums of revenues, MWh, and customer count
    res_data_zip = res_data_zip.groupby('zip').sum()
    # convert revenues to yearly bill and MWh to kWh
    # thousand dollars of revenue divided by customer count
    res_data_zip['average_yearly_bill'] = res_data_zip['Thousand Dollars'] * 1000 / res_data_zip['Count']
    # kwh divided by customer count
    res_data_zip['average_yearly_kwh'] = (res_data_zip['Megawatthours'] * 1000) / res_data_zip['Count']
    res_columns = ['average_yearly_bill', 'average_yearly_kwh']
    res_data_zip = res_data_zip[res_columns]


    # combine residential and utility info data
    # eia_861_data = pd.concat([res_data[res_columns], eia_utility_data], axis=1)

    # combine zipcodes with EIA861 utility data
    eia_util_zipcode = eia_utility_data.merge(eia_zip_df, left_on='Utility Number', right_on='eiaid')
    # get most-common utility name, service type, and ownership by zipcode
    common_util = eia_util_zipcode[['zip', 'Utility Name', 'Service Type', 'Ownership']].groupby('zip').agg(lambda x: x.value_counts().index[0])

    eia_861_summary = res_data_zip.merge(common_util, left_index=True, right_index=True)
    # change zip back to a column
    eia_861_summary.reset_index(inplace=True)

    return eia_861_summary


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
        acs_df = pd.read_csv(filename)
        acs_df['geo_id'] = acs_df['geo_id'].astype('str')
        acs_df['geo_id'] = acs_df['geo_id'].apply(lambda x: x.zfill(5))
    
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
    Extracts project sunroof data from Google BigQuery.-

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

    # some duplicate zip codes; seems to be one that includes most data and a few extra
    # drop dupes with small pct covered
    # ideally we would add them together, but different columns have to be combined in different ways
    dupe_idx = psr_df['region_name'].duplicated(keep=False)
    duplicates = psr_df[dupe_idx]
    psr_df = psr_df[~dupe_idx]
    duplicates = duplicates[duplicates['percent_covered'] > 5]
    psr_df = pd.concat([psr_df, duplicates])

    if save_csv:
        psr_df.to_csv(filename, index=False)

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


def merge_data(psr, acs, lbnl, eia, read_csv=True, write_csv=True, how='outer'):
    """
    Combines EIA, ACS, project sunroof, and LBNL datasets in preparation for writing to the database.
    
    psr - pandas DataFrame with project sunroof data
    acs - pandas DataFrame with ACS US census data
    lbnl - pandas DataFrame with LBNL data
    eia - pandas DataFrame with EIA data
    read_csv - boolean; if True, tries to read csv file if exists
    write_csv - boolean; if True, writes final dataframe to csv
    how - string; type of merge to perform like outer, inner, etc
    """
    filename = 'data/solar_metrics_data.csv'
    if read_csv and os.path.exists(filename):
        return pd.read_csv(filename)

    # eia have most zips, followed by lbnl then acs then psr
    # merge, then make column of zip codes with none missing after each step
    eia_lbnl = eia.merge(lbnl, left_on='zip', right_on='Zip Code', how=how)
    eia_lbnl['full_zip'] = eia_lbnl.apply(fill_zips, axis=1)
    eia_lbnl_acs = eia_lbnl.merge(acs, left_on='full_zip', right_on='geo_id', how=how)
    eia_lbnl_acs['full_zip'] = eia_lbnl_acs.apply(fill_zips, axis=1)
    eia_lbnl_acs_psr = eia_lbnl_acs.merge(psr, left_on='full_zip', right_on='region_name', how=how)
    # combine different zip code columns to make one column with no missing values
    eia_lbnl_acs_psr['full_zip'] = eia_lbnl_acs_psr.apply(fill_zips, axis=1)

    # combine different zip code columns to make one column with no missing values
    eia_lbnl_acs_psr['full_zip'] = eia_lbnl_acs_psr.apply(fill_zips, axis=1)

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
                'Installer ID',
                'Battery System',
                'Feed-in Tariff (Annual Payment)']

    final_df = eia_lbnl_acs_psr[cols_to_use]

    if write_csv:
        final_df.to_csv(filename, index=False)
    
    return final_df


def check_zips_len_5(df, zip_list):
    """
    Make sure all zip codes have length of 5

    df - pandas dataframe; dataframe with zip codes and other data
    zip_list - list; list of zip codes as strings
    """
    are5 = sum([len(l) == 5 for l in zip_list])
    return are5 == df.shape[0]


def data_quality_checks(psr_df, acs_df, lbnl_df, eia_df):
    """
    data quality checks: 
    1. make sure there are no duplicate zip codes left in individual dfs
    2. make sure total number of zips not above max of 42107

    psr_df - pandas dataframe with project sunroof data
    acs_df - pandas dataframe with ACS data
    lbnl_df - pandas dataframe with LBNL data
    eia_df - pandas dataframe with EIA861 data
    """
    psr_zips = set(psr_df['region_name'].unique())
    acs_zips = set(acs_df['geo_id'].unique())
    lbnl_zips = set(lbnl_df['Zip Code'].unique())
    eia_zips = set(eia_df['zip'].unique())

    if not check_zips_len_5(psr_df, psr_zips):
        print('DATA QUALITY CHECK ERROR:')
        print('project solar zip codes not all length 5')
    else:
        print('CHECK PASSED: project solar zip codes all length 5')
    if not check_zips_len_5(acs_df, acs_zips):
        print('DATA QUALITY CHECK ERROR:')
        print('ACS zip codes not all length 5')
    else:
        print('CHECK PASSED: ACS zip codes all length 5')
    if not check_zips_len_5(lbnl_df, lbnl_zips):
        print('DATA QUALITY CHECK ERROR:')
        print('LBNL zip codes not all length 5')
    else:
        print('CHECK PASSED: LBNL zip codes all length 5')
    if not check_zips_len_5(eia_df, eia_zips):
        print('DATA QUALITY CHECK ERROR:')
        print('EIA zip codes not all length 5')
    else:
        print('CHECK PASSED: EIA zip codes all length 5')

    total_zips = len(eia_zips.union(lbnl_zips).union(acs_zips).union(psr_zips))
    if total_zips > 41702:
        print('DATA QUALITY CHECK ERROR:')
        print(f'number of zip codes is {total_zips}; max should be 41,702')
    else:
        print('CHECK PASSED: total number of zip codes below maximum')



psr_df = extract_psr_data()
acs_df = extract_acs_data()
manufacturer_df, lbnl_df = extract_lbnl_data()
eia_df = extract_eia_data()

data_quality_checks(psr_df, acs_df, lbnl_df, eia_df)

final_df = merge_data(psr_df, acs_df, lbnl_df, eia_df, read_csv=False, write_csv=True)



# import configparser
# import psycopg2
# import sql_queries as sql_q


# def create_tables(cur, conn):
#     """
#     cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
#     """
#     for q in sql_q.create_table_queries:
#         print('executing query: {}'.format(q))
#         cur.execute(q)
#         conn.commit()


# for i, r in final_df.iterrows():
#     sql_q.solar_metrics_insert.format(*r)

# for i, r in location_df.iterrows():
#     sql_q.location_insert.format(*r)

# for i, r in eia_df.iterrows():
#     sql_q.utility_insert.format(*r[['Zip Code', 'Utility Name', 'Ownership', 'Service Type']])

# for i, r in manufacturer_df.iterrows():
#     sql_q.installer_insert.format(i, *r)