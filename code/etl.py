import os
import configparser

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras

# Set up GCP API
from google.cloud import bigquery
# Construct a BigQuery client object.
client = bigquery.Client()

import sql_queries as sql_q


def convert_int_zipcode_to_str(df, col):
    """
    Converts integer zipcode column into 0-padded str column.

    df - pandas dataframe with zipcode int column
    col - string; name of column with zipcodes
    """
    df[col] = df[col].astype('str')
    df[col] = df[col].apply(lambda x: x.zfill(5))


def remove_bad_zipcodes(zip_df, df, col):
    """
    Removes bad zipcodes from data (i.e. erroneous zipcodes that are 
    not valid US zipcodes).

    zip_df - pandas dataframe with valid US zipcodes
    df - pandas dataframe to be cleaned
    col - string; column name of zipcode column in df
    """
    zip_set = set(zip_df['Zipcode'].unique())
    return df[df[col].isin(zip_set)]


def load_lbnl_data(zip_df, replace_nans=True, short_zips=True):
    """
    Loads LBNL solar survey data.

    zip_df - pandas dataframe with zipcode data for cleaning bad zipcodes
    replace_nans - boolean; if True, replaces -9999 missing value placeholders with np.nan
    short_zips - boolean; if True, makes sure all zip codes are 5-digit
    """
    df1 = pd.read_csv('../data/TTS_LBNL_public_file_10-Dec-2019_p1.csv', encoding='latin-1', low_memory=False)
    df2 = pd.read_csv('../data/TTS_LBNL_public_file_10-Dec-2019_p2.csv', encoding='latin-1', low_memory=False)
    lbnl_df = pd.concat([df1, df2], axis=0)
    if replace_nans:
        lbnl_df.replace(-9999, np.nan, inplace=True)
        lbnl_df.replace('-9999', np.nan, inplace=True)
    
    if short_zips:
        lbnl_df['Zip Code'] = lbnl_df['Zip Code'].apply(lambda x: x.strip()[:5])
    
    # a few zip codes with only 4 digits
    lbnl_df['Zip Code'] = lbnl_df['Zip Code'].apply(lambda x: x.zfill(5))
    
    lbnl_df = remove_bad_zipcodes(zip_df, lbnl_df, 'Zip Code')
    return lbnl_df


def load_eia_zipcode_data(zip_df):
    """
    Loads EIA dataset with zipcodes and energy providers.

    zip_df - pandas dataframe with zipcode data for cleaning bad zipcodes
    """
    iou_df = pd.read_csv('../data/iouzipcodes2017.csv')
    noniou_df = pd.read_csv('../data/noniouzipcodes2017.csv')
    eia_zipcode_df = pd.concat([iou_df, noniou_df], axis=0)
    
    # zip codes are ints without zero padding
    convert_int_zipcode_to_str(eia_zipcode_df, 'zip')
    eia_zipcode_df = remove_bad_zipcodes(zip_df, eia_zipcode_df, 'zip')
    
    return eia_zipcode_df


def extract_lbnl_data(zip_df):
    """
    Gets data from LBNL dataset for the installer table and main metrics table.

    zip_df - pandas dataframe with zipcode data for cleaning bad zipcodes
    """
    lbnl_df = load_lbnl_data(zip_df, replace_nans=False)

    # get mode of module manufacturer #1 for each install company
    # doesn't seem to work when -9999 values are replaced with NaNs
    manufacturer_modes = lbnl_df[['Installer Name', 'Module Manufacturer #1']].groupby('Installer Name').agg(lambda x: x.value_counts().index[0])
    manufacturer_modes.reset_index(inplace=True)
    # dictionary of installer name to ID
    id_install_dict = {}
    for i, r in manufacturer_modes.iterrows():
        id_install_dict[r['Installer Name']] = i

    # get primary installers by zipcode
    installer_modes = lbnl_df[['Installer Name', 'Zip Code']].groupby('Zip Code').agg(lambda x: x.value_counts().index[0])

    lbnl_zip_data = lbnl_df[['Battery System', 'Feed-in Tariff (Annual Payment)', 'Zip Code']].copy()

    lbnl_zip_data.replace(-9999, 0, inplace=True)
    lbnl_zip_groups = lbnl_zip_data.groupby('Zip Code').mean()
    # merge with most common installer by zip codes
    lbnl_zip_groups = lbnl_zip_groups.merge(installer_modes, left_index=True, right_index=True)
    lbnl_zip_groups = lbnl_zip_groups[~(lbnl_zip_groups.index == '-9999')]
    lbnl_zip_groups.reset_index(inplace=True)
    lbnl_zip_groups['Installer ID'] = lbnl_zip_groups['Installer Name'].replace(id_install_dict)
    lbnl_zip_groups['Installer ID'] = lbnl_zip_groups['Installer ID'].astype('int')

    return manufacturer_modes.reset_index(), lbnl_zip_groups


def extract_eia_data(zip_df):
    """
    Extracts data from EIA for main metrics table and utility table.

    zip_df - pandas dataframe with zipcode data for cleaning bad zipcodes

    Note: several utilities serve the same zip codes.
    """
    # load zipcode to eiaid/util number data
    eia_zip_df = load_eia_zipcode_data(zip_df)
    # eia861 report loading
    eia861_df = pd.read_excel('../data/Sales_Ult_Cust_2018.xlsx', header=[0, 1, 2])

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
    
    eia_861_summary = remove_bad_zipcodes(zip_df, eia_861_summary, 'zip')

    return eia_861_summary


def extract_acs_data(zip_df, load_csv=True, save_csv=True):
    """
    Extracts ACS US census data from Google BigQuery.

    zip_df - pandas dataframe with zipcode data for cleaning bad zipcodes
    load_csv - boolean; if True, tries to load data from csv
    save_csv - boolean; if True, will save data to csv if downloading anew
    """
    # ACS US census data
    ACS_DB = '`bigquery-public-data`.census_bureau_acs'
    ACS_TABLE = 'zip_codes_2017_5yr'


    filename = '../data/acs_data.csv'
    if load_csv and os.path.exists(filename):
        acs_df = pd.read_csv(filename)
        convert_int_zipcode_to_str(acs_df, 'geo_id')
        return acs_df
    
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

    acs_data = remove_bad_zipcodes(zip_df, acs_data, 'geo_id')

    if save_csv:
        acs_data.to_csv(filename, index=False)
    
    return acs_data


def extract_psr_data(zip_df, load_csv=True, save_csv=True):
    """
    Extracts project sunroof data from Google BigQuery.-

    zip_df - pandas dataframe with zipcode data for cleaning bad zipcodes
    load_csv - boolean; if True, tries to load data from csv
    save_csv - boolean; if True, will save data to csv if downloading anew
    """
    PSR_DB = '`bigquery-public-data`.sunroof_solar'
    PSR_TABLE = 'solar_potential_by_postal_code'

    filename = '../data/psr_data.csv'
    if load_csv and os.path.exists(filename):
        df = pd.read_csv(filename)
        convert_int_zipcode_to_str(df, 'region_name')
        return df

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

    psr_df = remove_bad_zipcodes(zip_df, psr_df, 'region_name')

    if save_csv:
        psr_df.to_csv(filename, index=False)

    return psr_df


def extract_zipcode_data():
    """
    Extracts zipcode, city, state, lat/lng data from zipcode dataset.
    """
    filename = '../data/free-zipcode-database-Primary.csv'

    zip_df = pd.read_csv(filename)
    convert_int_zipcode_to_str(zip_df, 'Zipcode')

    # don't use decomissioned zipcodes
    zip_df = zip_df[~zip_df['Decommisioned']]

    cols = ['Zipcode', 'City', 'State', 'Lat', 'Long']
    return zip_df[cols]


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
    filename = '../data/solar_metrics_data.csv'
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
    
    # columns to convert to nullable integer type
    cols = ['Installer ID', 
            'potential_installs',
            'number_of_panels_total',
            'occupied_housing_units',
            'owner_occupied_housing_units',
            'family_homes',
            'bachelors_degree_2',
            'moved_recently']
    for c in cols:
        final_df[c] = final_df[c].astype('Int64')

    # combine different zip code columns to make one column with no missing values
    eia_lbnl_acs_psr['full_zip'] = eia_lbnl_acs_psr.apply(fill_zips, axis=1)
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


def zipcode_data_quality_checks(psr_df, acs_df, lbnl_df, eia_df, zip_df, final_df):
    """
    data quality checks: 
    1. Make sure there are no duplicate zip codes left in individual dfs
    2. Make sure total number of zips not above max of 42107
    3. Ensure all zip codes in our data are also in the zipcode dataset. 

    psr_df - pandas dataframe with project sunroof data
    acs_df - pandas dataframe with ACS data
    lbnl_df - pandas dataframe with LBNL data
    eia_df - pandas dataframe with EIA861 data
    zip_df - pandas dataframe with zipcodes, cities, states, lat/lng
    final_df - merged dataframe of the first 4 dataframes (psr, acs, lbnl, eia)
    """
    # check 1 -- make sure no duplicate zip codes in dfs
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

    # check 2 -- ensure zip codes does not exceed max number
    total_zips = len(eia_zips.union(lbnl_zips).union(acs_zips).union(psr_zips))
    if total_zips > 41859:
        print('FAILED DATA QUALITY CHECK:')
        print(f'number of zip codes is {total_zips}; max should be 41,859')
    else:
        print('CHECK PASSED: total number of zip codes below maximum')


    # check 3 -- make sure zipcodes in full dataset are also in zipcode/city/state dataset
    # from this test I found some zipcodes in the data are erroneous
    full_zips = set(final_df['full_zip'].unique())
    zips = set(zip_df['Zipcode'].unique())
    zipdiff = len(full_zips.difference(zips))
    if zipdiff > 0:
        print('FAILED DATA QUALITY CHECK:')
        print(f'{zipdiff} zipcodes in full dataset not in zipcode dataset')
    else:
        print('CHECK PASSED: all zipcodes in full dataset also in zipcode dataset')


def make_redshift_connection():
    """
    Makes connection to redshift cluster.
    """
    config = configparser.ConfigParser()
    # should be connection_filename from infrastructure_as_code.py
    config_file = os.path.expanduser('~/.aws_config/solar_cluster.cfg')
    config.read(config_file)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    return conn, cur


def drop_tables(cur, conn):
    """
    Drops all tables in Redshift.

    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    """
    for q in sql_q.drop_table_queries:
        print('executing query: {}'.format(q))
        cur.execute(q)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables in Redshift.
    
    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    """
    for q in sql_q.create_table_queries:
        print('executing query: {}'.format(q))
        cur.execute(q)
        conn.commit()
    

def insert_data(cur, conn, final_df, zip_df, eia_df, manufacturer_df):
    """
    Insert many values at once to redshift and convert dataframe to tuples.
    This works, but is slow.
    It would be faster to write to s3 then load from s3 into redshift:
    https://stackoverflow.com/a/56275519/4549682

    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    """
    print('inserting solar_metric table data...')
    psycopg2.extras.execute_values(cur, sql_q.solar_metrics_insert, list(final_df.itertuples(index=False, name=None)))
    # seems to hang here...maybe just slow
    conn.commit()

    print('inserting zipcodes table data...')
    psycopg2.extras.execute_values(cur, sql_q.zipcodes_insert, list(zip_df.itertuples(index=False, name=None)))
    conn.commit()

    print('inserting utility table data...')
    utility_df = eia_df[['zip', 'Utility Name', 'Ownership', 'Service Type']]
    psycopg2.extras.execute_values(cur, sql_q.utility_insert, list(utility_df.itertuples(index=False, name=None)))
    conn.commit()

    print('inserting installer table data...')
    psycopg2.extras.execute_values(cur, sql_q.installer_insert, list(manufacturer_df.itertuples(index=False, name=None)))
    conn.commit()


def write_csvs_to_s3(bucket='dend-capstone-ncg'):
    """
    Writes pandas dataframes to s3 bucket.

    bucket - string; bucket name
    """
    final_df.to_csv(f's3://{bucket}/final_df.csv', index=False)
    zip_df.to_csv(f's3://{bucket}/zip_df.csv', index=False)
    utility_df = eia_df[['zip', 'Utility Name', 'Ownership', 'Service Type']]
    utility_df.to_csv(f's3://{bucket}/utility_df.csv', index=False)
    manufacturer_df.to_csv(f's3://{bucket}/manufacturer_df.csv', index=False)


def copy_s3_to_redshift(cur, conn, bucket='dend-capstone-ncg'):
    """
    Copies csv files from s3 to redshift.

    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    bucket - string; bucket name
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.expanduser('~/.aws_config'), 'solar_cluster.cfg')
    config.read_file(open(config_path))


    solar_metrics_copy = ("""COPY solar_metrics FROM 's3://{}/final_df.csv'
    credentials 'aws_iam_role={}' IGNOREHEADER 1 CSV;
    """).format(bucket, config.get('IAM_ROLE', 'ARN'))

    cur.execute(solar_metrics_copy)
    conn.commit()

    zipcodes_copy = ("""COPY zipcodes FROM 's3://{}/zip_df.csv'
    credentials 'aws_iam_role={}' IGNOREHEADER 1 CSV;
    """).format(bucket, config.get('IAM_ROLE', 'ARN'))
    print('executing query:')
    print(zipcodes_copy)

    cur.execute(zipcodes_copy)
    conn.commit()

    utility_copy = ("""COPY utility FROM 's3://{}/utility_df.csv'
    credentials 'aws_iam_role={}' IGNOREHEADER 1 CSV;
    """).format(bucket, config.get('IAM_ROLE', 'ARN'))
    print('executing query:')
    print(utility_copy)

    cur.execute(utility_copy)
    conn.commit()

    installer_copy = ("""COPY installer FROM 's3://{}/manufacturer_df.csv'
    credentials 'aws_iam_role={}' IGNOREHEADER 1 CSV;
    """).format(bucket, config.get('IAM_ROLE', 'ARN'))
    print('executing query:')
    print(installer_copy)

    cur.execute(installer_copy)
    conn.commit()


if __name__=='__main__':
    # extracting and transforming data
    zip_df = extract_zipcode_data()
    psr_df = extract_psr_data(zip_df, load_csv=True, save_csv=False)
    acs_df = extract_acs_data(zip_df, load_csv=True, save_csv=False)
    manufacturer_df, lbnl_df = extract_lbnl_data(zip_df)
    eia_df = extract_eia_data(zip_df)

    # transforming data
    final_df = merge_data(psr_df, acs_df, lbnl_df, eia_df, read_csv=True, write_csv=False)

    # quality checks
    zipcode_data_quality_checks(psr_df, acs_df, lbnl_df, eia_df, zip_df, final_df)

    # write data to redshift
    conn, cur = make_redshift_connection()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    write_csvs_to_s3()
    copy_s3_to_redshift(cur, conn)