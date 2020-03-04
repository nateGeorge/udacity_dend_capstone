# drop tables; for resetting the DWH
solar_metrics_drop = 'DROP TABLE IF EXISTS solar_metrics;'
zipcodes_drop = 'DROP TABLE IF EXISTS zipcodes;'
utility_drop = 'DROP TABLE IF EXISTS utility;'
installer_drop = 'DROP TABLE IF EXISTS installer;'


# create tables
solar_metrics_table_create = """CREATE TABLE IF NOT EXISTS solar_metrics
(id INT IDENTITY(0, 1) PRIMARY KEY,
zip_code VARCHAR NOT NULL,
percent_qualified_bldgs NUMERIC,
number_potential_panels INT,
kw_median NUMERIC,
potential_installs INT,
median_income NUMERIC,
median_age NUMERIC,
occupied_housing_units INT,
owner_occupied_housing_units INT,
family_homes INT,
collegiates INT,
moved_recently INT,
average_yearly_electric_bill NUMERIC,
average_yearly_kwh_used NUMERIC,
primary_installer_id INT,
battery_system_fraction NUMERIC,
mean_annual_feedin_tariff NUMERIC);
"""

zipcode_table_create = """CREATE TABLE IF NOT EXISTS zipcodes
(zip_code VARCHAR PRIMARY KEY,
city_name VARCHAR,
state_name VARCHAR,
latitude NUMERIC,
longitude NUMERIC);
"""

utility_table_create = """CREATE TABLE IF NOT EXISTS utility
(zip_code VARCHAR PRIMARY KEY,
utility_name VARCHAR,
ownership VARCHAR, 
service_type VARCHAR);
"""

installer_table_create = """CREATE TABLE IF NOT EXISTS installer
(installer_id INT PRIMARY KEY,
installer_name VARCHAR,
installer_primary_module_manufacturer VARCHAR);
"""

# insert statements

solar_metrics_insert = """INSERT INTO solar_metrics
(zip_code,
percent_qualified_bldgs,
number_potential_panels,
kw_median,
potential_installs,
median_income,
median_age,
occupied_housing_units,
owner_occupied_housing_units,
family_homes,
collegiates,
moved_recently,
average_yearly_electric_bill,
average_yearly_kwh_used,
primary_installer_id,
battery_system_fraction,
mean_annual_feedin_tariff)
VALUES %s;
"""

zipcodes_insert = """INSERT INTO zipcodes
(zip_code, city_name, state_name, latitude, longitude)
VALUES %s;
"""

utility_insert = """INSERT INTO utility
(zip_code, utility_name, ownership, service_type)
VALUES %s;
"""

installer_insert = """INSERT INTO installer
(installer_id, installer_name, installer_primary_module_manufacturer)
VALUES %s;
"""

drop_table_queries = [solar_metrics_drop,
                    zipcodes_drop,
                    utility_drop,
                    installer_drop]

create_table_queries = [solar_metrics_table_create,
                        zipcode_table_create,
                        utility_table_create,
                        installer_table_create]
