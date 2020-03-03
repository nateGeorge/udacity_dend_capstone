# drop tables; for resetting the DWH



# create tables
solar_metrics_table_create = ("""CREATE TABLE IF NOT EXISTS solar_metrics
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
""")

location_table_create = ("""CREATE TABLE IF NOT EXISTS location
(zip_code VARCHAR PRIMARY KEY,
city_name VARCHAR,
state_name VARCHAR,
latitude NUMERIC,
longitude NUMERIC);
""")

utility_table_create = ("""CREATE TABLE IF NOT EXISTS utility
(zip_code VARCHAR PRIMARY KEY,
utility_name VARCHAR,
ownership VARCHAR, 
service_type VARCHAR);
""")

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
battery_system_fraction,
mean_annual_feedin_tariff)
{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {};
"""

location_insert = """INSERT INTO location
(zip_code, city_name, state_name, latitude, longitude)
{}, {}, {}, {}, {};
"""

utility_insert = """INSERT INTO utility
(zip_code, utility_name, ownership, service_type)
"""

installer_insert = """INSERT INTO installer
(installer_id, installer_name, installer_primary_module_manufacturer)
{}, {}, {};
"""

create_table_queries = [solar_metrics_table_create,
                        location_table_create,
                        utility_table_create,
                        installer_table_create]