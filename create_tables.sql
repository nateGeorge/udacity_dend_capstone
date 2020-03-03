-- this is for creating the ERD in visual paradigm from sql ddl statements

CREATE TABLE IF NOT EXISTS solar_metrics
(id INT PRIMARY KEY,
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
installer_id INT,
battery_system_fraction NUMERIC,
mean_annual_feedin_tariff NUMERIC);


CREATE TABLE IF NOT EXISTS location
(zipcode VARCHAR PRIMARY KEY,
city_name VARCHAR,
state_name VARCHAR,
latitude NUMERIC,
longitude NUMERIC);

CREATE TABLE IF NOT EXISTS utility
(zipcode VARCHAR PRIMARY KEY,
utility_name VARCHAR,
ownership VARCHAR, 
service_type VARCHAR);

CREATE TABLE IF NOT EXISTS installer
(installer_id INT PRIMARY KEY,
installer name VARCHAR,
installer_primary_module_manufacturer VARCHAR);