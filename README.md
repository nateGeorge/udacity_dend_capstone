# Understanding Opportunities in the US Residential Solar Market
This is my capstone project for the data engineering nanodegree (DEND), where I combine several data sources to answer to question:  Which US locations should solar installers target in order to install as many solar panels as possible?

## The Capstone
The purpose of the Udacity DEND capstone is to bring together disparate data sources to create something useful, and to use techniques learned in the DEND.  After looking for a long time at the [Google](https://console.cloud.google.com/marketplace/browse?filter=solution-type:dataset) and [AWS](https://registry.opendata.aws/) datasets, I had my idea: combine census, electricity, and solar data to understand which US cities are best for solar installers to target.  If you are working on a data science or engineering project yourself, realize this took me several hours of searching public datasets and APIs to figure out this idea, so don't be discouraged if it takes you a while to come up with a good project idea.

## Motivation

Chart from here showing energy sources:
https://www.eia.gov/energyexplained/us-energy-facts/

solar installations chart: https://www.seia.org/solar-industry-research-data

38.5% of electricity used by residential: https://www.eia.gov/energyexplained/electricity/use-of-electricity.php


I think most people would like clean energy to power our society, and the price of solar has decreased to the point where it's becoming [cheaper than conventional power sources in many places](https://e360.yale.edu/digest/renewables-cheaper-than-75-percent-of-u-s-coal-fleet-report-finds).  And since only about [2 million](https://www.seia.org/news/united-states-surpasses-2-million-solar-installations) out of the roughly [95 million single-family homes](https://www.quora.com/How-many-single-family-homes-are-there-in-the-US) have solar installed in the US, there's a massive opportunity for solar installers to bring solar power to US homes.  We can reduce pollution from coal and save people money on their electricity bill.  But which locations should solar installers target?  The primary aim of this project is to answer that question.

## Background and Data Sources
There are about [300 US cities](https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population) with populations of 100k or more, and solar installers probably don't have the resources to target all of them at once.  Even within a city, different areas may have much higher success rates for selling homeowners solar panels.  We need data that can help us decide which cities, and even locations within the cities (e.g. zipcodes), are best to target.

### Datasets
Thinking about the problem, we can quickly realize economics, demographics, and culture are going to play a big role in the success of residential solar installation.  Obviously people need to be able to afford it and people need to be amenable to installing solar cells on the home.  US Census data can help us understand the economics and possibly some of the sentiment towards solar via demographics (e.g. perhaps younger homeowners are more amenable to installing solar panels on their roof; more data analysis would have to be done to understand that).  People also need to have the economic incentive of saving money on their electric bill.  The other big factors are of course physics and potential roof space -- how much sunlight falls on a given area, and how many potential installs are there?

I chose a few relevant datasets for this project that give us the data we need:

| Name                                 | Abbreviation | Description                                               | Purpose                                                             | Link                                                                                                                                                | Number of Rows | Format   |
|--------------------------------------|--------------|-----------------------------------------------------------|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------------|----------|
| US Census ACS                        | ACS          | Survey of US people.                                      | Demographics                                                        | [link](https://console.cloud.google.com/marketplace/details/united-states-census-bureau/acs?q=census%20acs&id=1282ab4c-78a4-4da5-8af8-cd693fe390ab) | 33,120         | BigQuery |
| Project Sunroof                      | sunroof      | Google's residential solar install data.                  | Solar irradiation and # installs by zipcode                         | [link](https://console.cloud.google.com/marketplace/details/project-sunroof/project-sunroof)                                                        | 11,516         | BigQuery |
| LBNL Tracking the Sun                | LBNL         | LBNL installed solar system data.                         | Number of and installer for residential solar installs.             | [link](https://emp.lbl.gov/tracking-the-sun/)                                                                                                       | 1,543,831      | CSV      |
| EIA-861 Report                       | EIA861       | U.S. Energy Information Administration energy use report. | Amount and price of residential energy used.                        | [link](https://www.eia.gov/electricity/data/eia861/)                                                                                                | 3,436          | XLSX     |
| EIA IOU/Non-IOU Rates with Zip Codes | EIArates     | Utility company to zip code lookup.                       | Combine with EIA-861 to get zip codes of energy prices and amounts. | [link](https://openei.org/doe-opendata/dataset/u-s-electric-utility-companies-and-rates-look-up-by-zipcode-2017)                                    | 86,672         | CSV      |


Other potential datasets include [NREL's National Solar Radiation Database](https://registry.opendata.aws/nrel-pds-nsrdb/) for more data on solar energy potential, social media sources including Facebook, Reddit, and Twitter, to understand sentiment towards residential solar installs, and survey data (such as [Pew Center data](https://www.pewresearch.org/fact-tank/2016/10/05/americans-strongly-favor-expanding-solar-power-to-help-address-costs-and-environmental-concerns/)) to understand sentiment towards solar installs.  [RECS](https://www.eia.gov/consumption/residential/data/2015/index.php) also has more data on existing solar installs and demographics.  Since including these sources (especially social media) would require a great deal more of work, I have not done so yet.

## Data Model
I chose the classic star schema as a data model for our database.  We have a central fact table which gives us numbers on the things we want to know:

- How many potential solar installs?
- How much solar power could be generated?
- How much money do people have available?
- How much money will people save?
- Where is the least competition?

The ERD for the DB schema is shown below.  It is saved as ERD.xml for the [wwwsqldesigner](https://github.com/ondras/wwwsqldesigner).

![ERD](images/erd.png)


The data dictionary can be found in the data_dictionary.xlsx file.


The central table is the solar_metrics table with columns:
primary key id
zip_code
percent_qualified (percent of buildings qualified for solar installs) (sunroof)
number_potential_panels (number of potential solar panels for the area) (sunroof)
kw_median (kW of solar potential for the median building) (sunroof)
potential_installs (count_qualified - existing_installs_count) (sunroof)
median_income (ACS)
median_age (ACS)
occupied_housing_units (ACS)
single_family_homes (ACS)
collegiates (people with at least a bachelors degree) (ACS)
moved_recently (different house a year ago) (ACS)
owner_occupied_housing_units (ACS)
average_electric_bill (EIA)
average_kwh_used (EIA)
installer_ID (majority installer for that location) (LBNL)
battery_system (If the install has a battery system, fraction of installs with battery) (LBNL)
average_annual_feedin_tariff (Feed-in Tariff (Annual Payment)) (LBNL)


The dimension tables include:

- location table: zipcode, city name, state name, latitude, longitude
- utility table: zipcode, utility name, ownership, service type
- installer table: installer ID, installer name, installer primary module manufacturer

To expand, I would add another fact table (making the schema a galaxy schema) with installation data from the LBNL dataset.  This would enable looking at which areas have growing competition from other companies.  The table would have at least:
- primary key id
- Zip Code
- Feed-in Tariff (Annual Payment)
- Feed-in Tariff (Duration)
- Module Model ID
- Manufacturer ID
- Module Efficiency
- Installer ID

Then we would also need another dimension table:
- module manufacturer table: manufacturer code, manufacturer name

One further expansion of the database would be to include web log data from a solar installer website.  This would be things like data from the [fingerprintjs2](https://github.com/Valve/fingerprintjs2) JavaScript library (e.g. user agent), IP address and location, visit time, length of visit on site, and any 'conversion' metrics like did the user subscribe, ask for more information, or purchase something.  Then this web log data could be combined with the other data to understand how to best convert users on the site, and could be used with machine learning and backend coding to serve personalized content to users visiting the site.

## Data Pipeline

### Quality Checks
- need at least 2

## 

## DWH System 

https://hevodata.com/blog/google-bigquery-pricing/

bigquery faster than AWS athena: https://medium.com/cloudwithmore/aws-athena-vs-google-bigquery-81a5e885d5c6

I chose BigQuery because it makes sense for this context.  Our data is not that large, and we probably won't do that many queries on it since it's business analytics (not real-time web analytics/ML).  But if we were to integrate web visits from a site we run, we would probably want to move to RedShift.

https://blog.panoply.io/a-full-comparison-of-redshift-and-bigquery

If we had a website we were integrating with the data (i.e. we want to serve personalized messages to visitors depending on their location and demographics, and store related data from web visits), then we might want to reconsider our DWH solution and maybe use RedShift for it's scalability and in-memory processing capabilities.  This would allow us to do things like heavy analysis of web logs, ML with SageMaker, 

## Addressing Other Scenarios
As per the project requirements, here are summaries of addressing other scenarios:

**What if:**
### The data was increased by 100x.


### The pipelines would be run on a daily basis by 7 am every day.


### The database needed to be accessed by 100+ people.


## Misc
I used the [Visual Paradigm](https://www.visual-paradigm.com/download/) tool to convert my SQL CREATE statements (DDL statements) into an ERD.  At first I was manually creating the ERD with [wwwsqldesigner](https://github.com/ondras/wwwsqldesigner), but found it tedious to enter everythin by hand.



## Instructions for running IaC (infrastructure as code)

You first must set up a new user with administrative rights.  Go to the AWS console, then IAM, then Users, and create a new user with "AdministratorAccess".  There may be a way to do this with less than admin rights, however.  Then set the .cfg file from the starter .cfg is the repo.  After downloading the credentials for the admin role, set these as KEY and SECRET in the cfg file.  Then run `ipython` and `create_redshift_cluster.py`.  When finished with the cluster, run `delete_cluster.py`.  As long as the config file stays the same, the Redshift cluster identifier will be the same, and you'll be deleting the same cluster you created.