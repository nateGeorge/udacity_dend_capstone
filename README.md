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


The data dictionary can be found in the data_dictionary.xlsx file, although it would be better to 


primary id key
zip_code
percent_qualified (percent of buildings qualified for solar installs) (sunroof)
number_potential_panels (number of potential solar panels for the area) (sunroof)
kw_median (kW of solar potential for the median building) (sunroof)
potential installs (count_qualified - existing_installs_count) (sunroof)
median income (ACS)
average_electric_bill (EIA)
average_kwh_used (EIA)
median_age (ACS)
owner_occupied_housing_units (ACS)
installer_ID (majority installer for that location) (LBNL)
battery_system (If the install has a battery system) (LBNL)
median_annual_feedin_tariff (Feed-in Tariff (Annual Payment)) (LBNL)


The dimension tables include:

location table: zipcode, city name, state name, latitude, longitude
utility table: zipcode, utility name, ownership, service type
installer table: installer ID, installer name, installer primary module manufacturer

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
module manufacturer table: manufacturer code, manufacturer name

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