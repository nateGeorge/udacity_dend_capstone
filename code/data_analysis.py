import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

import etl

conn, cur = etl.make_redshift_connection()

# What are cities with the top number of potential installs?

# get top potential installs by zipcode first as a simpler query
query = """
SELECT SUM(potential_installs) AS potential, zip_code
FROM solar_metrics
WHERE potential_installs IS NOT NULL
GROUP BY zip_code
ORDER BY potential DESC
LIMIT 100;
"""

df = pd.read_sql(query, conn)
sns.barplot(x='zip_code', y='potential', data=df.iloc[:10], order=df.iloc[:10]['zip_code'])
plt.ylabel('potential solar installs')
plt.show()

# Good start, but zipcodes aren't so helpful.  Which cities should we target?


# Top cities with most potential installs.
query = """SELECT z.city_name, z.state_name, SUM(sm.potential_installs) AS potential_installs
FROM solar_metrics sm
INNER JOIN zipcodes z
ON sm.zip_code=z.zip_code
WHERE sm.potential_installs IS NOT NULL
GROUP BY z.city_name, z.state_name
ORDER BY potential_installs DESC
LIMIT 100;
"""

df = pd.read_sql(query, conn)
df['city, state'] = df['city_name'] + ', ' + df['state_name']

sns.barplot(x='city, state',
            y='potential_installs',
            data=df.iloc[:10],
            order=df.iloc[:10]['city, state'],
            color='black')
plt.ylabel('potential solar installs')
plt.xticks(rotation=70)
plt.tight_layout()
plt.savefig('../images/installs_by_city.png')
plt.show()

"""
These seem to be large cities in the south and west, which makes sense.
There are a lot of houses there, and in Texas especially, energy is probably cheap.
"""

# get top 10 city, state for further analysis
top_10_cities = df.iloc[:10]['city_name'].values
top_10_states = df.iloc[:10]['state_name'].values
top_10_citystates = df.iloc[:10]['city, state']
top_10_tuples = list(zip(top_10_cities, top_10_states))

# How much money will people save?
# See what the energy cost is for these top cities:

query = """SELECT z.city_name, z.state_name, AVG(sm.average_yearly_electric_bill) AS average_bill
FROM solar_metrics sm
INNER JOIN zipcodes z
ON sm.zip_code=z.zip_code
WHERE sm.average_yearly_electric_bill IS NOT NULL
AND (z.city_name, z.state_name) IN(
    {}
)
GROUP BY z.city_name, z.state_name
LIMIT 100;
""".format(top_10_tuples).replace('[', '').replace(']', '')


df = pd.read_sql(query, conn)
df['city, state'] = df['city_name'] + ', ' + df['state_name']

sns.barplot(x='city, state',
            y='average_bill',
            data=df,
            order=top_10_citystates,
            color='black')
plt.xticks(rotation=70)
plt.tight_layout()
plt.savefig('../images/bill_by_top_cities.png')
plt.show()


# How much solar power could be generated in various cities?
# Which cities have the top solar power generation potential?
query = """SELECT z.city_name, z.state_name, SUM(sm.kw_median) AS solar_potential
FROM solar_metrics sm
INNER JOIN zipcodes z
ON sm.zip_code=z.zip_code
WHERE sm.kw_median IS NOT NULL
GROUP BY z.city_name, z.state_name
ORDER BY solar_potential DESC
LIMIT 100;
"""

df = pd.read_sql(query, conn)
df['city, state'] = df['city_name'] + ', ' + df['state_name']


sns.barplot(x='city, state',
            y='solar_potential',
            data=df.iloc[:10],
            order=df.iloc[:10]['city, state'],
            color='black')
plt.ylabel('potential solar kW generation per house')
plt.xticks(rotation=70)
plt.tight_layout()
plt.savefig('../images/kw_by_city.png')
plt.show()


# How much money do people have available in our top cities?
query = """SELECT z.city_name, z.state_name, AVG(sm.median_income) AS average_median_income
FROM solar_metrics sm
INNER JOIN zipcodes z
ON sm.zip_code=z.zip_code
WHERE sm.median_income IS NOT NULL
AND (z.city_name, z.state_name) IN(
    {}
)
GROUP BY z.city_name, z.state_name
ORDER BY average_median_income DESC
LIMIT 100;
""".format(top_10_tuples).replace('[', '').replace(']', '')

df = pd.read_sql(query, conn)
df['city, state'] = df['city_name'] + ', ' + df['state_name']


sns.barplot(x='city, state',
            y='average_median_income',
            data=df,
            order=top_10_citystates,
            color='black')
plt.ylabel('average of median income')
plt.xticks(rotation=70)
plt.tight_layout()
plt.savefig('../images/income_by_city.png')
plt.show()


# Where is the least competition?
# To answer this question, we should really add 'existing_installs_count' to the dataset.
# This could be from project sunroof, or calculated from the LBNL data.

# For now, we can look at which modules are mainly used in the top 10 cities.
# we should follow something like this to do this within redshift: https://stackoverflow.com/a/36888982/4549682
query = """SELECT z.city_name, z.state_name, i.installer_primary_module_manufacturer AS module_manufacturer
FROM solar_metrics sm
INNER JOIN zipcodes z
ON sm.zip_code=z.zip_code
JOIN installer i
ON sm.primary_installer_id=i.installer_id
WHERE (z.city_name, z.state_name) IN(
    {}
);
""".format(top_10_tuples).replace('[', '').replace(']', '')

df = pd.read_sql(query, conn)
df = df[df['module_manufacturer'] != '-9999']
df['city, state'] = df['city_name'] + ', ' + df['state_name']
module_modes = df.groupby('city, state').agg(lambda x: x.value_counts().index[0])
print(module_modes['module_manufacturer'])