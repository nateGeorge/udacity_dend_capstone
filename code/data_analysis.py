import os
import configparser

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras

import etl

conn, cur = etl.make_redshift_connection()

# What are cities with the top number of potential installs?
# get top potential installs by zipcode
query = """
SELECT SUM(potential_installs) AS potential, zip_code
FROM solar_metrics
WHERE potential_installs IS NOT NULL
GROUP BY zip_code
ORDER BY potential DESC
LIMIT 10;
"""

query = """SELECT z.city_name, z.state_name, SUM(sm.potential_installs) AS potential_installs
FROM solar_metrics sm
INNER JOIN zipcodes z
ON sm.zip_code=z.zip_code
WHERE sm.potential_installs IS NOT NULL
GROUP BY z.city_name, z.state_name
ORDER BY potential_installs DESC
LIMIT 20;
"""

query = """SELECT SUM(potential_installs) AS potential, zip_code
FROM solar_metrics
GROUP BY zip_code
ORDER BY potential;
"""

cur.execute(query)

res = cur.fetchall()

# How much solar power could be generated in various cities?

# How much money do people have available?

# How much money will people save?

# Where is the least competition?