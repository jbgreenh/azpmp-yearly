import os
import re
import sys

import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()

fips_txt = requests.get('https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt').content.decode()
sections = re.split(r'-+\s+-+', fips_txt)

def parse_fips_data(section:str, col_names:list) -> pl.DataFrame:
    data_dict = {col_names[0]: [], col_names[1]: []}
    for line in section.strip().splitlines():
        if not line:
            break
        row = re.split(r'\s{2,}', line.strip())
        data_dict[col_names[0]].append(row[0])
        data_dict[col_names[1]].append(row[1])
    return pl.DataFrame(data_dict)

fips_state = parse_fips_data(sections[1], ['fips', 'state'])
fips = parse_fips_data(sections[2], ['fips', 'county'])

CENSUS_API_KEY = os.environ.get('CENSUS_API_KEY', 'CENSUS_API_KEY missing from .env file')
url = 'https://api.census.gov/data/2023/acs/acs5'   # may need to update as new census completed 2028
params = {
    'get':'B01003_001E',
    'for':'county:*',
    'in':f'state:{fips_state.filter(pl.col('state') == 'ARIZONA')['fips'].item()}',
    'key':CENSUS_API_KEY
}
county_pop_df = pl.DataFrame()
response = requests.get(url, params=params)
if response.status_code == 200:
    data = response.json()
    county_pop_dict = {'county':[], 'population':[]}
    counties = data[1:]
    for county in counties:
        population, state, county = county
        county_pop_dict['population'].append(int(population))
        county_pop_dict['county'].append(f'{state}{county}')
        county_pop_df = pl.DataFrame(county_pop_dict)
else:
    sys.exit(f'error: {response.status_code}, {response.text}')

pop = (
    county_pop_df.join(fips, left_on='county', right_on='fips', how='left', coalesce=True)
    .select(
        pl.col('county_right').str.to_uppercase().str.strip_suffix(' COUNTY').alias('county'),
        pl.col('population')
    )
)
