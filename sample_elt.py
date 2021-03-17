
# import packages
import pandas as pd
from parsons import Redshift, Table, VAN, S3, utilities
from requests.exceptions import HTTPError
from pandas.io.json import json_normalize


#CIVIS enviro variables 
## as if I were going to load these resultant dataframes into Civis

os.environ['REDSHIFT_PORT']
os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
os.environ['REDSHIFT_HOST']
os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME'] 
os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD'] 
os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'
os.environ['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']
van_key = os.environ['VAN_PASSWORD']

#define paramters
base_url="https://rickandmortyapi.com/api/"
character_url=base_url+"character/"
location_url=base_url+"location/"
episode_url=base_url+"episode/"


# read in API call in JSON
character_json = requests.get(character_url).json()

episodes_json = requests.get(episode_url).json()

locations_json = requests.get(location_url).json()


# convert to data frame
character_df = json_normalize(character_json['results'])
episodes_df = json_normalize(episodes_json['results'])
locations_df = json_normalize(locations_json['results'])

#save to CSVS
character_df.to_csv('data/rick_and_morty_characters')
episodes_df.to_csv('data/rick_and_morty_episodes')
locations_df.to_csv('data/rick_and_morty_locations')

# conver to Parsons tables
character_table = Table.from_dataframe(character_df)
episodes_table = Table.from_dataframe(episodes_df)
locations_table = Table.from_dataframe(locations_df)

# move to Redshift
rs.copy(character_table, 'indivisible.rick_and_morty_characters' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)
rs.copy(episodes_table, 'indivisible.rick_and_morty_episodes' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)
rs.copy(locations_table, 'indivisible.rick_and_morty_locations' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)
