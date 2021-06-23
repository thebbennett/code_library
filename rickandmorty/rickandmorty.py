# import packages
import requests
import pdb

#define paramters
base_url="https://rickandmortyapi.com/api/"
character_url=base_url+"character/"
location_url=base_url+"location/"
episode_url=base_url+"episode/"

url_list = [character_url, location_url, episode_url]

def get_json(url):
    json_data = requests.get(url).json()
    return json_data 

def get_results(json_blob):
    json_blob["results"]

# read in API call in JSON
d = {}
for url in url_list:
    d["{0}_api".format(url)] = get_json(url)

print("Print dictionary")
print(d)
pdb.set_trace()
print("Should not show")
########
characters_api = get_json(character_url)
episodes_api = get_json(episode_url)
locations_api = get_json(location_url)

characters = []
new_results = True
page = 1
while new_results:
    characters_api = requests.get(character_url + f"/?page={page}").json()
    new_results = characters_api.get("results", [])
    characters.extend(new_results)
    page += 1

episodes = []
new_results = True
page = 1
while new_results:
    episodes_api = requests.get(episode_url + f"/?page={page}").json()
    new_results = episodes_api.get("results", [])
    episodes.extend(new_results)
    page += 1

locations = []
new_results = True
page = 1
while new_results:
    locations_api = requests.get(location_url + f"/?page={page}").json()
    new_results = locations_api.get("results", [])
    locations.extend(new_results)
    page += 1

# convert to data frame
character_df = json_normalize(characters)
episodes_df = json_normalize(episodes)
locations_df = json_normalize(locations)
# Clean dataframes
character_df['episode_id'] = character_df['episode'].astype(str)\
         .str.replace('https://rickandmortyapi.com/api/episode/', '').apply(ast.literal_eval)
character_df.rename(columns={'id':'character_id'}, inplace=True)

locations_df['character_id'] = locations_df['residents'].astype(str)\
         .str.replace('https://rickandmortyapi.com/api/character/', '').apply(ast.literal_eval)

locations_df.rename(columns={'id':'location_id'}, inplace=True)
## episode character lookup
character_episode_lookup = character_df[['character_id', 'episode_id']]
character_episode_lookup = character_episode_lookup.explode('episode_id').reset_index(drop=True)

## Location / character lookup
location_character_lookup = locations_df[['location_id', 'character_id']]
character_episode_lookup = character_episode_lookup.explode('character_id').reset_index(drop=True)
#save to CSVS
character_df.to_csv('data/rick_and_morty_characters')
episodes_df.to_csv('data/rick_and_morty_episodes')
locations_df.to_csv('data/rick_and_morty_locations')
character_episode_lookup.to_csv('data/character_episode_lookup')
location_character_lookup.to_csv('data/location_character_lookup')
