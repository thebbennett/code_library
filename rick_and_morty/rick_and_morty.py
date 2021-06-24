import requests
import csv
import pdb

# define paramaters
BASE_URL = "https://rickandmortyapi.com/api/"
TARGET_DIR = "target"


def get_results(endpoint):
    """
    This function hits the API endpoint, gets the whole set of results and exports it as a CSV
    """
    print(f"Starting {endpoint} export")
    url = BASE_URL + endpoint + "/"

    results = []
    page = 1
    while True:
        response = requests.get(url + f"/?page={page}").json()

        results.extend(response.get("results", []))
        if response.get("info").get("next"):
            # Rather than inferring the page number, should we instead extract it from the `next` key?
            page += 1
        else:
            break
    num_results = len(results)
    print(f"Finished {endpoint} export, got {len(results)} results")
    return results


def export_to_csv(array, filename):
    """
    Takes an array of dictionaries and writes to a CSV file
    """
    keys = array[0].keys()
    with open(f"{TARGET_DIR}/{filename}.csv", "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(array)


# # convert to data frame
# character_df = json_normalize(characters)
# episodes_df = json_normalize(episodes)
# locations_df = json_normalize(locations)
# # Clean dataframes
# character_df["episode_id"] = (
#     character_df["episode"]
#     .astype(str)
#     .str.replace("https://rickandmortyapi.com/api/episode/", "")
#     .apply(ast.literal_eval)
# )
# character_df.rename(columns={"id": "character_id"}, inplace=True)

# locations_df["character_id"] = (
#     locations_df["residents"]
#     .astype(str)
#     .str.replace("https://rickandmortyapi.com/api/character/", "")
#     .apply(ast.literal_eval)
# )

# locations_df.rename(columns={"id": "location_id"}, inplace=True)
# ## episode character lookup
# character_episode_lookup = character_df[["character_id", "episode_id"]]
# character_episode_lookup = character_episode_lookup.explode("episode_id").reset_index(
#     drop=True
# )

# ## Location / character lookup
# location_character_lookup = locations_df[["location_id", "character_id"]]
# character_episode_lookup = character_episode_lookup.explode("character_id").reset_index(
#     drop=True
# )
# # save to CSVS
# character_df.to_csv("target/rick_and_morty_characters")
# episodes_df.to_csv("target/rick_and_morty_episodes")
# locations_df.to_csv("target/rick_and_morty_locations")
# character_episode_lookup.to_csv("target/character_episode_lookup")
# location_character_lookup.to_csv("target/location_character_lookup")


def extract_id_from_url(url):
    if url == '':
        return None
    else:
        id = int(url.split("/")[-1])
        return id



def character_cleanup(character_api_result):
    character = character_api_result

    episode_ids = []
    for episode_url in character["episode"]:
        episode_id = extract_id_from_url(episode_url)
        episode_ids.append(episode_id)

    character["episode_ids"] = episode_ids

    character.pop("episode")

    location_id = extract_id_from_url(character["location"]["url"])

    character["location_id"] = location_id

    character.pop("location")

    return character


if __name__ == "__main__":
    endpoints = ["character", "location", "episode"]

    # TODO: Now that we have CSVs of data, we might need to clean up some of the nested objects
    # for endpoint in endpoints:
    #     results = get_results(endpoint)
    #     export_to_csv(results, endpoint)

    # characters
    characters_api_results = get_results("character")
    characters = []
    for character_api_result in characters_api_results:
        character = character_cleanup(character_api_result)
        characters.append(character)

    export_to_csv(characters, "characters")

    # TODO: create character_episodes table and character_locations
