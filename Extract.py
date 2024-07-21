#!/usr/bin/env python3

from logger_config import main_logger
import requests
import sys
import json

# Function to fetch recipes from the Spoonacular API
def fetch_recipes(url, api_key, offset, number):
    params = {
        'apiKey': api_key, 
        'number': number,  # The number of records to fetch in one request.
        'offset': offset,   # The number of records to skip (used for pagination).
        'addRecipeInformation': True,
        'instructionsRequired': True,
        'fillIngredients': True,
        'addRecipeInstructions': True,
        'addRecipeNutrition': True
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad status codes
    data = response.json() 
    return data

main_logger.info('Starting data extraction')
# Set your API key
API_KEY = 'c724b4224ab24504ad57792b2acd2605'

# Set your API URL
API_URL='https://api.spoonacular.com/recipes/complexSearch'

# Check if the offset and titles_set arguments are provided
if len(sys.argv) < 3:
    main_logger.error('offset or titles_set argument is missing.')
    sys.exit(1)

# Get the offset and titles_set from the command-line arguments
try:
    offset = int(sys.argv[1])
    main_logger.info(f"initial offset: {offset}")
except Exception as e:
    main_logger.error(f"Failed to extract initial offset:{e}", exc_info=True)

try:
    titles_set = set(sys.argv[2].split('\n')) if sys.argv[2] else set()
    main_logger.debug(f"initial titles_set: {titles_set}")
except Exception as e:
    main_logger.error(f"Failed to extract initial offset:{e}", exc_info=True)

# Fetch 50 recipes
try:
    NUMBER = 50
    execution_batch = fetch_recipes(API_URL, API_KEY, offset, NUMBER)
    main_logger.debug(f"Extracted {NUMBER} recipes")
except Exception as e:
    main_logger.error(f"Failed to extract recipes:{e}", exc_info=True)

# Filter out recipes that have titles already in titles_set
try:
    filtered_recipes = [recipe for recipe in execution_batch['results'] if recipe['title'] not in titles_set]
    main_logger.debug("Filtered recipes")
except Exception as e:
    main_logger.error(f"Failed to Filter recipes:{e}", exc_info=True)

# Extract titles of the execution batch to ensure the next batch is different
try:
    unique_new_titles = {recipe['title'] for recipe in filtered_recipes}
    titles_set.update(unique_new_titles)
    main_logger.debug("titles_set updated")
except Exception as e:
    main_logger.error(f"Failed to update titles_set:{e}", exc_info=True)

offset += NUMBER
main_logger.info(f"intermediate offset: {offset}")

# Check if we need more recipes to reach 100 unique recipes
while len(filtered_recipes) < NUMBER:
    try:
        remaining_needed = NUMBER - len(filtered_recipes)
        more_recipes = fetch_recipes(api_key, offset, remaining_needed)
        main_logger.debug(f"Extracted more {remaining_needed} recipes")
        filtered_more_recipes = [recipe for recipe in more_recipes['results'] if recipe['title'] not in titles_set]
        main_logger.debug("Filtered more recipes")
        filtered_recipes.extend(filtered_more_recipes)
        main_logger.debug("Extended filtered_recipes")
        unique_new_titles = {recipe['title'] for recipe in filtered_more_recipes}  # Ensure only needed titles are added
        titles_set.update(unique_new_titles)
        main_logger.debug("titles_set updated")
        offset += len(more_recipes) # Because the first extracted recipes are already used
        main_logger.info(f"intermediate offset: {offset}")
        if len(filtered_more_recipes) == 0:
            break  # Stop if there are no more unique recipes available
    except Exception as e:
        main_logger.error(f"Failed to extract more recipes: {e}", exc_info=True)

# Write the updated offset to the new_offset.txt file, each title on a new line
try:
    with open('new_offset.txt', 'w') as file:
        file.write(f"{offset}\n")     
    main_logger.debug('offset data written to file')     
except Exception as e:
    main_logger.error(f"Failed to write offset data to file:{e}", exc_info=True)

# Write the updated titles_set to the new_titles_set.txt file, each title on a new line
try:
    with open('new_titles_set.txt', 'w') as file:
        for title in titles_set:
            file.write(f"{title}\n")
    main_logger.debug('Unique titles written to file') 
except Exception as e:
    main_logger.error(f"Failed to write Unique titles to file:{e}", exc_info=True)

# Write the filtered_recipes to the new_recipes.txt file
try:
    with open('new_recipes.txt', 'w') as file:
        json.dump(filtered_recipes, file, indent=4)  
        #The json.dump function in Python is used to serialize a Python object (in this case, a list of dictionaries) to a JSON formatted string and write it to a file. 
    main_logger.info('Extracted data written to file')   
except Exception as e:
    main_logger.error(f"Failed to write Extracted data to file:{e}", exc_info=True)

