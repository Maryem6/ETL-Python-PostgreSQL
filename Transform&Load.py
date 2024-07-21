#!/usr/bin/env python3

import json
import pandas as pd
from logger_config import main_logger
import psycopg2
import numpy as np

#----------------------------------------------------------------
####################### Transform #######################
#----------------------------------------------------------------

#----------------------------------------------------------------
# 1. load_json_to_dataframe
#----------------------------------------------------------------
try:
    with open('recipes_to_transform.txt', 'r') as file:
        data = json.load(file)
    main_logger.info('Successfully loaded data from json_file')
    df = pd.DataFrame(data)
    main_logger.debug(df.head())
    main_logger.debug(df.info())
except Exception as e:
    main_logger.error(f"Error loading data from recipes_to_transform.txt: {e}", exc_info=True)

#----------------------------------------------------------------
# 2. Dealing with recipes
#----------------------------------------------------------------

# Define the required columns for the dataframe
dfrecipes_COLUMNS = [
    'vegetarian', 'vegan', 'glutenFree', 'dairyFree', 'veryHealthy', 
    'cheap', 'veryPopular', 'sustainable', 'lowFodmap', 'pricePerServing', 
    'title', 'readyInMinutes', 'servings', 'sourceUrl', 'summary', 'license'
]

# Construct the dataframe
try:
    dfrecipes = pd.DataFrame(df, columns=dfrecipes_COLUMNS)
    main_logger.info(f"Constructed DataFrame with {len(dfrecipes)} recipes.")
    main_logger.debug(dfrecipes.head())
    main_logger.debug(dfrecipes.info())
except Exception as e:
    main_logger.error(f"Failed to construct DataFrame: {e}", exc_info=True)

# Rename columns for clarity and consistency
COLUMNS_TO_RENAME_MAP = {
    'vegetarian': 'is_vegetarian', 'vegan': 'is_vegan',
    'glutenFree': 'is_glutenFree', 'dairyFree': 'is_dairyFree',
    'veryHealthy': 'is_healthy', 'cheap': 'is_cheap',
    'veryPopular': 'is_Popular', 'sustainable': 'is_sustainable',
    'lowFodmap': 'is_lowFodmap', 'pricePerServing': 'price_per_serving', 
    'readyInMinutes': 'ready_min', 'sourceUrl': 'source_url', 
    'title': 'recipe_title'
}

try:
    dfrecipes = dfrecipes.rename(columns=COLUMNS_TO_RENAME_MAP)
    main_logger.info("Renamed columns.")
except Exception as e:
    main_logger.error(f"Failed to rename columns: {e}", exc_info=True)

# Extract a counter from which we start the id range
try:
    with open('counters.txt', 'r') as file:
        lines = file.readlines()
        if len(lines) > 0:
            counter_recipe = int(lines[0].strip())
        else:
            counter_recipe = 1
    main_logger.info(f"Counter recipe: {counter_recipe}")
except Exception as e:
    main_logger.error(f"Failed to extract counter_recipe: {e}", exc_info=True)

# Generate 'id_recipe' column
try:
    dfrecipes['id_recipe'] = range(counter_recipe, counter_recipe + len(dfrecipes))
    main_logger.info("Added 'id_recipe'.")
    main_logger.debug(dfrecipes.head())
    main_logger.debug(dfrecipes.info())
except Exception as e:
    main_logger.error(f"Failed to add 'id_recipe' column: {e}", exc_info=True)

# Append the counter content to the file
try:
    counter_recipe = counter_recipe + len(dfrecipes)
    with open('new_counters.txt', 'a') as file:
        file.write(str(counter_recipe) + "\n")
    print(f"'counter_recipe' appended to {file.name} successfully.")
except Exception as e:
    print(f"Error appending to file: {e}")

# Create a mapping dictionary from dfrecipes
try:
    RECIPE_MAPPING = dfrecipes.set_index('recipe_title')['id_recipe'].to_dict()
    main_logger.info("Created 'RECIPE_MAPPING'.")
except Exception as e:
    main_logger.error(f"Failed to create 'RECIPE_MAPPING': {e}", exc_info=True)

#----------------------------------------------------------------
# 3. Dealing with ingredients
#----------------------------------------------------------------
# 3.1 Reading Existing Ingredient Mapping
#----------------------------------------------------------------
# Function to read existing ING_MAPPING.txt
def read_ingredient_mapping(file_path):
    try:
        with open(file_path, 'r') as file:
            ing_mapping = json.load(file)
        return set(ing_mapping.keys()), ing_mapping
    except FileNotFoundError:
        main_logger.warning(f"{file_path} not found. Starting with an empty set.")
        return set(), {}
    except Exception as e:
        main_logger.error(f"Failed to read ING_MAPPING file: {e}", exc_info=True)
        return set(), {}

# Read existing ING_MAPPING.txt
existing_ingredients, existing_ingredients_mapping = read_ingredient_mapping('ING_MAPPING.txt')

#----------------------------------------------------------------
# 3.2 Extracting Ingredients from DataFrame
#----------------------------------------------------------------
# Define the required columns for the ingredients DataFrame
dfIng_COLUMNS = ['consistency', 'nameClean', 'aisle', 'name']

# Define a function to use it with the apply function
def extract_dicts(lst):
    """Extract dictionaries from a list of dictionaries and return a list"""
    return [d for d in lst if isinstance(d, dict)]

# Apply the function and sum the results
try:
    # 'all_ingredients' variable receives a list of all dictionaries in the extendedIngredients column
    all_ingredients = df['extendedIngredients'].apply(extract_dicts).sum()
    main_logger.info(f"Extracted {len(all_ingredients)} ingredients.")
except Exception as e:
    main_logger.error(f"Failed  extract ingredient dictionaries: {e}", exc_info=True)
    all_ingredients = []

#----------------------------------------------------------------
# 3.3 Ingredients DataFrame
#----------------------------------------------------------------
# Construct the ingredients DataFrame
try:
    dfIng = pd.DataFrame(all_ingredients, columns=dfIng_COLUMNS)
    main_logger.info(f"Constructed ingredients DataFrame with {len(dfIng)} ingredients.")
    main_logger.debug(dfIng.head())
    main_logger.debug(dfIng.info())
except Exception as e:
    main_logger.error(f"Failed to construct ingredients DataFrame: {e}", exc_info=True)
    dfIng = pd.DataFrame(columns=dfIng_COLUMNS)

# Fill missing 'nameClean' values with 'name' values
try:
    none_rows = dfIng[dfIng['nameClean'].isna()]
    main_logger.debug(none_rows)
    if not none_rows.empty:
        main_logger.warning(f"Found {len(none_rows)} null nameClean. Filling with name.")
        main_logger.debug(none_rows)
        dfIng['nameClean'] = dfIng['nameClean'].fillna(dfIng['name'])
        main_logger.info("Filled missing 'nameClean' values.")
    else:
        main_logger.info("No null 'nameClean' found.")
    main_logger.debug(dfIng.info())

    dfIng.drop('name', axis=1, inplace=True)
    main_logger.info("Dropped 'name' column.")
    main_logger.debug(dfIng.info())
except Exception as e:
    main_logger.error(f"Failed to handle null values: {e}", exc_info=True)

# Handle duplicates
try:
    duplicates = dfIng[dfIng.duplicated()]
    main_logger.debug(duplicates)
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate ingredients. Removing duplicates.")
        dfIng = dfIng.drop_duplicates().reset_index(drop=True)
        main_logger.info(f"DataFrame now contains {len(dfIng)} ingredients after removing duplicates.")
    else:
        main_logger.info("No duplicates found.")
    main_logger.debug(dfIng.info())
except Exception as e:
    main_logger.error(f"Failed to handle duplicate values: {e}", exc_info=True)

# Assign existing ingredient IDs and handle new ingredients
try:
    # Map existing ingredient IDs based on the nameClean column
    dfIng['id_ingredient'] = dfIng['nameClean'].map(existing_ingredients_mapping)

    # Identify new ingredients
    new_ingredients = dfIng[dfIng['id_ingredient'].isna()]

    # Extract a counter from which we start the id range
    with open('counters.txt', 'r') as file:
        lines = file.readlines()
        if len(lines) > 1:
            counter_ing = int(lines[1].strip())
            main_logger.info(f"Counter ingredients: {counter_ing}")
        else:
            counter_ing = 1
            main_logger.info(f"Counter ingredients initialized: {counter_ing}")

    # Assign new IDs to new ingredients
    new_ingredients['id_ingredient'] = range(counter_ing, counter_ing + len(new_ingredients))
    main_logger.info("New IDs assigned to new ingredients")

    # Combine existing and new ingredients
    dfIng.update(new_ingredients)
    main_logger.info("New ingredients IDs combined with existing ingredients.")

    # Convert id_ingredient to integer type where applicable, keeping None values
    dfIng['id_ingredient'] = dfIng['id_ingredient'].astype('Int64')
    main_logger.info("'id_ingredient' column converted to Integer")

except Exception as e:
    main_logger.error(f"Failed to assign IDs or combine ingredients: {e}", exc_info=True)

# Append the counter content to the file
try:
    counter_ing += len(new_ingredients)
    main_logger.info(f"Counter ingredients incremented: {counter_ing}")
    with open('new_counters.txt', 'a') as file:
        file.write(str(counter_ing) + "\n")
    main_logger.info(f"'counter_ing' appended to {file.name} successfully.")
except Exception as e:
    main_logger.error(f"Error appending to file: {e}", exc_info=True)

# Rename 'nameClean' to 'ing_name'
try:
    dfIng = dfIng.rename(columns={'nameClean': 'ing_name'})
    main_logger.info("Renamed 'nameClean' to 'ing_name'.")
    main_logger.debug(dfIng.info())
except Exception as e:
    main_logger.error(f"Failed to rename columns: {e}", exc_info=True)

# Create a mapping dictionary from dfIng
try:
    new_ingredients_mapping = dfIng.set_index('ing_name')['id_ingredient'].to_dict()
    main_logger.info("Created 'new_ingredients_mapping'.")
except Exception as e:
    main_logger.error(f"Failed to create 'new_ingredients_mapping': {e}", exc_info=True)

# Update ING_MAPPING with new ingredients
try:
    ING_MAPPING = {**existing_ingredients_mapping, **new_ingredients_mapping}
    with open('ING_MAPPING.txt', 'w') as file:
        json.dump(ING_MAPPING, file)
    main_logger.info("Updated and saved 'ING_MAPPING'.")
except Exception as e:
    main_logger.error(f"Failed to update and save 'ING_MAPPING': {e}", exc_info=True)

    
#----------------------------------------------------------------
# 4. Dealing with measures (reference_ing)
#----------------------------------------------------------------
# 4.1 Extract the column that contains all the ingredients measures along with the recipes titles
#----------------------------------------------------------------
try:
    dfrecipeIng = pd.DataFrame(df, columns=['extendedIngredients', 'title'])
    main_logger.info(f"Extracted ingredients and titles for {len(dfrecipeIng)} recipes.")
    main_logger.debug(dfrecipeIng.head())
    main_logger.debug(dfrecipeIng.info())
except Exception as e:
    logger.error("Failed to extract ingredients and titles", exc_info=True)

#----------------------------------------------------------------
# 4.2 Obtaining a new dataframe that contains measures of ingredients in each recipe
#----------------------------------------------------------------

# Define the required columns for the measures DataFrame
dfmeasures_COLUMNS =['nameClean', 'name', 'measures', 'title']

# Initialize an empty list to collect rows of data
rows_data = []

# Iterate through each row of the original DataFrame
for i, row_series in dfrecipeIng.iterrows():
    try:
        # Access the recipe title for the current row 
        title = row_series['title']  # title is a string
        
        # Access the 'extendedIngredients' column for the current row
        recipe_ingredients_list = row_series['extendedIngredients']  # recipe_ingredients_list is a list
    
        # if recipe_ingredients_list is not an empty list skip the iteration (according to the Guard clauses principle of clean code)
        if not (recipe_ingredients_list and isinstance(recipe_ingredients_list, list)):
            continue
            
        # Iterate through each dictionary in the list
        for each_dict in recipe_ingredients_list:
            if each_dict and isinstance(each_dict, dict):  # Check if each_dict is non-empty and is a dictionary
                # Get measures dict 
                measures_dict = each_dict.get('measures', {})
                # Transform measures dict to a list
                measures_list = [measures_dict]       
            else:
                measures_list = None
            
            # Create a dictionary for the row data
            row_data = {
                'nameClean': each_dict.get('nameClean', None),
                'measures': measures_list,
                'name': each_dict.get('name'),
                'title': title
            }
            
            # Append the row data to the list
            rows_data.append(row_data)
    except Exception as e:
        main_logger.error(f"Error processing row {i} for recipe '{title}'", exc_info=True)

# Create the DataFrame from the list of row_data
try:
    dfmeasures = pd.DataFrame(rows_data, columns=dfmeasures_COLUMNS)
    main_logger.info(f"Constructed measures DataFrame with {len(dfmeasures)} rows.")
    main_logger.debug(dfmeasures.head())
    main_logger.debug(dfmeasures.info())
except Exception as e:
    main_logger.error("Failed to construct measures DataFrame", exc_info=True)

# Handle missing values in the nameClean column
try:
    none_rows = dfmeasures[dfmeasures['nameClean'].isna()]
    main_logger.debug(none_rows)
    if not none_rows.empty:
        main_logger.warning(f"Found {len(none_rows)} null nameClean. Filling with name.")
        dfmeasures['nameClean'] = dfmeasures['nameClean'].fillna(dfmeasures['name'])
        main_logger.info("Filled missing 'nameClean' values.")
    else:
        main_logger.info("No null 'nameClean' found.")
except Exception as e:
    main_logger.error("Failed to handle null values in 'nameClean'", exc_info=True)

# Drop the 'name' column after filling 'nameClean'
try:
    dfmeasures.drop('name', axis=1, inplace=True)
    main_logger.info("Dropped 'name' column.")
except Exception as e:
    main_logger.error("Failed to drop 'name' column", exc_info=True)

# Rename 'nameClean' to 'ing_name'
try:
    dfmeasures = dfmeasures.rename(columns={'nameClean': 'ing_name'})
    main_logger.info("Renamed 'nameClean' to 'ing_name'.")
    main_logger.debug(dfmeasures.info())
except Exception as e:
    main_logger.error("Failed to rename columns", exc_info=True)

#----------------------------------------------------------------
# 4.3 transforming the measures column
#----------------------------------------------------------------

# Function to extract measure information from measures dict
def extract_measure(measures_dict, key_name):
  """
  Extracts a string value representing the measure from the given dictionary using a given name.
  Handles cases where the key name might be different.

  Args:
      measures_dict: A dictionary containing the measure information.
      key_name: The key name to look for.

  Returns:
      A string representing the measure in the format "amount unitShort".
  """
  if key_name in measures_dict:
    return f"{measures_dict[key_name]['amount']} {measures_dict[key_name]['unitShort']}"
  else:
    # Handle cases where the key might be different
    for key in measures_dict:
      if isinstance(measures_dict[key], dict):
        return extract_measure(measures_dict[key], key)
    # If no matching key is found, return an empty string
    return None

# Create two new columns with extracted measures
try:
    dfmeasures['measure_1'] = dfmeasures['measures'].apply(lambda x: extract_measure(x[0], "us"))
    dfmeasures['measure_2'] = dfmeasures['measures'].apply(lambda x: extract_measure(x[0], "metric"))
    main_logger.info("Created columns 'measure_1' and 'measure_2'.")
except Exception as e:
    logger.error("Failed to create 'measure_1' and 'measure_2' columns", exc_info=True)

# Drop the original 'measures' column
try:
    dfmeasures.drop('measures', axis=1, inplace=True)
    main_logger.info("Dropped 'measures' column.")
    main_logger.debug(dfmeasures.head())
    main_logger.debug(dfmeasures.info())
except Exception as e:
    main_logger.error("Failed to drop 'measures' column", exc_info=True)

# Function to combine measures from 'measure_1' and 'measure_2'
def combine_measures(row):
  """
  Combines values from 'measure_1' and 'measure_2' columns into a single string.

  Args:
      row: A pandas Series representing a row of the DataFrame.

  Returns:
      A string containing the combined measure value(s).
  """
  measure_1 = row['measure_1']
  measure_2 = row['measure_2']

  if measure_1 == measure_2:
    return measure_1  # Same values, return one
  else:
    return f"{measure_1} / {measure_2}"  # Different values are concatenated with "/"

# Apply the function to create a new 'measure' column
try:
    dfmeasures['measure'] = dfmeasures.apply(combine_measures, axis=1)
    main_logger.info("Created 'measure' column by combining 'measure_1' and 'measure_2'.")
except Exception as e:
    main_logger.error("Failed to create 'measure' column", exc_info=True)

# Drop 'measure_1' and 'measure_2' columns
try:
    dfmeasures.drop(['measure_1', 'measure_2'], axis=1, inplace=True)
    main_logger.info("Dropped 'measure_1' and 'measure_2' columns.")
    main_logger.debug(dfmeasures.head())
    main_logger.debug(dfmeasures.info())
except Exception as e:
    logger.error("Failed to drop 'measure_1' and 'measure_2' columns", exc_info=True)

#----------------------------------------------------------------
# 4.4 dealing with id_recipe in dfmeasures
#----------------------------------------------------------------

# Function to map recipe titles to id_recipe
def map_with_none(recipe_title, RECIPE_MAPPING):
    """Maps recipe names to id_recipe, handling missing values."""
    if pd.isna(recipe_title):
        return None  # Return None for missing titles
    return RECIPE_MAPPING.get(recipe_title, None)  # Use get() to avoid KeyError for missing keys

try:
    # Apply the mapping function to create 'id_recipe' column
    dfmeasures['id_recipe'] = dfmeasures['title'].apply(map_with_none, args=(RECIPE_MAPPING,))
    main_logger.info("Added 'id_recipe' column based on 'RECIPE_MAPPING'.")
    # Convert id_recipe to integer type where applicable, keeping None values
    dfmeasures['id_recipe'] = dfmeasures['id_recipe'].astype('Int64')
    main_logger.info("Converted 'id_recipe' column to Integer type.")
    main_logger.debug(dfmeasures.head())
    main_logger.debug(dfmeasures.info())
except Exception as e:
    main_logger.error("Failed to add or convert 'id_recipe' column", exc_info=True)

#----------------------------------------------------------------
# 4.5 dealing with id_ingredient in dfmeasures
#----------------------------------------------------------------

# Function to map ingredient names to id_ingredient
def map_with_none(ing_name, ING_MAPPING):
    """Maps ingredient names to id_ingredient, handling missing values."""
    if pd.isna(ing_name):
        return None  # Return None for missing titles
    return ING_MAPPING.get(ing_name, None)  # Use get() to avoid KeyError for missing keys
    
try:
    # Apply the mapping function to create 'id_ingredient' column
    dfmeasures['id_ingredient'] = dfmeasures['ing_name'].apply(map_with_none, args=(ING_MAPPING,))
    main_logger.info("Added 'id_ingredient' column based on 'ING_MAPPING'.")
    # Convert id_ingredient to integer type where applicable, keeping None values
    dfmeasures['id_ingredient'] = dfmeasures['id_ingredient'].astype('Int64')
    main_logger.info("Converted 'id_ingredient' column to Integer type.")
    main_logger.debug(dfmeasures.head())
    main_logger.debug(dfmeasures.info())
except Exception as e:
    main_logger.error(f"Failed to add or convert 'id_ingredient' column: {e}", exc_info=True)

#----------------------------------------------------------------
# 4.6 Some Cleaning
#----------------------------------------------------------------

# Drop 'ing_name' and 'title' columns to finalize the reference DataFrame
try:
    dfreference_ing = dfmeasures.drop(['ing_name', 'title'], axis=1)
    main_logger.info("Dropped 'ing_name' and 'title' columns.")
    main_logger.debug(dfreference_ing.head())
    main_logger.debug(dfreference_ing.info())
except Exception as e:
    logger.error("Failed to finalize reference DataFrame by dropping 'ing_name' and 'title' columns", exc_info=True)


try:
    # Find duplicates
    duplicates = dfreference_ing[dfreference_ing.duplicated()]
    main_logger.debug(duplicates)
    # Remove duplicates
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate reference ingredients. Removing duplicates.")
        dfreference_ing = dfreference_ing.drop_duplicates().reset_index(drop=True)
        main_logger.info(f"DataFrame now contains {len(dfreference_ing)} reference ingredients after removing duplicates.")
    else:
        logger.info("No duplicates found.")
except Exception as e:
    main_logger.error("Failed to handle duplicates", exc_info=True)


#----------------------------------------------------------------
# 5. Dealing with steps
#----------------------------------------------------------------
# 5.1 Extract the column that contains all the instructions along with the recipes titles
#----------------------------------------------------------------
try:
    dfAllIns = pd.DataFrame(df, columns=['analyzedInstructions', 'title'])
    main_logger.info(f"Extracted instructions for {len(dfAllIns)} recipes.")
    main_logger.debug(dfAllIns.head())
    main_logger.debug(dfAllIns.info())
except Exception as e:
    main_logger.error(f"Failed to extract instructions and titles: {e}", exc_info=True)

#---------------------------------------------------------
# 5.2 obtaining a new dataframe that contains steps of each recipe
#---------------------------------------------------------

# Define the required columns for the steps DataFrame
dfsteps_COLUMNS = ['steps', 'title']

# Initialize an empty list to collect rows of data
rows_data = []

# Define the required columns for the steps DataFrame
for i, row_series in dfAllIns.iterrows():
    try:
        # Access the recipe title for the current row 
        title = row_series['title']  # title is a string
        
        # Access the 'analyzedInstructions' column for the current row
        instructions_list = row_series['analyzedInstructions']  # instructions_list is a list containing one dictionary
        
        if instructions_list and isinstance(instructions_list[0], dict):  # Check if instructions_list is non-empty and its first element is a dictionary
            # Get steps list
            steps_list = instructions_list[0].get('steps', [])
        else:
            steps_list = None
        
        # Create a dictionary for the row data
        row_data = {
            'steps': steps_list,
            'title': title
        }
        
        # Append the row data to the list
        rows_data.append(row_data)
    except Exception as e:
        logger.error(f"Error processing row {i} for recipe '{title}': {e}", exc_info=True)

# Create the DataFrame from the list of row data
try:
    dfsteps = pd.DataFrame(rows_data, columns=dfsteps_COLUMNS)
    main_logger.info(f"Constructed steps DataFrame for {len(dfsteps)} recipes.")
    main_logger.debug(dfsteps.head())
    main_logger.debug(dfsteps.info())
except Exception as e:
    main_logger.error(f"Failed to construct steps DataFrame: {e}", exc_info=True)


#---------------------------------------------------------
# 5.3 transforming the steps column
#---------------------------------------------------------

# Define the required columns for the step DataFrame
dfsteps_COLUMNS = ['equipment', 'step', 'length', 'number', 'title']

# Initialize an empty list to collect rows of data
rows_data = []

# Iterate through each row of the original DataFrame dfsteps
for i, row_series in dfsteps.iterrows():
    try:
        # Access the recipe title for the current row 
        title = row_series['title']  # title is a string
        
        # Access the 'steps' column for the current row
        steps_list = row_series['steps']
        
        if steps_list is not None:
            # Iterate through each dictionary in the steps_list
            for each_dict in steps_list:
                if each_dict and isinstance(each_dict, dict):  # Check if each_dict is non-empty and is a dictionary
                    # Extract relevant information from each_dict
                    number = each_dict.get('number', None)
                    step = each_dict.get('step', None)
                    time = each_dict.get('length', {}).get('number', None)
                    unit = each_dict.get('length', {}).get('unit', None)
                    
                    # Calculate length based on time and unit
                    if time is None or unit is None:
                        length = None
                    else:
                        length = f"{time} {unit}"
                    
                    equipment_list = each_dict.get('equipment', [])  # Get equipment list or empty list if 'equipment' is missing
                    
                    # Handle NaN or None values in equipment_list
                    if isinstance(equipment_list, list):
                        equipment_list = [e if pd.notna(e) else None for e in equipment_list]
                    
                    # Create a dictionary for the row data
                    row_data = {
                        'length': length,
                        'number': number,
                        'step': step,
                        'equipment': equipment_list,
                        'title': title
                    }
                    
                    # Append the row data to the list
                    rows_data.append(row_data)
        else:
            # If steps_list is None, create a row with None values
            row_data = {
                'length': None,
                'number': None,
                'step': None,
                'equipment': None,
                'title': title
            }
            
            # Append the row data to the list
            rows_data.append(row_data)
    except Exception as e:
        main_logger.error(f"Error processing row {i} for recipe '{title}': {e}", exc_info=True)

# Create the DataFrame from the list of row data
try:
    dfstep = pd.DataFrame(rows_data, columns=dfsteps_COLUMNS)
    main_logger.info(f"Constructed steps DataFrame with {len(dfstep)} steps.")
    main_logger.debug(dfstep.head())
    main_logger.debug(dfstep.info())
except Exception as e:
    main_logger.error(f"Failed to construct steps DataFrame: {e}", exc_info=True)

# Find duplicates
try:
    duplicates = dfstep[dfstep.duplicated(subset=["step", "length", "number", "title"])]
    main_logger.debug(duplicates)

    # Remove duplicates
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate steps. Removing duplicates.")
        dfstep = dfstep.drop_duplicates()
        dfstep.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfstep)} steps after removing duplicates.")
    else:
        main_logger.info("No duplicates found")
except Exception as e:
    main_logger.error(f"Failed to find or remove duplicates: {e}", exc_info=True)

# Checking if there's a Null values in step column
try:
    none_rows = dfstep[dfstep['step'].isna()]
    main_logger.debug(none_rows)

    # Deleting the null steps in dfstep DataFrame.
    if len(none_rows) > 0:
        main_logger.warning(f"Found {len(none_rows)} null steps. Deleting null steps.")
        dfstep.dropna(subset=['step'], inplace=True)
        dfstep.reset_index(drop=True, inplace=True)
        main_logger.info("Null steps deleted")
        main_logger.info(f"DataFrame now contains {len(dfstep)} steps after removing null values.")
    else:
        main_logger.info("No null steps found")
except Exception as e:
    main_logger.error(f"Failed to handle null steps: {e}", exc_info=True)

main_logger.debug(dfstep.head())
main_logger.debug(dfstep.info())   
    
#---------------------------------------------------------
# 5.4 transforming equipment column
#---------------------------------------------------------

# Initialize an empty list to collect rows of data
rows_data = []

# Iterate through each row of the original DataFrame dfstep
for i, row_series in dfstep.iterrows():
    try:
        # Access the recipe title for the current row 
        title = row_series['title']
        # Access the step length for the current row 
        length = row_series['length']
        # Access the recipe step for the current row 
        step = row_series['step']
        # Access the step number for the current row 
        number = int(row_series['number'])
        # Access the 'equipment' column for the current row
        equipments_list = row_series['equipment']  
        
        # Initialize variables to store processed data
        equipments_name_list = []
        
        # Process equipments_list
        if equipments_list and isinstance(equipments_list, list):
            for each_dict in equipments_list:
                if each_dict and isinstance(each_dict, dict):  # Check if each_dict is non-empty and is a dictionary
                    # Get a list containing equipments names
                    equipment_name = each_dict.get('name', None)
                    if equipment_name is not None:  # Ensure equipment_name is not None
                        equipments_name_list.append(equipment_name)
        else:
            equipments_name_list = None  # Handle case where equipments_list is None or not a list
        
        # Create a dictionary for the row data
        row_data = {
            'length': length,
            'number': number,
            'step': step,
            'equipment': equipments_name_list,
            'title': title
        }
        
        # Append the row data to the list
        rows_data.append(row_data)
    except Exception as e:
        logger.error(f"Error processing row {i} for recipe '{title}'", exc_info=True)

# Create the DataFrame from the list of row data
try:
    dfstepclean = pd.DataFrame(rows_data, columns=dfsteps_COLUMNS)
    main_logger.info(f"Constructed dfstepclean DataFrame with {len(dfstepclean)} steps and contains cleaned lists of equipments")
    main_logger.debug(dfstepclean.head())
    main_logger.debug(dfstepclean.info())
except Exception as e:
    main_logger.error(f"Failed to construct dfstepclean DataFrame: {e}", exc_info=True)

#---------------------------------------------------------
# 5.6 dealing with dfstep_final
#---------------------------------------------------------
# Extract a counter from which we start the id range
try:
    with open('counters.txt', 'r') as file:
        lines = file.readlines()
        if len(lines) > 2:
            counter_step = int(lines[2].strip())
        else:
            counter_step = 1
    main_logger.info(f"Counter step: {counter_step}")
except Exception as e:
    main_logger.error(f"Failed to extract counter_step: {e}", exc_info=True)

# Generate 'id_step' column
try:
    dfstepclean['id_step'] = range(counter_step, counter_step + len(dfstepclean))
    main_logger.info("Added 'id_step'.")
    main_logger.debug(dfstepclean.head())
    main_logger.debug(dfstepclean.info())
except Exception as e:
    main_logger.error(f"Failed to add 'id_recipe' column: {e}", exc_info=True)

# Append the counter content to the file
try:
    counter_step = counter_step + len(dfstepclean)
    with open('new_counters.txt', 'a') as file:
        file.write(str(counter_step) + "\n")
    print(f"'counter_step' appended to {file.name} successfully.")
except Exception as e:
    print(f"Error appending to file: {e}")


# add id_recipe column 
try:
    dfstep_final = dfstepclean.copy()
    main_logger.debug(dfstep_final.info())

    # Function to map recipe titles to id_recipe
    def map_with_none(recipe_title, RECIPE_MAPPING):
        """Maps recipe names to id_recipe, handling missing values."""
        if pd.isna(recipe_title):
            return None  # Return None for missing titles
        return RECIPE_MAPPING.get(recipe_title, None)  # Use get() to avoid KeyError for missing keys

    # Apply the mapping function to create 'id_recipe' column
    dfstep_final['id_recipe'] = dfstep_final['title'].apply(map_with_none, args=(RECIPE_MAPPING,))
    main_logger.info("Added 'id_recipe' column based on 'RECIPE_MAPPING'.")
    # Convert id_recipe to integer type where applicable, keeping None values
    dfstep_final['id_recipe'] = dfstep_final['id_recipe'].astype('Int64')
    main_logger.info("Converted 'id_recipe' column to Integer type.")
    main_logger.debug(dfstep_final.head())
    main_logger.debug(dfstep_final.info())

    # Drop columns by label (column name)
    dfstep_final.drop(columns=['equipment', 'title'], inplace=True)
    main_logger.info("'equipment' and 'title' columns deleted")
    main_logger.debug(dfstep_final.head())
    main_logger.debug(dfstep_final.info())
except Exception as e:
    main_logger.error(f"Failed to finalize dfstep_final DataFrame: {e}", exc_info=True)


#---------------------------------------------------------
# 6. Dealing with instructions
#---------------------------------------------------------
# Extract a counter from which we start the id range

try:
    with open('counters.txt', 'r') as file:
        lines = file.readlines()
        if len(lines) > 3:
            counter_inst = int(lines[3].strip())
        else:
            counter_inst = 1
    main_logger.info(f"Counter instruction: {counter_inst}")
except Exception as e:
    main_logger.error(f"Failed to extract 'counter_inst': {e}", exc_info=True)



try:
    # Assign a unique number to each unique title
    dfstep_final['instruction_id'] = pd.factorize(dfstep_final['id_recipe'])[0] + counter_inst
    main_logger.info("Added 'instrction_id'.")
    main_logger.debug(dfstep_final.head(30))
    main_logger.debug(dfstep_final.info())
except Exception as e:
    main_logger.error(f"Failed to add 'instruction_id': {e}", exc_info=True)


try:
    # Create the instructions dataframe
    dfIns = dfstep_final[['instruction_id', 'id_recipe']]
    main_logger.info("Constructed instructions dataframe")
    main_logger.debug(dfIns.head())
    main_logger.debug(dfIns.info())
except Exception as e:
    main_logger.error(f"Failed to construct instructions dataframe: {e}", exc_info=True)

try:
    # Find duplicates
    duplicates = dfIns[dfIns.duplicated()]
    main_logger.debug(duplicates)

    # Remove duplicates
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate instructions. Removing duplicates.")
        dfIns = dfIns.drop_duplicates()
        dfIns.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfIns)} instructions after removing duplicates.")
    else:
        main_logger.info("No duplicates found")
except Exception as e:
    main_logger.error("An unexpected error occurred while handling duplicates.", exc_info=True)


# Append the counter content to the file
try:
    counter_inst = counter_inst + len(dfIns)
    with open('new_counters.txt', 'a') as file:
        file.write(str(counter_inst) + "\n")
    main_logger.info(f"'counter_inst' appended to {file.name} successfully.")
except Exception as e:
    main_logger.error(f"Error appending to file: {e}")

#Drop columns
try:
    dfstep_final.drop(columns=['id_recipe'], inplace=True)
    main_logger.info("'id_recipe' deleted from steps dataframe")
except Exception as e:
    main_logger.error("Failed to drop 'id_recipe' column.", exc_info=True)

main_logger.debug(dfIns.head())
main_logger.debug(dfIns.info())
main_logger.debug(dfstep_final.info())

#---------------------------------------------------------
# 7. Dealing with equipments
#---------------------------------------------------------
# 7.1 Extract Equipments names
#---------------------------------------------------------
# Define the required columns for the equipments DataFrame
dfequip_COLUMNS = ['name']

# Initialize an empty list to collect rows of data
rows_data = []

# Iterate through each row of the original DataFrame
for i, row_series in dfstep.iterrows():
    try:
        equip_list = row_series['equipment']
        
        if equip_list and isinstance(equip_list, list):
            for each_dict in equip_list:
                # Check if the element is a dictionary
                if each_dict and isinstance(each_dict, dict):
                    name = each_dict.get('name', None)
                    
                    # Create a dictionary for the row data
                    row_data = {
                        'name': name
                    }
                    
                    # Append the row data to the list
                    rows_data.append(row_data)
                else:
                    main_logger.warning(f"Non-dictionary element found in equipment list at row {i}. Skipping element.")
        else:
            main_logger.warning(f"Equipment list is empty or not a list at row {i}. Skipping row.")

    except Exception as e:
        main_logger.error(f"Error processing row {i}", exc_info=True)

# Create the DataFrame from the list of row data
try:
    dfequip = pd.DataFrame(rows_data, columns=dfequip_COLUMNS)
    main_logger.info(f"Constructed equipments DataFrame with {len(dfequip)} equipments.")
    main_logger.debug(dfequip.head())
    main_logger.debug(dfequip.info())
except Exception as e:
    main_logger.error("Failed to construct equipments DataFrame.", exc_info=True)
    dfequip = pd.DataFrame(columns=dfequip_COLUMNS)  # Create an empty DataFrame as a fallback

try:
    # Find duplicates
    duplicates = dfequip[dfequip.duplicated()]
    main_logger.debug(duplicates)

    # Remove duplicates
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate equipments. Removing duplicates.")
        dfequip = dfequip.drop_duplicates()
        dfequip.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfequip)} equipments after removing duplicates.")
    else:
        main_logger.info("No duplicates found")
except Exception as e:
    main_logger.error("An error occurred while finding or removing duplicates.", exc_info=True)

# Rename 'name' column to 'equip_name'
try:
    dfequip = dfequip.rename(columns={'name': 'equip_name'})
    main_logger.info("Renamed column 'name' to 'equip_name'.")
    main_logger.debug(dfequip.head())
    main_logger.debug(dfequip.info())
except KeyError as e:
    main_logger.error(f"Failed to rename 'name' column because it is missing: {e}", exc_info=True)
except Exception as e:
    main_logger.error(f"An unexpected error occurred while renaming 'name' column: {e}", exc_info=True)
#----------------------------------------------------------------
# 7.2 Reading Existing Equipment Mapping
#----------------------------------------------------------------
# Function to read existing ING_MAPPING.txt
def read_equipment_mapping(file_path):
    try:
        with open(file_path, 'r') as file:
            EQUIP_MAPPING = json.load(file)
        return set(EQUIP_MAPPING.keys()), EQUIP_MAPPING
    except FileNotFoundError:
        main_logger.warning(f"{file_path} not found. Starting with an empty set.")
        return set(), {}
    except Exception as e:
        main_logger.error(f"Failed to read EQUIP_MAPPING file: {e}", exc_info=True)
        return set(), {}

# Read existing EQUIP_MAPPING.txt
existing_equipments, existing_equipments_mapping = read_equipment_mapping('EQUIP_MAPPING.txt')
main_logger.info("mapping read")
#----------------------------------------------------------------
# 7.3 Construct the id_equipment column
#----------------------------------------------------------------

# Assign existing equipment IDs and handle new equipments
try:
    # Map existing equipment IDs based on the equip_name column
    dfequip['id_equipment'] = dfequip['equip_name'].map(existing_equipments_mapping)

    # Identify new equipments
    new_equipments = dfequip[dfequip['id_equipment'].isna()]

    # Extract a counter from which we start the id range
    with open('counters.txt', 'r') as file:
        lines = file.readlines()
        if len(lines) > 4:
            counter_equip = int(lines[4].strip())
            main_logger.info(f"Counter equipments: {counter_equip}")
        else:
            counter_equip = 1
            main_logger.info(f"Counter equipments initialized: {counter_equip}")

    # Assign new IDs to new counter_equip
    new_equipments['id_equipment'] = range(counter_equip, counter_equip + len(new_equipments))
    main_logger.info("New IDs assigned to new equipments")

    # Combine existing and new equipments
    dfequip.update(new_equipments)
    main_logger.info("New equipments IDs combined with existing equipments.")

    # Convert id_equipment to integer type 
    dfequip['id_equipment'] = dfequip['id_equipment'].astype('Int64')
    main_logger.info("'id_equipment' column converted to Integer")

except Exception as e:
    main_logger.error(f"Failed to assign IDs or combine equipments: {e}", exc_info=True)

# Append the counter content to the file
try:
    counter_equip += len(new_equipments)
    main_logger.info(f"Counter equipments incremented: {counter_equip}")
    with open('new_counters.txt', 'a') as file:
        file.write(str(counter_equip) + "\n")
    main_logger.info(f"'counter_equip' appended to {file.name} successfully.")
except Exception as e:
    main_logger.error(f"Error appending to file: {e}", exc_info=True)


# Create a mapping dictionary from dfequip
try:
    new_equipments_mapping = dfequip.set_index('equip_name')['id_equipment'].to_dict()
    main_logger.info("Created 'new_equipments_mapping'.")
except Exception as e:
    main_logger.error(f"Failed to create 'new_equipments_mapping': {e}", exc_info=True)

# Update EQUIP_MAPPING with new equipments
try:
    EQUIP_MAPPING = {**existing_equipments_mapping, **new_equipments_mapping}
    with open('EQUIP_MAPPING.txt', 'w') as file:
        json.dump(EQUIP_MAPPING, file)
    main_logger.info("Updated and saved 'EQUIP_MAPPING'.")
except Exception as e:
    main_logger.error(f"Failed to update and save 'EQUIP_MAPPING': {e}", exc_info=True)

#----------------------------------------------------------------
# 8. Dealing with reference_equip
#----------------------------------------------------------------
# 8.1 'equip_name' column
#----------------------------------------------------------------

# Explode the equipment column in dfstepclean dataframe because equipment is a list of equipments 
try:
    dfstepclean_exploded = dfstepclean.explode(['equipment'])
    dfstepclean_exploded = dfstepclean_exploded.reset_index(drop=True)
    main_logger.info("'equipment' column in step dataframe exploded.")
except Exception as e:
    main_logger.error("Failed to explode 'equipment' column.", exc_info=True)

# Rename the exploded column to equip_name
try:
    dfstepclean_exploded = dfstepclean_exploded.rename(columns={'equipment': 'equip_name'})
    main_logger.info("Renamed column 'equipment' to 'equip_name'.")
    main_logger.debug(dfstepclean_exploded.head())
except Exception as e:
    main_logger.error("Failed to rename 'equipment' column", exc_info=True)

#----------------------------------------------------------------
# 8.2 'id_equipment' column
#----------------------------------------------------------------

# Function to map equipment names to id_equipment, handling missing values
def map_with_none(equip_name, EQUIP_MAPPING):
    """Maps equipment names to id_equipment, handling missing values."""
    if pd.isna(equip_name):
        return None  # Return None for missing equipment names
    return EQUIP_MAPPING.get(equip_name, None)  # Use get() to avoid KeyError for missing keys

# Apply the custom map function to fill the new column
try:
    dfstepclean_exploded['id_equipment'] = dfstepclean_exploded['equip_name'].apply(map_with_none, args=(EQUIP_MAPPING,))
    main_logger.info("'id_equipment' column added according to the 'EQUIP_MAPPING'.")
except Exception as e:
    main_logger.error("Failed to add 'id_equipment' column.", exc_info=True)

# Convert id_equipment to integer type where applicable, keeping None values
try:
    dfstepclean_exploded['id_equipment'] = dfstepclean_exploded['id_equipment'].astype('Int64')
    main_logger.info("'id_equipment' column converted to Integer.")
    main_logger.debug(dfstepclean_exploded.head())
    main_logger.debug(dfstepclean_exploded.info())
except Exception as e:
    main_logger.error("Failed to convert 'id_equipment' column to Integer.", exc_info=True)

#----------------------------------------------------------------
# 8.3 'id_recipe' column
#----------------------------------------------------------------

# dealing with id_recipe in dfstepclean_exploded
try:
    # Apply the custom map function to fill the new column
    dfstepclean_exploded['id_recipe'] = dfstepclean_exploded['title'].apply(map_with_none, args=(RECIPE_MAPPING,))
    main_logger.info("'id_recipe' column added according to the 'RECIPE_MAPPING'.")
except Exception as e:
    main_logger.error("Failed to add 'id_recipe' column.", exc_info=True)
    
try:
    # Convert id_recipe to integer type where applicable, keeping None values
    dfstepclean_exploded['id_recipe'] = dfstepclean_exploded['id_recipe'].astype('Int64')
    main_logger.info("'id_recipe' column converted to Integer.")
    main_logger.debug(dfstepclean_exploded.head())
    main_logger.debug(dfstepclean_exploded.info())
except Exception as e:
    main_logger.error("Failed to convert 'id_recipe' column to Integer.", exc_info=True)

#----------------------------------------------------------------
# 8.4 dfreference_equip dataframe
#----------------------------------------------------------------

# creating the dfreference_equip dataframe
try:
    dfreference_equip = dfstepclean_exploded[['id_recipe', 'id_step', 'id_equipment']]
    main_logger.info("Constructed reference_equip dataframe.")
    main_logger.debug(dfreference_equip.head())
    main_logger.debug(dfreference_equip.info())
except Exception as e:
    main_logger.error("Failed to create 'dfreference_equip' dataframe.", exc_info=True)

# Clean up dataframes
try:
    del dfstepclean_exploded
    del dfstepclean
    main_logger.info("Deleted intermediate dataframes.")
except Exception as e:
    main_logger.error("One or more intermediate dataframes were not found during deletion.", exc_info=True)


try:
    # Find duplicates
    duplicates = dfreference_equip[dfreference_equip.duplicated()]
    main_logger.debug(duplicates)

    # Remove duplicates
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate refrence equipment. Removing duplicates.")
        dfreference_equip = dfreference_equip.drop_duplicates()
        dfreference_equip.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfreference_equip)} refrence equipment after removing duplicates.")
    else:
        main_logger.info("No duplicates found")
except Exception as e:
    main_logger.error("An error occurred while finding or removing duplicates.", exc_info=True)

  
#----------------------------------------------------------------
# 9. Dealing with dish types
#----------------------------------------------------------------
# 9.1 Constructing the dfdish dataframe
#----------------------------------------------------------------
# Define the required columns for the dish DataFrame
dfdish_COLUMNS = ['dishTypes', 'title']
# Construct the DataFrame
try:
    dfdish = pd.DataFrame(df, columns=dfdish_COLUMNS)
    main_logger.info(f"Extracted dish types for {len(dfdish)} recipes.")
    main_logger.debug(dfdish.head())
    main_logger.debug(dfdish.info())
except Exception as e:
    main_logger.error("Failed to construct dfdish DataFrame.", exc_info=True)

# Explode the 'dishTypes' column
try:
    dfALLdish_types = dfdish.explode('dishTypes').reset_index(drop=True)
    main_logger.info(f"'dishTypes' column exploded and there are {len(dfALLdish_types)} dish types.")
    main_logger.debug(dfALLdish_types.head())
    main_logger.debug(dfALLdish_types.info())
except Exception as e:
    main_logger.error("Failed to explode 'dishTypes' column.", exc_info=True)

# Copy the exploded DataFrame
dfdish_type=dfALLdish_types.copy()

# Delete 'title' column
try:
    dfdish_type.drop(columns=['title'], inplace=True)
    main_logger.info("Deleted 'title' column.")
except Exception as e:
    main_logger.error("Failed to delete 'title' column.", exc_info=True)

try:
    # Find duplicates
    duplicates = dfdish_type[dfdish_type.duplicated()]
    main_logger.debug(duplicates)
    
    # Remove duplicates
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate dish types. Removing duplicates.")
        dfdish_type = dfdish_type.drop_duplicates()
        dfdish_type.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfdish_type)} dish types after removing duplicates.")
    else:
        main_logger.info("No duplicates found.")
except Exception as e:
    main_logger.error("An error occurred while finding or removing duplicates.", exc_info=True)


try:
    # Find Null values
    none_rows = dfdish_type[dfdish_type['dishTypes'].isna()]
    main_logger.debug(none_rows)

    # Remove null values
    if not none_rows.empty:
        main_logger.warning(f"Found {len(none_rows)} null dish types. Removing null values.")
        dfdish_type.dropna(inplace=True)
        dfdish_type.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfdish_type)} dish types after removing null values.")
    else:
        main_logger.info("No null values found.")
except Exception as e:
    main_logger.error("An error occurred while finding or removing null values.", exc_info=True)

# Rename 'dishTypes' column to 'dish_type'
try:
    dfdish_type = dfdish_type.rename(columns={'dishTypes': 'dish_type'})
    main_logger.info("Renamed column 'dishTypes' to 'dish_type'.")
except Exception as e:
    main_logger.error("Failed to rename 'dishTypes' column.", exc_info=True)

#----------------------------------------------------------------
# 9.2 Reading Existing dish types Mapping
#----------------------------------------------------------------
# Function to read existing DISH_MAPPING.txt
def read_dish_mapping(file_path):
    try:
        with open(file_path, 'r') as file:
            DISH_MAPPING = json.load(file)
        return set(DISH_MAPPING.keys()), DISH_MAPPING
    except FileNotFoundError:
        main_logger.warning(f"{file_path} not found. Starting with an empty set.")
        return set(), {}
    except Exception as e:
        main_logger.error(f"Failed to read DISH_MAPPING file: {e}", exc_info=True)
        return set(), {}

# Read existing DISH_MAPPING.txt
existing_dishes, existing_dish_mapping = read_dish_mapping('DISH_MAPPING.txt')
main_logger.info("mapping read")

#----------------------------------------------------------------
# 9.3 Construct the id_dish_type column
#----------------------------------------------------------------

# Assign existing dish type IDs and handle new dish types
try:
    # Map existing dish type IDs based on the dish_type column
    dfdish_type['id_dish_type'] = dfdish_type['dish_type'].map(existing_dish_mapping)

    # Identify new dish types
    new_dish_types = dfdish_type[dfdish_type['id_dish_type'].isna()]

    # Extract a counter from which we start the id range
    with open('counters.txt', 'r') as file:
        lines = file.readlines()
        if len(lines) > 5:
            counter_dish = int(lines[5].strip())
            main_logger.info(f"Counter dish types: {counter_dish}")
        else:
            counter_dish = 1
            main_logger.info(f"Counter dish types initialized: {counter_dish}")

    # Assign new IDs to new dish types
    new_dish_types['id_dish_type'] = range(counter_dish, counter_dish + len(new_dish_types))
    main_logger.info("New IDs assigned to new dish types")

    # Combine existing and new dish types
    dfdish_type.update(new_dish_types)
    main_logger.info("New dish types IDs combined with existing dish types.")

    # Convert id_dish_type to integer type 
    dfdish_type['id_dish_type'] = dfdish_type['id_dish_type'].astype('Int64')
    main_logger.info("'id_dish_type' column converted to Integer")

except Exception as e:
    main_logger.error(f"Failed to assign IDs or combine dish types: {e}", exc_info=True)

# Append the counter content to the file
try:
    counter_dish += len(dfdish_type)
    main_logger.info(f"Counter dish types incremented: {counter_dish}")
    with open('new_counters.txt', 'a') as file:
        file.write(str(counter_dish) + "\n")
    main_logger.info(f"'counter_dish' appended to {file.name} successfully.")
except Exception as e:
    main_logger.error(f"Error appending to file: {e}", exc_info=True)


# Create a mapping dictionary from dfdish_type
try:
    new_dish_mapping = dfdish_type.set_index('dish_type')['id_dish_type'].to_dict()
    main_logger.info("Created 'new_dish_mapping'.")
except Exception as e:
    main_logger.error(f"Failed to create 'new_dish_mapping': {e}", exc_info=True)

# Update DISH_MAPPING with new dish types
try:
    DISH_MAPPING = {**existing_dish_mapping, **new_dish_mapping}
    with open('DISH_MAPPING.txt', 'w') as file:
        json.dump(DISH_MAPPING, file)
    main_logger.info("Updated and saved 'DISH_MAPPING'.")
except Exception as e:
    main_logger.error(f"Failed to update and save 'DISH_MAPPING': {e}", exc_info=True)



#----------------------------------------------------------------
# 10. dfis_a dataframe
#----------------------------------------------------------------
# 10.1 'id_dish_type' column
#----------------------------------------------------------------
# Define a function to map dish types to id_dish_type, handling missing values
def map_with_none(dish_type, mapping_dict):
    """Maps dish type names to id_dish_type, handling missing values."""
    if pd.isna(dish_type):
        return None  # Return None for missing dish types
    return mapping_dict.get(dish_type, None)  # Use get() to avoid KeyError for missing keys

# Apply the custom map function to 'dishTypes' column
try:
    dfALLdish_types['id_dish_type'] = dfALLdish_types['dishTypes'].apply(map_with_none, args=(DISH_MAPPING,))
    main_logger.info("'id_dish_type' column added according to the 'DISH_MAPPING'.")
except Exception as e:
    main_logger.error("Failed to map 'dishTypes' to 'id_dish_type'.", exc_info=True)

# Convert 'id_dish_type' to integer type where applicable, keeping None values
try:
    dfALLdish_types['id_dish_type'] = dfALLdish_types['id_dish_type'].astype('Int64')
    main_logger.info("'id_dish_type' column converted to Integer.")
except Exception as e:
    main_logger.error("Failed to convert 'id_dish_type' to Integer.", exc_info=True)

#----------------------------------------------------------------
# 10.2 'id_recipe' column
#----------------------------------------------------------------

# Apply the custom map function to 'title' column for 'id_recipe'
try:
    dfALLdish_types['id_recipe'] = dfALLdish_types['title'].apply(map_with_none, args=(RECIPE_MAPPING,))
    main_logger.info("'id_recipe' column added according to the 'RECIPE_MAPPING'.")
except Exception as e:
    main_logger.error("Failed to map 'title' to 'id_recipe'.", exc_info=True)

# Convert 'id_recipe' to integer type where applicable, keeping None values
try:
    dfALLdish_types['id_recipe'] = dfALLdish_types['id_recipe'].astype('Int64')
    main_logger.info("'id_recipe' column converted to Integer.")
except Exception as e:
    main_logger.error("Failed to convert 'id_recipe' to Integer.", exc_info=True)

#----------------------------------------------------------------
# 10.3 Some cleaning
#----------------------------------------------------------------

# Log the DataFrame information
main_logger.debug(dfALLdish_types.head())
main_logger.debug(dfALLdish_types.info())

# Delete 'dishTypes' and 'title' columns
try:
    dfALLdish_types.drop(columns=['dishTypes', 'title'], inplace=True)
    main_logger.info("Deleted columns 'dishTypes' and 'title'.")
except Exception as e:
    main_logger.error("Failed to delete columns 'dishTypes' or 'title'.", exc_info=True)

# Copy the DataFrame and delete the original
try:
    dfis_a = dfALLdish_types.copy()
    del dfALLdish_types
    main_logger.info("Copied dfALLdish_types to dfis_a and deleted dfALLdish_types.")
except Exception as e:
    main_logger.error("Failed to copy or delete dfALLdish_types.", exc_info=True)

# Log the final DataFrame information
main_logger.debug(dfis_a.head())
main_logger.debug(dfis_a.info())

#----------------------------------------------------------------
# 11. Dealing with cuisines
#----------------------------------------------------------------
# 11.1 Constructing the dfcuisine dataframe
#----------------------------------------------------------------
# Define the required columns for the cuisines DataFrame
dfcuisines_COLUMNS = ['cuisines', 'title']

# Construct the DataFrame
try:
    dfALLcuisines = pd.DataFrame(df, columns=dfcuisines_COLUMNS)
    main_logger.info(f"Extracted cuisines for {len(dfALLcuisines)} recipes.")
    main_logger.debug(dfALLcuisines.head())
    main_logger.debug(dfALLcuisines.info())
except Exception as e:
    main_logger.error("Failed to construct dfALLcuisines DataFrame.", exc_info=True)

# Explode the 'cuisines' column
try:
    dfALLcuisines = dfALLcuisines.explode('cuisines').reset_index(drop=True)
    main_logger.info(f"'cuisines' column exploded and contains {len(dfALLcuisines)} cuisines.")
    main_logger.debug(dfALLcuisines.info())
except Exception as e:
    main_logger.error("Failed to explode 'cuisines' column.", exc_info=True)

# Copy the exploded DataFrame
dfcuisine=dfALLcuisines.copy()

# Delete 'title' column
try:
    dfcuisine.drop(columns=['title'], inplace=True)
    main_logger.info("Deleted 'title' column.")
except Exception as e:
    main_logger.error("Failed to delete 'title' column.", exc_info=True)

try:
    # Find duplicates
    duplicates = dfcuisine[dfcuisine.duplicated()]
    main_logger.debug(duplicates)

    # Remove duplicates
    if not duplicates.empty:
        main_logger.warning(f"Found {len(duplicates)} duplicate cuisines. Removing duplicates.")
        dfcuisine = dfcuisine.drop_duplicates()
        dfcuisine.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfcuisine)} cuisines after removing duplicates.")
    else:
        main_logger.info("No duplicates found.")
except Exception as e:
    main_logger.error("An error occurred while finding or removing duplicates.", exc_info=True)

try:
    # Find Null values
    none_rows = dfcuisine[dfcuisine['cuisines'].isna()]
    main_logger.debug(none_rows)

    # Remove null values
    if not none_rows.empty:
        main_logger.warning(f"Found {len(none_rows)} null cuisine values. Removing null values.")
        dfcuisine.dropna(inplace=True)
        dfcuisine.reset_index(drop=True, inplace=True)
        main_logger.info(f"DataFrame now contains {len(dfcuisine)} cuisines after removing null values.")
    else:
        main_logger.info("No null values found.")
except Exception as e:
    main_logger.error("An error occurred while finding or removing null values.", exc_info=True)

# Rename 'cuisines' column to 'recipe_cuisine'
try:
    dfcuisine = dfcuisine.rename(columns={'cuisines': 'recipe_cuisine'})
    main_logger.info("Renamed column 'cuisines' to 'recipe_cuisine'.")
    main_logger.debug(dfcuisine.info())
except Exception as e:
    main_logger.error("Failed to rename 'cuisines' column.", exc_info=True)

#----------------------------------------------------------------
# 11.2 Reading Existing cuisines Mapping
#----------------------------------------------------------------
# Function to read existing CUISINES_MAPPING.txt
def read_cuisines_mapping(file_path):
    try:
        with open(file_path, 'r') as file:
            CUISINES_MAPPING = json.load(file)
        return set(CUISINES_MAPPING.keys()), CUISINES_MAPPING
    except FileNotFoundError:
        main_logger.warning(f"{file_path} not found. Starting with an empty set.")
        return set(), {}
    except Exception as e:
        main_logger.error(f"Failed to read CUISINES_MAPPING file: {e}", exc_info=True)
        return set(), {}

# Read existing CUISINES_MAPPING.txt
existing_cuisines, existing_cuisines_mapping = read_cuisines_mapping('CUISINES_MAPPING.txt')
main_logger.info("mapping read")

#----------------------------------------------------------------
# 11.3 Construct the 'id_cuisine' column
#----------------------------------------------------------------

# Assign existing cuisine IDs and handle new cuisines
try:
    # Map existing cuisine IDs based on the 'id_cuisine' column
    dfcuisine['id_cuisine'] = dfcuisine['recipe_cuisine'].map(existing_cuisines_mapping)

    # Identify new cuisines
    new_cuisines = dfcuisine[dfcuisine['id_cuisine'].isna()]

    # Extract a counter from which we start the id range
    with open('counters.txt', 'r') as file:
        lines = file.readlines()
        if len(lines) > 6:
            counter_cuisines = int(lines[6].strip())
            main_logger.info(f"Counter cuisines: {counter_cuisines}")
        else:
            counter_cuisines = 1
            main_logger.info(f"Counter cuisines initialized: {counter_cuisines}")

    # Assign new IDs to new cuisines
    new_cuisines['id_cuisine'] = range(counter_cuisines, counter_cuisines + len(new_cuisines))
    main_logger.info("New IDs assigned to new cuisines")

    # Combine existing and new cuisines
    dfcuisine.update(new_cuisines)
    main_logger.info("New cuisine IDs combined with existing cuisines.")

    # Convert id_cuisine to integer type 
    dfcuisine['id_cuisine'] = dfcuisine['id_cuisine'].astype('Int64')
    main_logger.info("'id_cuisine' column converted to Integer")

except Exception as e:
    main_logger.error(f"Failed to assign IDs or combine cuisines: {e}", exc_info=True)

# Append the counter content to the file
try:
    counter_cuisines += len(new_cuisines)
    main_logger.info(f"Counter cuisines incremented: {counter_cuisines}")
    with open('new_counters.txt', 'a') as file:
        file.write(str(counter_cuisines) + "\n")
    main_logger.info(f"'counter_cuisines' appended to {file.name} successfully.")
except Exception as e:
    main_logger.error(f"Error appending to file: {e}", exc_info=True)

# Create a mapping dictionary from dfcuisine
try:
    new_cuisines_mapping = dfcuisine.set_index('recipe_cuisine')['id_cuisine'].to_dict()
    main_logger.info("Created 'new_cuisines_mapping'.")
except Exception as e:
    main_logger.error(f"Failed to create 'new_cuisines_mapping': {e}", exc_info=True)

# Update CUISINES_MAPPING with new cuisines
try:
    CUISINES_MAPPING = {**existing_cuisines_mapping, **new_cuisines_mapping}
    with open('CUISINES_MAPPING.txt', 'w') as file:
        json.dump(CUISINES_MAPPING, file)
    main_logger.info("Updated and saved 'CUISINES_MAPPING'.")
except Exception as e:
    main_logger.error(f"Failed to update and save 'CUISINES_MAPPING': {e}", exc_info=True)

#----------------------------------------------------------------
# 12. dfbelongs dataframe
#----------------------------------------------------------------
# 12.1 'id_cuisine' column
#----------------------------------------------------------------

# Define a function to map recipe cuisines to id_cuisine, handling missing values
def map_with_none(recipe_cuisine, CUISINES_MAPPING):
    """Maps recipes cuisines to id_cuisine, handling missing values."""
    if pd.isna(recipe_cuisine):
        return None  # Return None for missing cuisines
    return CUISINES_MAPPING.get(recipe_cuisine, None)  # Use get() to avoid KeyError for missing keys

# Apply the custom map function to add 'id_cuisine' column
try:
    dfALLcuisines['id_cuisine'] = dfALLcuisines['cuisines'].apply(map_with_none, args=(CUISINES_MAPPING,))
    main_logger.info("'id_cuisine' column added according to the 'CUISINES_MAPPING'.")
except Exception as e:
    main_logger.error("Failed to map 'id_cuisine' to 'cuisines'.", exc_info=True)

# Convert id_cuisine to integer type where applicable, keeping None values
try:
    dfALLcuisines['id_cuisine'] = dfALLcuisines['id_cuisine'].astype('Int64')
    main_logger.info("'id_cuisine' column converted to Integer ")
except Exception as e:
    main_logger.error("Failed to convert 'id_cuisine' to Integer.", exc_info=True)

#----------------------------------------------------------------
# 11.2 'id_recipe' column
#----------------------------------------------------------------

# Apply the custom map function to add 'id_recipe' column
try:
    dfALLcuisines['id_recipe'] = dfALLcuisines['title'].apply(map_with_none, args=(RECIPE_MAPPING,))
    main_logger.info("'id_recipe' column added according to the 'RECIPE_MAPPING'.")
except Exception as e:
    main_logger.error("Failed to map 'id_recipe' to 'title'.", exc_info=True)

# Convert id_recipe to integer type where applicable, keeping None values
try:
    dfALLcuisines['id_recipe'] = dfALLcuisines['id_recipe'].astype('Int64')
    main_logger.info("'id_recipe' column converted to Integer.")
except Exception as e:
    main_logger.error("Failed to convert 'id_recipe' to Integer.", exc_info=True)

#----------------------------------------------------------------
# 10.3 Some cleaning
#----------------------------------------------------------------

# Log the DataFrame head and info
main_logger.debug(dfALLcuisines.head())
main_logger.debug(dfALLcuisines.info())

# Drop 'cuisines' and 'title' columns from dfcuisines
try:
    dfALLcuisines.drop(columns=['cuisines', 'title'], inplace=True)
    main_logger.info("Deleted 'cuisines' and 'title' columns.")
except Exception as e:
    main_logger.error("Failed to delete 'cuisines' or 'title' columns.", exc_info=True)

# Copy dfcuisines to dfbelongs and delete dfcuisines
try:
    dfbelongs = dfALLcuisines.copy()
    main_logger.info("Copied dfALLcuisines to dfbelongs.")
    
    del dfALLcuisines
    main_logger.info("Deleted dfALLcuisines.")

    main_logger.debug(dfbelongs.head())
    main_logger.debug(dfbelongs.info())
except Exception as e:
    main_logger.error("Failed to copy or delete dfcuisines.", exc_info=True)
#----------------------------------------------------------------

#----------------------------------------------------------------
####################### Load #######################
#----------------------------------------------------------------

# Database configuration
DB_HOST='10.0.2.15' # Database host IP address
DB_PORT= 5432 # Database port number
DB_NAME='recipe_etl' # Name of the database
DB_USER='maryem' # Database user name
DB_PASSWORD='HelloWorld' # Database user password

try:
    # Establish connection to the database
    conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    main_logger.info(f"Connected to {DB_NAME}")
except psycopg2.Error as e:
    main_logger.error(f"Error: Could not make connection to the database {DB_NAME}")
    main_logger.error(e)
    conn = None

#----------------------------------------------------------------

# Function to check if a record already exists in the database
def record_exists(query, values):
    """
    Check if a record already exists in the database.

    Parameters:
    query (str): The SQL query to execute.
    values (tuple): The values to use in the SQL query.

    Returns:
    bool: True if the record exists, False otherwise.
    """
    try:
        # Execute the query with the provided values
        cur.execute(query, values)
        # Check if any record was fetched (exists)
        return cur.fetchone() is not None
    except psycopg2.Error as e:
        # Log an error if any occurs during the check
        main_logger.error(f"Error in checking for existing record: {e}")
        return False

# Function to execute an insert query and check if the insertion was successful
def execute_insert_and_check(query, values, table_name, check_query, check_values):
    """
    Execute an insert query and check if the insertion was successful.

    Parameters:
    query (str): The SQL insert query to execute.
    values (tuple): The values to use in the SQL insert query.
    table_name (str): The name of the table to insert into.
    check_query (str): The SQL query to check if the record already exists.
    check_values (tuple): The values to use in the check query.

    Raises:
    ValueError: If the table is empty after insertion attempts.
    """
    try:
        # Check if the record already exists
        if not record_exists(check_query, check_values):
            # Execute the insert query with the provided values
            cur.execute(query, values)
            # Commit the transaction
            conn.commit()
            # Check the number of rows in the table after the insert
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cur.fetchone()[0]
            # Raise an error if the table is empty after insertion
            if row_count == 0:
                main_logger.error(f"The {table_name} table is empty after insertion attempts.")
                raise ValueError(f"The {table_name} table is empty after insertion attempts.")
            else:
                # Log success if insertion was successful
                main_logger.info(f"Values inserted into {table_name} successfully")
        else:
            # Log a warning if a duplicate entry is found
            main_logger.warning(f"Duplicate entry found in {table_name}. Skipping insertion.")
    except psycopg2.Error as e:
        # Log an error if any occurs during the insertion
        main_logger.error(f"Error in inserting values into {table_name}: {e}", exc_info=True)
        # Revert the database to the state it was in before the current transaction began.
        conn.rollback()
        raise
    except ValueError as ve:
        # Log any value errors
        main_logger.error(ve)
        raise

#----------------------------------------------------------------

# Proceed only if the connection was successful
if conn:
    try:
        # Obtain a cursor to execute queries
        cur = conn.cursor()
        # Log that the cursor was obtained successfully
        main_logger.info(f"Cursor obtained for the database {DB_NAME}")
    except psycopg2.Error as e:
        # Log an error if the cursor could not be obtained
        main_logger.error(f"Error: Could not get cursor for the database {DB_NAME}")
        main_logger.error(e)
        cur = None
    
    # If cursor is obtained successfully
    if cur:
        # Insert into Recipe table
        try:
            id_recipe_set=set()
            # Iterate over each row in the dataframe dfrecipes
            for index, row in dfrecipes.iterrows():
                # Check if the recipe already exists by id_recipe or recipe_title
                check_query = """
                SELECT 1 FROM Recipe WHERE recipe_title = %s
                """
                check_values = (row['recipe_title'],)

                # Execute the insert and check function for the Recipe table
                execute_insert_and_check(
                    """
                    INSERT INTO Recipe (id_recipe, recipe_title, ready_min, summary, servings, is_cheap, price_per_serving, 
                    is_vegetarian, is_vegan, is_glutenFree, is_dairyFree, is_healthy, is_sustainable, is_lowFodmap, 
                    is_Popular, license, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                    (
                        int(row['id_recipe']),
                        row['recipe_title'],
                        int(row['ready_min']),
                        row['summary'],
                        int(row['servings']),
                        bool(row['is_cheap']),
                        float(row['price_per_serving']),
                        bool(row['is_vegetarian']),
                        bool(row['is_vegan']),
                        bool(row['is_glutenFree']),
                        bool(row['is_dairyFree']),
                        bool(row['is_healthy']),
                        bool(row['is_sustainable']),
                        bool(row['is_lowFodmap']),
                        bool(row['is_Popular']),
                        row['license'],
                        row['source_url'],
                    ),
                    'Recipe',
                    check_query,
                    check_values
                )
    
                # Check if the recipe was inserted
                cur.execute(check_query, check_values)
                if not cur.fetchone():
                    id_recipe_set.add(int(row['id_recipe'])) # Save the recipe ID for related table insertions
                    continue  # Skip to the next recipe if this one already exists
           
            # Insert into Instruction table
            try:
                id_ins_set=set()
                for index, row in dfIns.iterrows():
                    if int(row['id_recipe']) not in id_recipe_set:
                        execute_insert_and_check(
                            """
                            INSERT INTO Instruction (id_instruction, id_recipe)
                            VALUES (%s, %s);""",
                            (
                                int(row['instruction_id']),
                                int(row['id_recipe'])
                            ),
                            'Instruction',
                            "SELECT 1 FROM Instruction WHERE id_instruction = %s AND id_recipe = %s",
                            (int(row['instruction_id']), int(row['id_recipe']))
                        )
                    # Check if the instruction was inserted
                    cur.execute(check_query, check_values)
                    if not cur.fetchone():
                        id_ins_set.add(int(row['instruction_id'])) # Save the instruction ID for related table insertions
                        continue  # Skip to the next instruction if this one already exists
            except Exception as e:
                main_logger.error(f"Failed to insert into Instruction table: {e}", exc_info=True)
            
            # Insert into Ingredient table
            try:
                id_ing_set = set()
                for index, row in dfIng.iterrows():
                    # Check if the ingredient already exists by ing_name
                    check_query = """
                    SELECT 1 FROM Ingredient WHERE ing_name = %s
                    """
                    check_values = (row['ing_name'],)

                    execute_insert_and_check(
                        """
                        INSERT INTO Ingredient (id_ingredient, ing_name, consistency, aisle)
                        VALUES (%s, %s, %s, %s);""",
                        (
                            int(row['id_ingredient']),
                            row['ing_name'],
                            row['consistency'],
                            row['aisle']
                        ),
                        'Ingredient',
                        check_query,
                        check_values
                    )

                    # Check if the ingredient was inserted
                    cur.execute(check_query, check_values)
                    if cur.fetchone():
                        continue  # Skip to the next ingredient if this one already exists
                    cur.execute("""SELECT id_ingredient FROM Ingredient WHERE ing_name = %s""", (row['ing_name'],))
                    # Fetch the result
                    result = cur.fetchone()
                    # Extract the id_ingredient from the result
                    old_id = result[0]
                    # Append to the set
                    id_ing_set.add({int(row['id_ingredient']): old_id})

                # Initialize an empty dictionary to hold the final combined dictionary
                ing_id_MAP = {}
                # Iterate over the set and update the final dictionary with each dictionary
                for d in id_ing_set:
                    ing_id_MAP.update(d)
            except Exception as e:
                main_logger.error(f"Failed to insert into Ingredient table: {e}", exc_info=True)

            # Insert into Step table
            try:
                id_step_set=set()
                for index, row in dfstep_final.iterrows():
                    if int(row['instruction_id']) not in id_ins_set:
                        execute_insert_and_check(
                            """
                            INSERT INTO Step (id_step, step, number, length, id_instruction)
                            VALUES (%s, %s, %s, %s, %s);""",
                            (
                                int(row['id_step']),
                                row['step'],
                                int(row['number']),
                                row['length'],
                                int(row['instruction_id'])
                            ),
                            'Step',
                            "SELECT 1 FROM Step WHERE id_step = %s",
                            (int(row['id_step']),)
                    )
                    # Check if the step was inserted
                    cur.execute(check_query, check_values)
                    if not cur.fetchone():
                        id_recipe_set.add(int(row['id_step'])) # Save the step ID for related table insertions
                        continue  # Skip to the next step if this one already exists
            except Exception as e:
                main_logger.error(f"Failed to insert into Step table: {e}", exc_info=True)

            # Insert into Equipment table
            try:
                for index, row in dfequip.iterrows():
                    execute_insert_and_check(
                        """
                        INSERT INTO Equipment (id_equipment, equip_name)
                        VALUES (%s, %s);""",
                        (
                            int(row['id_equipment']),
                            row['equip_name']
                        ),
                        'Equipment',
                        "SELECT 1 FROM Equipment WHERE id_equipment = %s",
                        (int(row['id_equipment']),)
                    )
            except Exception as e:
                main_logger.error(f"Failed to insert into Equipment table: {e}", exc_info=True)

            # Insert into Dish table
            try:
                for index, row in dfdish_type.iterrows():
                    execute_insert_and_check(
                        """
                        INSERT INTO Dish (id_dish_type, dish_type)
                        VALUES (%s, %s);""",
                        (
                            int(row['id_dish_type']),
                            row['dish_type']
                        ),
                        'Dish',
                        "SELECT 1 FROM Dish WHERE id_dish_type = %s",
                        (int(row['id_dish_type']),)
                    )
            except Exception as e:
                main_logger.error(f"Failed to insert into Dish table: {e}", exc_info=True)

            # Insert into Cuisine table
            try:
                for index, row in dfcuisine.iterrows():
                    execute_insert_and_check(
                        """
                        INSERT INTO Cuisine (id_cuisine, recipe_cuisine)
                        VALUES (%s, %s);""",
                        (
                            int(row['id_cuisine']),
                            row['recipe_cuisine']
                        ),
                        'Cuisine',
                        "SELECT 1 FROM Cuisine WHERE id_cuisine = %s",
                        (int(row['id_cuisine']),)
                    )
            except Exception as e:
                main_logger.error(f"Failed to insert into Cuisine table: {e}", exc_info=True)

            # Insert into reference_ing table
            try:
                dfreference_ing = dfreference_ing.replace({pd.NA: np.nan})
                for index, row in dfreference_ing.iterrows():
                    if int(row['id_recipe']) not in id_recipe_set:
                        ing_id= int(row['id_ingredient'])
                        if ing_id in ing_id_MAP.keys():
                            ing_id=ing_id_MAP.get(ing_id)
                            main_logger.warning("altered ingredient id because it exists")
                        execute_insert_and_check(
                            """
                            INSERT INTO reference_ing (id_recipe, id_ingredient, measure)
                            VALUES (%s, %s, %s);""",
                            (
                                int(row['id_recipe']),
                                ing_id,
                                row['measure']
                            ),
                            'reference_ing',
                            "SELECT 1 FROM reference_ing WHERE id_recipe = %s AND id_ingredient = %s",
                            (int(row['id_recipe']), ing_id)
                        )
            except Exception as e:
                main_logger.error(f"Failed to insert into reference_ing table: {e}", exc_info=True)

            # Insert into reference_equip table
            try:
                dfreference_equip = dfreference_equip.replace({pd.NA: np.nan})
                for index, row in dfreference_equip.iterrows():
                    if int(row['id_recipe']) not in id_recipe_set:
                        execute_insert_and_check(
                            """
                            INSERT INTO reference_equip (id_recipe, id_step, id_equipment)
                            VALUES (%s, %s, %s);""",
                            (
                                int(row['id_recipe']),
                                int(row['id_step']) if pd.notna(row['id_step']) else None,
                                int(row['id_equipment']) if pd.notna(row['id_equipment']) else None
                            ),
                            'reference_equip',
                            "SELECT 1 FROM reference_equip WHERE id_recipe = %s AND id_step = %s AND id_equipment = %s",
                            (
                                int(row['id_recipe']),
                                int(row['id_step']) if pd.notna(row['id_step']) else None,
                                int(row['id_equipment']) if pd.notna(row['id_equipment']) else None
                            )
                            )
            except Exception as e:
                main_logger.error(f"Failed to insert into reference_equip table: {e}", exc_info=True)

            # Insert into belongs table
            try:
                dfbelongs = dfbelongs.replace({pd.NA: np.nan})
                for index, row in dfbelongs.iterrows():
                    if int(row['id_recipe']) not in id_recipe_set:
                        execute_insert_and_check(
                            """
                            INSERT INTO belongs (id_recipe, id_cuisine)
                            VALUES (%s, %s);""",
                            (
                                int(row['id_recipe']),
                                int(row['id_cuisine']) if pd.notna(row['id_cuisine']) else None
                            ),
                            'belongs',
                            "SELECT 1 FROM belongs WHERE id_recipe = %s AND id_cuisine = %s",
                            (
                                int(row['id_recipe']),
                                int(row['id_cuisine']) if pd.notna(row['id_cuisine']) else None
                            )
                            )
            except Exception as e:
                logger.error(f"Failed to insert into belongs table: {e}", exc_info=True)

            # Insert into is_a table
            try:
                # Replace pd.NA with np.nan
                dfis_a = dfis_a.replace({pd.NA: np.nan})
                for index, row in dfis_a.iterrows():
                    if int(row['id_recipe']) not in id_recipe_set:
                        execute_insert_and_check(
                            """
                            INSERT INTO is_a (id_recipe, id_dish_type)
                            VALUES (%s, %s);""",
                            (
                                int(row['id_recipe']),
                                int(row['id_dish_type']) if pd.notna(row['id_dish_type']) else None
                            ),
                            'is_a',
                            "SELECT 1 FROM is_a WHERE id_recipe = %s AND id_dish_type = %s",
                            (
                                int(row['id_recipe']),
                                int(row['id_dish_type']) if pd.notna(row['id_dish_type']) else None
                            )
                            )
            except Exception as e:
                main_logger.error(f"Failed to insert into is_a table: {e}", exc_info=True)

        finally:
            cur.close()
    else:
        main_logger.error("Cursor could not be obtained. Exiting the program.")
    conn.close()
else:
    main_logger.error("Connection to the database could not be established. Exiting the program.")





