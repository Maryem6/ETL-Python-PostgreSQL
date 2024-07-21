# ETL-Python-PostgreSQL

## Overview 

This project involves extracting data from the Spoonacular API, performing data modeling, preprocessing the data with Python, and loading it into PostgreSQL. The goal is to enable users to find recipe details based on the ingredients they have, the recipe name, or their dietary preferences.

## Data modeling

### Conceptual Model
#### The desired:

I wanted that both steps and recipes could reference ingredients

![image](https://github.com/Maryem6/ETL-Python-PostgreSQL/assets/96294018/2a7c0cca-97db-4761-a90a-efb4ab6b4e32)

#### The actual

Due to unresolved naming inconsistencies, I deleted the relationship between steps and ingredients, as it is not essential.

![image](https://github.com/Maryem6/ETL-Python-PostgreSQL/assets/96294018/3ee8d293-e16d-46cc-bf05-469e15f2c0ee)

### Logical Model

Recipe = (<ins>id_recipe INT</ins>, recipe_title VARCHAR(50), ready_min INT, summary VARCHAR(2000), servings INT, is_cheap LOGICAL, price_per_serving DOUBLE, is_vegetarian LOGICAL, is_vegan LOGICAL, is_glutenFree LOGICAL, is_dairyFree LOGICAL, is_healthy LOGICAL, is_sustainable LOGICAL, is_lowFodmap LOGICAL, is_Popular LOGICAL, license VARCHAR(20), source_url VARCHAR(100));

Ingredient = (<ins>id_ingredient</ins> INT, ing_name VARCHAR(50), consistency VARCHAR(20), aisle VARCHAR(20));

reference_ing = (<ins>#id_recipe</ins>, <ins>#id_ingredient</ins>, measure VARCHAR(50));

Instruction = (<ins>id_instruction</ins> INT, <ins>#id_recipe</ins>);

Step = (<ins>id_step</ins> INT, step VARCHAR(8000), number INT, length VARCHAR(50), <ins>#id_instruction</ins>);

Equipment = (<ins>id_equipment</ins> INT, equip_name VARCHAR(50));

reference_equip = (<ins>#id_recipe</ins>, <ins>#id_step</ins>, <ins>#id_equipment</ins>);

dish = (<ins>id_dish_type</ins> INT, dish_type VARCHAR(50));

is_a = (<ins>#id_recipe</ins>, <ins>#id_dish_type</ins>);

Cuisine = (<ins>id_cuisine</ins> INT, recipe_cuisine VARCHAR(50));

belongs = (<ins>#id_recipe</ins>, <ins>#id_cuisine</ins>);


## Versions:
 This project has 4 versions:
 1. **Version 1:** The code where I developed the structure of the project.
 2. **Version 2:** The code with improvements for single execution and incorporated logging.
 3. **Version 3:** The code where I addressed redundant data issues in the loading function.
 4. **Version 4:** The code where I handled redundant data in the extract and transform functions and included shell scripting.
    - In this version, the pipeline is scheduled to run on the first day of each month at 7 AM using cron.
   
## Challenges

1. There are some inconsistencies regarding ingredient names that I was unable to resolve. I tried using some Python libraries to address the issue. For example, I transformed 'tomato' and 'tomatoes' to 'tomatoes,' but there are exceptions like 'ice' becoming 'rice.'
  
**If you have any suggestions, please feel free to share them with me to improve my project.**
