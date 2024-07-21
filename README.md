# Version 3 

## Overview

I noticed that while extracting the second batch of data, the loading function presents several errors due to its inability to handle redundant data across batches. In this version, I tried to ensure that each record is inserted only once at its initial occurrence and skipped in subsequent batches, as well as properly manage the foreign key relationships in other tables.

## Challenges

1. There are some inconsistencies regarding ingredient names that I was unable to resolve. I tried using some Python libraries to address the issue. For example, I transformed 'tomato' and 'tomatoes' to 'tomatoes,' but there are exceptions like 'ice' becoming 'rice.'
2. The loading function still presents several errors when trying to update it to append non-duplicate data to the database.
  
**If you have any suggestions, please feel free to share them with me to improve my project.**
