# Version 3 

## Overview

In this version, I aimed to schedule my pipeline to run on a monthly basis, so the load function in the Python code needed to be updated to append data to the database. This presented a challenge!

I noticed that while extracting the second batch of data, the loading function encountered several errors due to its inability to handle redundant data across batches. To address this, I ensured that each record is inserted only once at its initial occurrence and skipped in subsequent batches. Additionally, I properly managed the foreign key relationships in other tables.

## Challenges

1. There are some inconsistencies regarding ingredient names that I was unable to resolve. I tried using some Python libraries to address the issue. For example, I transformed 'tomato' and 'tomatoes' to 'tomatoes,' but there are exceptions like 'ice' becoming 'rice.'
2. The loading function still presents several errors when trying to update it to append non-duplicate data to the database.
  
**If you have any suggestions, please feel free to share them with me to improve my project.**
