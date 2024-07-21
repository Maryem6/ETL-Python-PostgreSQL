#!/bin/bash

# File to store the variables
OFFSET_FILE="execution_offset.txt"
TITLES_FILE="execution_tiles.txt"
RECIPES_FILE="recipes_to_transform.txt"
#This allows allows the offset value and titles_set to persist between different executions of the script

# Check if the offset file exists, if not, create it and initialize with 0
if [ ! -f $OFFSET_FILE ]; then
    echo 0 > $OFFSET_FILE
fi

# Check if the titles file exists, if not, create it
if [ ! -f $TITLES_FILE ]; then
    touch $TITLES_FILE
fi

# Check if the RECIPES file exists, if not, create it
if [ ! -f $RECIPES_FILE ]; then
    touch $RECIPES_FILE
fi

# Read the current value of the variable from the file
offset=$(tail -n 1 $OFFSET_FILE)

# Read the titles set from the file
titles_set=$(cat $TITLES_FILE)

# Execute the Python script with the required variables as arguments
python3 Extract.py $offset "$titles_set"

# Save the incremented offset back to the file, appending to the end
# Save the incremented offset back to the file, appending to the end
if [ -f new_offset.txt ]; then
    cat new_offset.txt >> $OFFSET_FILE
    rm new_offset.txt
else
    echo "Error: new_offset.txt was not created. Check Extract.py for issues."
fi
# The ">>" symbol appends new line of data to end of file. 


# Save the updated titles set back to the file, ensuring each title is on a new line
if [ -f new_titles_set.txt ]; then
    sort -u new_titles_set.txt > $TITLES_FILE
    rm new_titles_set.txt
else
    echo "Error: new_titles_set.txt was not created. Check Extract.py for issues."
fi

# Save the new_recipes to transform
if [ -f new_recipes.txt ]; then
    cat new_recipes.txt > $RECIPES_FILE
    rm new_recipes.txt
else
    echo "Error: new_recipes.txt was not created. Check Extract.py for issues."
fi


#!/bin/bash

# File to store the variables
COUNTERS_FILE="counters.txt"


# Check if the counters file exists, if not, create it
if [ ! -f $COUNTERS_FILE ]; then
    touch $COUNTERS_FILE
fi

python3 "Transform&Load.py"

if [ -f new_counters.txt ]; then
    cat new_counters.txt > $COUNTERS_FILE
    rm new_counters.txt
fi

