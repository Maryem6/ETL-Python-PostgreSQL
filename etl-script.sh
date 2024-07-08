#!/usr/bash

# File to store the variable
VAR_FILE="counter.txt"
#This allows allows the counter value to persist between different executions of the script


# Check if the variable file exists, if not, create it and initialize with 0
if [ ! -f $VAR_FILE ]; then
    echo 1 > $VAR_FILE
fi

# Read the current value of the variable from the file
counter=$(cat $VAR_FILE)

# Increment the variable
counter=$((counter + 100))

# Save the incremented variable back to the file
echo $counter > $VAR_FILE

# Execute the Python script with the incremented variable as an argument
python3 ETLpythonproject.py $counter
