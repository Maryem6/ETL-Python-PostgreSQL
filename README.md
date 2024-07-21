# Version 4 

## Overview

In the previous version, I determined that the better approach is to extract unique data from the beginning and handle redundant data related to foreign keys during the transformation phase. Any remaining redundant data is then addressed in the loading function. In this version, I implemented the new solution, and it worked as intended.

## Challenges

1. There are some inconsistencies regarding ingredient names that I was unable to resolve. I tried using some Python libraries to address the issue. For example, I transformed 'tomato' and 'tomatoes' to 'tomatoes,' but there are exceptions like 'ice' becoming 'rice.'
  
**If you have any suggestions, please feel free to share them with me to improve my project.**
