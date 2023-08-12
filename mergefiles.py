import os
import json

# Initialize an empty dictionary to store the results
result = {}

# Iterate through every file in the "variants" directory
for filename in os.listdir('variants'):
    if filename.endswith('.json'):
        filepath = os.path.join('variants', filename)
        # Open each JSON file and load its contents
        with open(filepath, 'r') as file:
            data = json.load(file)
            # Assuming each JSON file contains a dictionary, you can update the result dictionary with its contents
            result[data["molecular_profile_id"]] = data

# Result now contains the combined contents of all the JSON files in the "variants" directory
print(json.dumps(result, indent=4))
