import csv
import json
import os

# Change the current working directory to the desktop
os.chdir('/Users/kila/Desktop/Database/datasets/csv/mock1m')

def csv_to_json(csv_file, json_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    with open(json_file, 'w') as file:
        json.dump(rows, file, indent=4)

# Loop through all CSV files in the folder
for csv_file in os.listdir():
    if csv_file.endswith('.csv'):
        json_file = os.path.splitext(csv_file)[0] + '.json'
        print('Converting ' + csv_file + ' to ' + json_file + '...')
        csv_to_json(csv_file, json_file)
        print('Done')

print('All CSV files in the folder have been converted to JSON format.')
print('JSON files created in ' + os.getcwd() + '/json')