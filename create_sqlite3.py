import json
import sqlite3
import sys

# Read JSON data from stdin
input_data = sys.stdin.read()
data_list = json.loads(input_data)

# Connect to the SQLite database (this will create a new file called 'variants.db')
conn = sqlite3.connect('variants.db')
cursor = conn.cursor()

# Create a new table if it does not exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS variants (
    chrom TEXT,
    start INTEGER,
    ref TEXT,
    alt TEXT,
    mp_id INTEGER,
    variant_ids TEXT,
    molecular_profile_score REAL,
    num_acc_eids INTEGER,
    num_sub_eids INTEGER
)
''')

# Iterate through the list and insert each JSON object into the table
for data in data_list:
    variant_ids_str = json.dumps(data['variant_ids'])
    cursor.execute('''
    INSERT INTO variants (chrom, start, ref, alt, mp_id, variant_ids, molecular_profile_score, num_acc_eids, num_sub_eids)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (data['chrom'], data['start'], data['ref'], data['alt'], data['mp_id'], variant_ids_str, data['molecular_profile_score'], data['num_acc_eids'], data['num_sub_eids']))

# Commit the transaction and close the connection
conn.commit()
conn.close()
