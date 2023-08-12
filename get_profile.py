import csv
import sys 
from civicpy import civic
import collections

import requests
import json

def fetch_molecular_profile(mpId):
    url = 'https://civicdb.org/api/graphql'

    query = """
    query MolecularProfileSummary($mpId: Int!) {
      molecularProfile(id: $mpId) {
        ...MolecularProfileSummaryFields
      }
    }

    fragment MolecularProfileSummaryFields on MolecularProfile {
      parsedName {
        ...MolecularProfileParsedName
      }
    }

    fragment MolecularProfileParsedName on MolecularProfileSegment {
      __typename
      ... on MolecularProfileTextSegment {
        text
      }
      ... on Gene {
        id
        name
        link
      }
      ... on Variant {
        id
        name
        link
        deprecated
      }
    }
    """

    variables = {"mpId": mpId}

    response = requests.post(url, json={'query': query, 'variables': variables})
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

reader = csv.reader(sys.stdin)

writer = csv.writer(sys.stdout)

counter = collections.defaultdict(int)
results = []

for row in reader:
    chrom, start, ref, var, molecular_profile_id  = row
    print(civic.get_molecular_profile_by_id(molecular_profile_id))
    
    result = fetch_molecular_profile(int(molecular_profile_id))
    result["molecular_profile_id"] = molecular_profile_id
    results.append(result)
    with open(f"variants/{molecular_profile_id}.json", "w") as fp:
        print(json.dumps(result, indent=4), file=fp)
