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

# Example usage
mpId = 4432
result = fetch_molecular_profile(mpId)
print(json.dumps(result))
