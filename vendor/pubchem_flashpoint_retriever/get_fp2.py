# get_fp2.py
#
# Python script to fetch flash point data from PubChem for CAS numbers provided via stdin.
# Uses PubChem PUG REST API to get CID from CAS, then extract flash points from compound data.
#
# Requirements:
# - Python 3.x
# - requests library (install via: pip install requests)
# - json library (built-in)
#
# Usage:
# echo "50-00-0\n64-17-5" | python get_fp2.py
# (Reads CAS numbers from stdin, one per line; outputs JSON to stdout)
#
# Output:
# JSON object: {"CAS1": ["value1", "value2"], "CAS2": []}
#
# Notes:
# - Batch processing via stdin for efficiency.
# - Extracts from 'Flash Point' subsection; returns all available strings (may include units/conditions).
# - Handles API errors gracefully (returns empty list for failures).
# - No rate limiting implemented; add sleeps for large batches to comply with PubChem terms.
#
# Author: glsalierno
# Date: September 2025

import sys
import json
import requests

def get_compound_id_from_cas(cas_number):
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{cas_number}/cids/JSON"
    response = requests.get(url)
    
    if response.status_code != 200:
        return None
    
    data = json.loads(response.text)
    if 'IdentifierList' in data and 'CID' in data['IdentifierList']:
        return str(data['IdentifierList']['CID'][0])
    return None

def get_flash_points(compound_id):
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{compound_id}/JSON"
    response = requests.get(url)
    
    if response.status_code != 200:
        return []
    
    data = json.loads(response.text)
    flash_points = []
    
    if 'Record' in data and 'Section' in data['Record']:
        for section in data['Record']['Section']:
            if section.get('TOCHeading') == 'Chemical and Physical Properties':
                if 'Section' in section:
                    for subsection in section['Section']:
                        if subsection.get('TOCHeading') == 'Experimental Properties':
                            if 'Section' in subsection:
                                for property_section in subsection['Section']:
                                    if property_section.get('TOCHeading') == 'Flash Point':
                                        if 'Information' in property_section:
                                            for info in property_section['Information']:
                                                value = info.get('Value', {}).get('StringWithMarkup', [{}])[0].get('String')
                                                if value:
                                                    flash_points.append(value)
    return flash_points

def process_cas_numbers(cas_numbers):
    results = {}
    for cas in cas_numbers:
        compound_id = get_compound_id_from_cas(cas)
        if compound_id:
            flash_points = get_flash_points(compound_id)
            results[cas] = flash_points
        else:
            results[cas] = []
    return results

if __name__ == "__main__":
    # Read CAS numbers from stdin (one per line)
    cas_numbers = [line.strip() for line in sys.stdin if line.strip()]
    results = process_cas_numbers(cas_numbers)
    # Print results as JSON to stdout
    print(json.dumps(results))
