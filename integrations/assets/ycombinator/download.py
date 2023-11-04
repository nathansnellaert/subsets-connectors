import requests
import json

def generate_batch_identifiers(start_year, end_year):
    batches = []
    for year in range(start_year, end_year + 1):
        batch_number = f"{'0' if year < 2010 else ''}{year - 2000}"

        batches.append({
            "batch_name": f'S{batch_number}',
            "batch_year": year,
            "batch_type": "summer"
        })
        batches.append({
            "batch_name": f'W{batch_number}',
            "batch_year": year,
            "batch_type": "winter"
        })
    
    # K12 was an edtech accelerator, that joined YC in 2016: https://www.ycombinator.com/blog/ik12-plus-yc/
    # Companies from this accelerator have 'IK12' as their batch identifier, but encompass any startups prior to the merge
    # Therefore, we cannot determine the batch year
    batches.append({
        "batch_name": 'IK12',
        "batch_year": None,
        "batch_type": "special"
    })

    return batches

def retrieve_companies_for_batch(batch, algolia_config):
    body = {
        "requests": [{
            "indexName": algolia_config["index_name"],
            "params": f"filters=batch:{batch}&hitsPerPage=1000"
        }]
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded"
    }

    url = f'{algolia_config["url"]}?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser%3B%20JS%20Helper%20(3.11.3)&x-algolia-application-id={algolia_config["app_id"]}&x-algolia-api-key={algolia_config["api_key"]}'
    
    response = requests.post(url, headers=headers, data=json.dumps(body))

    if response.status_code == 200:
        data = response.json()
        return data['results'][0]['hits']
    else:
        print(f'Error: {response.status_code}')
        return []

def download_ycombinator_companies(start_year, end_year, algolia_config):
    all_companies = []
    batches = generate_batch_identifiers(start_year, end_year)
    for batch in batches:
        companies = retrieve_companies_for_batch(batch['batch_name'], algolia_config)
        print(f'Retrieved {len(companies)} companies for batch {batch["batch_name"]}')
        # Assign batch year to each company
        for company in companies:
            company['batch_year'] = batch['batch_year']
            company['batch_type'] = batch['batch_type']

        all_companies.extend(companies)
        
    return all_companies
