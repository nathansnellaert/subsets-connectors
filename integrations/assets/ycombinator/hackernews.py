from dagster import asset, StaticPartitionsDefinition
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def id_range_partitions():
    start_id = 1
    max_id = 39 * 1000 * 1000
    partition_size = 10000 

    return [f"{i}-{min(i + partition_size - 1, max_id)}" for i in range(start_id, max_id, partition_size)]

id_partitions = StaticPartitionsDefinition(partition_keys=id_range_partitions())

def fetch_item(item_id, retry_count=3, delay=5):
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json?print=pretty"
    
    for attempt in range(retry_count):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429: 
            time.sleep(delay) 
        else:
            break

    return None

@asset(partitions_def=id_partitions)
def hacker_news_items(context):
    start_id, end_id = [int(x) for x in context.partition_key.split('-')]
    items = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_id = {executor.submit(fetch_item, item_id): item_id for item_id in range(start_id, end_id + 1)}

        for future in as_completed(future_to_id):
            item_id = future_to_id[future]
            item = future.result()
            if item:
                items.append(item)

    return pd.DataFrame(items)
