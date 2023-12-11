from dagster import asset, DailyPartitionsDefinition, FreshnessPolicy
import requests
import bz2
from collections import defaultdict
import pyarrow as pa
from datetime import datetime

@asset(partitions_def=DailyPartitionsDefinition(start_date="2016-01-01"), io_manager_key="vanilla_parquet_io_manager",
freshness_policy=FreshnessPolicy(cron_schedule="0 0 * * *", maximum_lag_minutes=60 * 24))
def wikipedia_daily_pageviews(context):
    date_str = context.partition_key
    year, month, day = date_str.split('-')

    url = f"https://dumps.wikimedia.org/other/pageview_complete/{year}/{year}-{month}/pageviews-{year}{month}{day}-user.bz2"
    views_data = defaultdict(lambda: defaultdict(int))
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with bz2.open(r.raw, mode='rt') as bz2_file:
            for line in bz2_file:
                split = line.split()
                if len(split) != 6:
                    continue
                project, entity, page_id, channel, views, _ = split
                if project == 'en.wikipedia' and page_id != 'null':
                    views_data[entity][page_id] += int(views)

    # Prepare data for Arrow Table
    ids = []
    entities = []
    views = []
    for entity, pages in views_data.items():
        for page_id, view_count in pages.items():
            ids.append(page_id)
            entities.append(entity)
            views.append(view_count)

    dates = [datetime.strptime(date_str, '%Y-%m-%d') for _ in range(len(ids))]

    # Create Arrow Arrays
    id_array = pa.array(ids, pa.string())
    entity_array = pa.array(entities, pa.string())
    views_array = pa.array(views, pa.int32())
    date_array = pa.array(dates, pa.date32())

    # Create Arrow Table
    table = pa.Table.from_arrays([id_array, entity_array, views_array, date_array], names=['id', 'entity', 'views', 'date'])

    return table