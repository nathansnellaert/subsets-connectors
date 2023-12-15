from dagster import asset
import pandas as pd

@asset
def subsets_datasets() -> pd.DataFrame:
    from integrations import defs
    assets = defs.get_asset_graph().assets
    datasets = []
    for asset in assets:
        metadata_by_key = asset.metadata_by_key
        for k, metadata in metadata_by_key.items():
            id = k[0][0]
            datasets.append({
                'id': id,
                'name': metadata.get('name', None),
                'description': metadata.get('description', None),
            })

    return pd.DataFrame(datasets)

@asset
def subsets_dataset_columns() -> pd.DataFrame:
    from integrations import defs
    assets = defs.get_asset_graph().assets
    columns = []
    for asset in assets:
        metadata_by_key = asset.metadata_by_key
        for k, metadata in metadata_by_key.items():
            id = k[0][0]
            for column in metadata.get('columns', []):
                columns.append({
                    'dataset_id': id,
                    'name': column.get('name', None),
                    'description': column.get('description', None),
                })
         
    return pd.DataFrame(columns)