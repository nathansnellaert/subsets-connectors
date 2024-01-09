from dagster import asset
import pandas as pd
import ast
import os

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
                'columns': metadata.get('columns', None),
                'source': metadata.get('source', None),
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


def find_asset_functions_in_file(file_path):
    with open(file_path, 'r') as file:
        node = ast.parse(file.read())
        for function in node.body:
            if isinstance(function, ast.FunctionDef):
                for decorator in function.decorator_list:
                    if (isinstance(decorator, ast.Name) and decorator.id == 'asset') or \
                       (isinstance(decorator, ast.Call) and 
                        isinstance(decorator.func, ast.Name) and 
                        decorator.func.id == 'asset'):
                        yield function.name, function.lineno


def scan_assets_directory(relative_path='./integrations/assets'):
    asset_functions = []
    assets_directory = os.path.abspath(relative_path)
    for root, dirs, files in os.walk(assets_directory):
        for file in files:
            if file.endswith('.py'):
                full_path = os.path.join(root, file)
                relative_file_path = os.path.relpath(full_path, assets_directory)
                for function_name, line_no in find_asset_functions_in_file(full_path):
                    asset_functions.append((relative_file_path, function_name, line_no))
    return asset_functions

@asset
def subsets_asset_paths() -> pd.DataFrame:
    asset_paths = scan_assets_directory()
    return pd.DataFrame(asset_paths, columns=['path', 'function', 'line_no'])