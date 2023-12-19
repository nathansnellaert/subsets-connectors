import requests
import io
import py7zr
import pyarrow.csv as pac

def download_dataset(dataset):
    # replace the first underscore with a dot
    dataset = dataset.replace('_', '.', 1)

    # first, get the file id for the dataset
    file_id_resp = requests.get(f'https://unctadstat-api.unctad.org/api/reportMetadata/{dataset}/bulkfiles/en')
    file_id = file_id_resp.json()[0]['fileId']

    # load the dataset using the file id
    dataset_resp = requests.get(f"https://unctadstat-api.unctad.org/api/reportMetadata/{dataset}/bulkfile/{file_id}/en")
    archive_data = io.BytesIO(dataset_resp.content)

    with py7zr.SevenZipFile(archive_data, mode='r') as archive:
        all_files = archive.getnames()
        csv_files = [name for name in all_files if name.endswith('.csv')]

        if len(csv_files) != 1:
            raise Exception("Expected only one DataFrame. Exiting.")
        
        extracted_data = archive.read(targets=csv_files)

    for name, content in extracted_data.items():
        csv_content = io.BytesIO(content.read())
        table = pac.read_csv(csv_content)

        # Process column names: lowercase and replace spaces with underscores
        new_column_names = [col_name.lower().replace(' ', '_') for col_name in table.column_names]
        table = table.rename_columns(new_column_names)

    return table