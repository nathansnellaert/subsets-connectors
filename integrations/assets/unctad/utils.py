import requests
import py7zr
import io
import pandas as pd

def download_dataset(dataset, dtypes=None):

    # new version uses '.' in dataset name rather than '.', patch in download utility for now
    dataset = dataset.replace('_', '.')    

    # first, get the file id for the dataset
    file_id_resp = requests.get(f'https://unctadstat-api.unctad.org/api/reportMetadata/{dataset}/bulkfiles/en')
    file_id = file_id_resp.json()[0]['fileId']

    # load the dataset using the file id
    dataset = requests.get(f"https://unctadstat-api.unctad.org/api/reportMetadata/US.MerchVolumeQuarterly/bulkfile/{file_id}/en")
    archive_data = io.BytesIO(dataset.content)

    csv_files = {}
    with py7zr.SevenZipFile(archive_data, mode='r') as archive:
        all_files = archive.getnames()
        for name in all_files:
            if name.endswith('.csv'):
                csv_files[name] = None  

        if len(csv_files.keys()) != 1:
            raise Exception("Expected only one DataFrame. Exiting.")
        
        extracted_data = archive.read(targets=csv_files.keys())

    for name, content in extracted_data.items():
        csv_content_str = content.read().decode('utf-8') 
        csv_content = io.StringIO(csv_content_str)
        if dtypes is not None:
            df = pd.read_csv(csv_content, dtype=dtypes)
        else:
            df = pd.read_csv(csv_content)

    # all columns to lowercase, and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df