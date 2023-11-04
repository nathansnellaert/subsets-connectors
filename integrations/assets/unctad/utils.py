import requests
import py7zr
import io
import pandas as pd

def download_dataset(path, dtypes=None):
    base_url = "https://unctadstat.unctad.org/7zip"
    url = f"{base_url}/{path}.csv.7z"
    response = requests.get(url)
    archive_data = io.BytesIO(response.content)

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