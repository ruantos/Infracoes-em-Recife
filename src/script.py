from load import Loader
from transform import Transformer
from extract import fetch_dataframe_api, fetch_dataframe
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    
    load_dotenv()
    supa_url = os.environ.get('SUPA_URL')
    supa_key = os.environ.get('API_KEY')

    cleaner = Transformer()
    supabase = Loader(supa_url, supa_key)
    supabase.connect()


    for record in supabase.fetch_ids():
        year = record['year']
        identifier = record['identifier']
        
        try:

            print(f"Fetching {year} file...")
            if year < 2021:
                df = fetch_dataframe_api(identifier)
            else:
                df = fetch_dataframe(identifier)

            if not df.empty:
                df = cleaner.transform(df)
                records = df.to_dict('records')

                supabase.insert(records)
                print(f"{year} records loaded successfully!\n")

            else:
                print(f"{year} file is empty or couldn't be found")

        except Exception as e:
            print(f"Error caught : {e}")