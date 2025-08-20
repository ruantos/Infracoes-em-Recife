from load import Loader
from transform import Transformer
from fetch import fetch_dataframe
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    
    load_dotenv()
    supa_url = os.environ.get('SUPA_URL')
    supa_key = os.environ.get('API_KEY')

    id_list = {
        "2023": "c269789d-da47-4dde-8ce7-42fba10fe8e2",
        "2024": "4adf9430-35a5-4e88-8ecf-b45748b81c7d",
        "2025": "48bd8822-df18-48d0-bbc1-2de87ca0d70b",
        }


    cleaner = Transformer()
    supabase = Loader(supa_url, supa_key)
    supabase.connect()
    
    for year, id in id_list.items():
        try:
            print(f"Fetching {year} file...")
            df = fetch_dataframe(id)

            if not df.empty:
                df = cleaner.transform(df)
                records = df.to_dict('records')

                supabase.insert(records)
                print(f"{year} records loaded successfully!")

            else:
                print(f"{year} file is empty or couldn't be found")

        except Exception as e:
            print(f"Error caught : {e}")