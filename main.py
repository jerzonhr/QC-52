import pandas as pd
import requests
import os
import sqlalchemy
from dotenv import load_dotenv
import numpy as np

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
FRANCONNECT_URL = os.getenv("FRANCONNECT_BASE_URL")
HEADERS = {
    "Authorization ": f"Bearer  {ACCESS_TOKEN}",
    "Content-Type": "application/x-www-form-urlencoded"
}
PARAMS = {
    "module": "cm",
    "subModule": "contact",
    "filterXML": "",
    "responseType": "json"
}


def add_reference_id_column(file_path):
    df = pd.read_csv(file_path)
    # Add a new column called 'reference_id' with default value
    if 'reference_id' not in df.columns:
        df['reference_id'] = 0
        cols = df.columns.tolist()
        cols.insert(1, cols.pop(cols.index('reference_id')))
        df = df.reindex(columns=cols)

    df.to_csv(file_path, index=False)
    print("✅ Column added successfully!")


def fetch_reference_id(lead_email, contact_owner_id):
    try:
        PARAMS["filterXML"] = f"<fcRequest><filter><emailIds>{lead_email}</emailIds><contactOwnerID>{contact_owner_id}</contactOwnerID></filter></fcRequest>"
        response = requests.post(
            url=f"{FRANCONNECT_URL}/rest/dataservices/retrieve", params=PARAMS, headers=HEADERS)
        response.raise_for_status()
        json_response = response.json().get("fcResponse")

        if json_response.get("responseStatus") != "Error":
            return json_response.get("responseData").get(
                "cmContact").get("referenceId")
        else:
            raise Exception(f"{json_response.get('error')}")

    except requests.RequestException as e:
        print(
            f"Error fetching reference_id for ({lead_email}, {contact_owner_id}) : {e}")
        return None
    except Exception as e:
        print(
            f"Error fetching reference_id for ({lead_email}, {contact_owner_id}) : {e}")
        return None


def fill_reference_id_column(file_path):
    try:
        df = pd.read_csv(file_path)

        start_index = df[df['reference_id'] == 0].index.min()

        if start_index is not None:
            print(f"🚩 start_index: {start_index}")
            for index in range(start_index, 100):
                if df.at[index, 'reference_id'] == 0:
                    print(f"🔄 index: {index}")
                    print(f"==> lead_email: {df.at[index, 'lead_email']}")
                    print(
                        f"==> contact_owner_id: {df.at[index, 'fc_contact_owner_id']}")

                    lead_email = df.at[index, 'lead_email']
                    contact_owner_id = df.at[index, 'fc_contact_owner_id']
                    reference_id = fetch_reference_id(
                        lead_email, contact_owner_id)
                    if reference_id is not None:
                        df.at[index, 'reference_id'] = np.int64(reference_id)
                        print("👍 Success!")

        print("✅ Column filled successfully!")
    except Exception as e:
        print(f"Error filling reference_id column: {e}")
    finally:
        df.to_csv(file_path, index=False)
        print("✅ File updated successfully!")


def main():
    # Your code here
    # add_reference_id_column("files/query_leads_cleaned.csv")
    fill_reference_id_column("files/query_leads_cleaned.csv")


if __name__ == "__main__":
    main()
