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

UPDATE_PARAMS = {
    "module": "cm",
    "subModule": "contact",
    "xmlString": "",
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
    if 'status' not in df.columns:
        df['status'] = ''

    df.to_csv(file_path, index=False)
    print("✅ Column added successfully!")


def clear_reference_id_and_status(file_path):
    df = pd.read_csv(file_path)
    df['reference_id'] = 0
    df['status'] = ''
    df.to_csv(file_path, index=False)
    print("✅ Column cleared successfully!")


def fetch_reference_id(lead_email, contact_owner_id):
    try:
        PARAMS["filterXML"] = f"<fcRequest><filter><emailIds>{lead_email}</emailIds><contactOwnerID>{contact_owner_id}</contactOwnerID></filter></fcRequest>"
        response = requests.post(
            url=f"{FRANCONNECT_URL}/rest/dataservices/retrieve", params=PARAMS, headers=HEADERS)
        response.raise_for_status()
        json_response = response.json().get("fcResponse")

        if json_response.get("responseStatus") != "Error":
            response_data = json_response.get("responseData")
            if response_data == "No data found.":
                return (None, "No data found")
            if isinstance(response_data.get("cmContact"), list):
                return (None, "Multiple results")
            return (response_data.get(
                "cmContact").get("referenceId"), "Success")
        else:
            raise Exception(f"{json_response.get('error')}")

    except requests.RequestException as e:
        print(
            f"Error fetching reference_id for ({lead_email}, {contact_owner_id}) : {e}")
        return (None, "Request Error")
    except Exception as e:
        print(
            f"Error fetching reference_id for ({lead_email}, {contact_owner_id}) : {e}")
        return (None, "Error")


def fill_reference_id_column(file_path, limit):
    try:
        df = pd.read_csv(file_path)

        # Ensure the 'status' column is of type object (string)
        df['status'] = df['status'].astype(object)

        start_index = df[(df['reference_id'] == 0) & (
            pd.isna(df["status"]))].index.min()

        if start_index is not None:
            print(f"🚩 start_index: {start_index}")
            for index in range(start_index, len(df)):
                # print(f"🔄 index: {index}, status: {df.at[index, 'status']}")

                if df.at[index, 'reference_id'] == 0 and pd.isna(df.at[index, 'status']):
                    print(f"🔄 index: {index}")
                    print(f"==> lead_email: {df.at[index, 'lead_email']}")
                    print(
                        f"==> contact_owner_id: {df.at[index, 'fc_contact_owner_id']}")

                    lead_email = df.at[index, 'lead_email']
                    contact_owner_id = df.at[index, 'fc_contact_owner_id']
                    reference_id, status = fetch_reference_id(
                        lead_email, contact_owner_id)
                    if reference_id is not None:
                        df.at[index, 'reference_id'] = np.int64(reference_id)
                        df.at[index, 'status'] = status
                        print("👍 Success!")
                    else:
                        df.at[index, 'status'] = status
                        print("❌ Failed! Status updated")

        print("✅ Column filled successfully!")
    except Exception as e:
        print(f"Error filling reference_id column: {e}")
    finally:
        df.to_csv(file_path, index=False)
        print("✅ File updated successfully!")


def filter_leads_by_status(file_path, status):
    df = pd.read_csv(file_path)
    filtered_df = df[df['status'] == status]
    filtered_df.to_csv(f"files/{status}_query_leads.csv", index=False)
    print(f"✅ {status} leads filtered successfully!")


def update_qualifyi_lead_nurturing(reference_id):
    try:
        UPDATE_PARAMS["xmlString"] = f"<fcRequest><cmContact><referenceId>{reference_id}</referenceId><_qualifyiLeadNurture8991633>Yes</_qualifyiLeadNurture8991633></cmContact></fcRequest>"
        response = requests.post(
            url=f"{FRANCONNECT_URL}/rest/dataservices/update", params=UPDATE_PARAMS, headers=HEADERS)
        response.raise_for_status()
        json_response = response.json().get("fcResponse")
        if json_response.get("responseStatus") != "Error":
            response_data = json_response.get("responseData")

            return response_data.get("cmContact").get("status")
        else:
            raise Exception(
                f"{json_response.get('responseData').get('error')}")
    except requests.RequestException as e:
        print(f"Error updating lead nurturing for {reference_id}: {e}")
        return "Request Error"
    except Exception as e:
        print(f"Error updating lead nurturing for {reference_id}: {e}")
        return "Error"


def update_franconnect_leads(file_path, batch_size):
    try:
        df = pd.read_csv(file_path)

        start_index = df[df['status'] == 'pending'].index.min()

        batch_size = batch_size if batch_size < len(df) else len(df)

        if start_index is not None:
            print(f"🚩 start_index: {start_index}")
            for index in range(start_index, start_index + batch_size):
                # print(f"🔄 index: {index}, status: {df.at[index, 'status']}")

                print(f"🔄 index: {index}")
                print(f"==> email: {df.at[index, 'email']}")
                print(f"==> reference_id: {df.at[index, 'reference_id']}")

                reference_id = df.at[index, 'reference_id']
                result = update_qualifyi_lead_nurturing(reference_id)

                if result != "Error" and result != "Request Error":
                    print("👍 Success!")
                else:
                    print("❌ Failed! Status updated")

                df.at[index, 'status'] = result
    except Exception as e:
        print(f"Error updating leads: {e}")
    finally:
        df.to_csv(file_path, index=False)
        print("✅ File updated successfully!")


def main():
    # Your code here
    # add_reference_id_column("files/query_leads_cleaned.csv")
    # clear_reference_id_and_status("files/query_leads_cleaned.csv")
    # fill_reference_id_column("files/query_leads_cleaned.csv", 1000)
    # filter_leads_by_status("files/query_leads_cleaned.csv", "Success")
    update_franconnect_leads("files/QC-54/lead_table.csv", batch_size=10000)
    print("🚀 Done!")


if __name__ == "__main__":
    main()
