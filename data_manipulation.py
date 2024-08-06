import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()


def read_lead_emails(file_path):
    df = pd.read_csv(file_path)
    emails = df["lead_email"].to_list()
    return emails


def query_leads(emails):
    query = text('SELECT l.lead_id, l.email AS "lead_email", pa.client_id, pc.name AS "client_name", pc.email AS "client_email", pc.fc_contact_owner_id '
                 'FROM "public.lead" l '
                 'JOIN public."public.assessment" pa ON l.lead_id = pa.lead_id '
                 'JOIN public."public.client" pc ON pa.client_id = pc.client_id '
                 'WHERE l.email IN :emails')

    engine = create_engine(os.getenv("DATABASE_URL"))

    with engine.connect() as conn:
        result = conn.execute(query, {'emails': tuple(emails)})

        results = result.fetchall()
        return results


def save_leads(leads):
    df_result = pd.DataFrame(leads)

    df_result['fc_contact_owner_id'] = df_result['fc_contact_owner_id'].fillna(
        0)
    df_result['fc_contact_owner_id'] = df_result['fc_contact_owner_id'].astype(
        int)
    df_result = df_result.sort_values(by='lead_id')
    df_result.to_csv("files/query_leads.csv", index=False)
    return df_result


emails = read_lead_emails("files/unique_leads.csv")
results = query_leads(emails)
save_leads(results)


def check_duplicates(file_path, header_names, check_column):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path, header=None, names=header_names)

    # Check for duplicates in the 'email' column
    duplicates = df[df.duplicated(subset=check_column, keep=False)]

    # Print the duplicate entries
    print(duplicates)
    # Save the duplicate entries to a new CSV file
    duplicates.to_csv("files/duplicated_leads.csv", index=False)


check_duplicates("files/leads.csv", [
    'lead_id', 'lead_email', 'client_id', 'client_name', 'client_email', 'fc_contact_owner_id'], 'lead_email')

# print("___________________________________________")
# print("Unique Leads:")
# check_duplicates("files/unique_leads.csv", [
#     'lead_id', 'lead_name', 'lead_email'], 'lead_email')
