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


# emails = read_lead_emails("files/unique_leads.csv")
# results = query_leads(emails)
# save_leads(results)


def remove_duplicates(file_path, check_column):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Identify and remove duplicates
    duplicates = df[df.duplicated(subset=check_column, keep=False)]
    print("Number of duplicated lead_email entries removed:")
    print(duplicates.count())

    df_cleaned = df.drop_duplicates(subset=check_column, keep=False)
    print("Number of unique lead_email entries:")
    print(df_cleaned.count())

    # Save the cleaned and duplicate entries to new CSV files
    df_cleaned.to_csv("files/query_leads_cleaned.csv", index=False)
    duplicates.to_csv(
        "files/query_leads_duplicate_lead_email.csv", index=False)
    print("✅ Files saved successfully!")


# remove_duplicates("files/query_leads.csv", 'lead_email')


def remove_no_owner(file_path):
    df = pd.read_csv(file_path)

    leads_without_contact_owner = df[df['fc_contact_owner_id'] == 0]
    print("Number of leads without owner:")
    print(leads_without_contact_owner.count())

    cleaned_leads = df[df['fc_contact_owner_id'] != 0]
    print("Number of leads with unique email and owner:")
    print(cleaned_leads.count())

    cleaned_leads.to_csv("files/query_leads_cleaned.csv", index=False)
    leads_without_contact_owner.to_csv(
        "files/query_leads_without_owner.csv", index=False)
    print("✅ Files saved successfully!")


# remove_no_owner("files/query_leads_cleaned.csv")
