import streamlit as st
import pandas as pd
import requests
import json
from io import StringIO
import gspread
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

def save_data_to_google_sheets(data):
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(credentials)
    sheet_id = st.secrets["sheet_id"]
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet("Sheet1")

    worksheet = client.open_by_key(sheet_id).worksheet('Sheet1')  # Replace 'Sheet1' with the actual sheet name
    if worksheet:
        worksheet.clear()
    else:
        st.write("Worksheet not found.")
    st.write("hello")
    # Clear the existing content
    #worksheet.clear()
    
    # Update with new data
    worksheet.update([data.columns.values.tolist()] + data.values.tolist())
    st.write(f"Data saved to Google Sheets with ID {sheet_id}")

def fetch_technographics(api_key, domains, limit):
    main_df = pd.DataFrame()
    for domain in domains:
        url = f"https://api.similarweb.com/v4/website/{domain}/technographics/all?api_key={api_key}&format=json&limit={limit}"
        
        headers = {"x-sw-source":"streamlit_kw"}
        response = requests.get(url, headers = headers)
        if response.status_code == 200:
            json_response = response.json()
            
            if json_response.get("technologies"):
                new_df = pd.json_normalize(json_response["technologies"])
                new_df["domain"] = domain
                main_df = pd.concat([main_df, new_df])
            else:
                st.warning(f"No related apps found for {domain}")
        else:
            st.error(f"Error fetching data for {domain}: {response.status_code}")
    return main_df[["domain", "technology_name","category","sub_category","free_paid","description"]] if not main_df.empty else None

def fetch_lead_enrichment(api_key, start_date, end_date, country, domains):
    lead_enrichment_df = pd.DataFrame()
    for domain in domains:
        url = f"https://api.similarweb.com/v1/website/{domain}/lead-enrichment/all?api_key={api_key}&start_date={start_date}&end_date={end_date}&country={country}&main_domain_only=false&format=json&show_verified=false"
        
        headers = {"x-sw-source":"streamlit_kw"}
        response = requests.get(url, headers = headers)
        if response.status_code == 200:
            json_response = response.json()
            #data = json_response
            if json_response:
                pages_per_visit_df = pd.DataFrame(json_response['pages_per_visit']).rename(columns={'value': 'pages_per_visit'})
                visits_df = pd.DataFrame(json_response['visits']).rename(columns={'value': 'visits'})
                mom_growth_df = pd.DataFrame(json_response['mom_growth']).rename(columns={'value': 'mom_growth'})
                unique_visitors_df = pd.DataFrame(json_response['unique_visitors']).rename(columns={'value': 'unique_visitors'})
                bounce_rate_df = pd.DataFrame(json_response['bounce_rate']).rename(columns={'value': 'bounce_rate'})
                average_visit_duration_df = pd.DataFrame(json_response['average_visit_duration']).rename(columns={'value': 'average_visit_duration'})

                # Mobile vs desktop data extraction
                mobile_desktop_df = pd.DataFrame(json_response['mobile_desktop_share'])
                mobile_desktop_df = pd.json_normalize(mobile_desktop_df['value']).assign(date=mobile_desktop_df['date'])

                # Merge all dataframes into one big dataframe
                new_df = pages_per_visit_df.merge(visits_df, on='date') \
                    .merge(mom_growth_df, on='date') \
                    .merge(unique_visitors_df, on='date') \
                    .merge(bounce_rate_df, on='date') \
                    .merge(average_visit_duration_df, on='date') \
                    .merge(mobile_desktop_df, on='date')
                
                new_df["date"] = new_df["date"].str[:7]
                new_df["domain"] = domain
                new_df["country"] = country
                new_df["global_rank"] = json_response["global_rank"]
                new_df["site_type"] = json_response["site_type"]
                new_df["site_type_new"] = json_response["site_type_new"]
                new_df["company_name"] = json_response["company_name"]
                new_df["employee_range"] = json_response["employee_range"]
                new_df["estimated_revenue_in_usd"] = json_response["estimated_revenue_in_usd"]
                new_df["zip_code"] = json_response["zip_code"]
                new_df["headquarters"] = json_response["headquarters"]
                new_df["website_category"] = json_response["website_category"]
                new_df["website_category_new"] = json_response["website_category_new"]
                new_df["category_rank"] = json_response["category_rank"]

                new_df = new_df[["domain", "date", "country", "global_rank", "site_type", "site_type_new", "company_name", "employee_range", "estimated_revenue_in_usd", "zip_code", "headquarters", "website_category", "website_category_new", "category_rank", "pages_per_visit", "visits", "mom_growth", "unique_visitors", "bounce_rate", "average_visit_duration", "desktop_share", "mobile_share"]]
                lead_enrichment_df = pd.concat([lead_enrichment_df, new_df])
                save_data_to_google_sheets(lead_enrichment_df)
            else:
                st.warning(f"No lead_enrichment for {domain}")
        else:
            st.error(f"Error fetching data for {domain}: {response.status_code}")
    return lead_enrichment_df if not lead_enrichment_df.empty else None

def main():
    st.title("SimilarWeb Data Fetcher")

    endpoint = st.radio("Select Endpoint", ["Technographics", "Lead Enrichment"])

    api_key = st.text_input("API Key", type="password")
    input_type = st.radio("Input Type", options=["Site", "List", "File"])

    domains = []
    if input_type == "Site":
        domains = [st.text_input("Domain", value = "google.com")]
    elif input_type == "List":
        domains = st.text_area("Domains (one per line)").split('\n')
    elif input_type == "File":
        uploaded_file = st.file_uploader("Choose a file with domains")
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file, header=None)
            domains = df[0].tolist()

    if endpoint == "Technographics":
        limit = st.number_input("Row Limit", min_value=1, value=1000)
    else:  # Lead Enrichment
        start_date = st.text_input("Start Date (YYYY-MM)", value = "2024-08")
        end_date = st.text_input("End Date (YYYY-MM)", value = "2024-08")
        country = st.text_input("Country", value = "us")

    if st.button("Fetch Data"):
        if api_key and domains:
            domains = [domain.strip() for domain in domains if domain.strip()]
            
            if endpoint == "Technographics":
                result_df = fetch_technographics(api_key, domains, limit)
                file_name = "technographics.csv"
            else:  # Lead Enrichment
                result_df = fetch_lead_enrichment(api_key, start_date, end_date, country, domains)
                file_name = "lead_enrichment.csv"
            
            if result_df is not None:
                st.write("Results:")
                st.dataframe(result_df)

                csv = result_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=file_name,
                    mime="text/csv",
                )
            else:
                st.warning("No data found for the given domains.")
        else:
            st.error("Please provide an API key and at least one domain.")

if __name__ == "__main__":
    main()
