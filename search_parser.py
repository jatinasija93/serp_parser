import requests
import csv
from urllib.parse import urlparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st


# Function to extract hostname from a URL
def extract_hostname(url):
    try:
        return urlparse(url).hostname
    except Exception:
        return None


# Function to parse JSON for organic results
def parse_organic_results(json_data):
    # print(json_data)
    organic_results = json_data.get("results", {}).get("results", {}).get("organic", [])
    print(organic_results)
    top_10 = organic_results[:10]  # Limit to top 10 results
    links = []

    for result in top_10:
        link = result.get("link", "")
        hostname = extract_hostname(link)
        if hostname:
            links.append(hostname)

    # Count hostname occurrences
    hostname_count = pd.Series(links).value_counts().to_dict()
    return hostname_count


# Function to write results to CSV
def append_to_csv(filename, data, search_term):
    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        for hostname, count in data.items():
            writer.writerow([search_term, hostname, count])


# Function to call the API for a single search term
def call_api(api_url, api_key, payload, search_term):
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {api_key}",
        "content-type": "application/json",
    }
    payload["data"]["q"] = search_term  # Set the search term dynamically

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            return search_term, parse_organic_results(json_data)
        else:
            return search_term, None
    except Exception as e:
        return search_term, None


# Function to process search terms in parallel
def process_in_batches(
    api_url, api_key, payload, search_terms, output_csv, batch_size=50
):
    results = []

    # Use ThreadPoolExecutor for concurrent calls
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        futures = {
            executor.submit(call_api, api_url, api_key, payload, term): term
            for term in search_terms
        }

        for future in as_completed(futures):
            term = futures[future]
            try:
                search_term, hostname_count = future.result()
                if hostname_count:
                    append_to_csv(output_csv, hostname_count, search_term)
                    results.append((search_term, hostname_count))
                else:
                    st.error(f"Failed to process term: {search_term}")
            except Exception as e:
                st.error(f"Error processing term: {term} - {str(e)}")

    return results


# Streamlit UI
def main():
    st.title("SERP Parser Tool with Batch Processing")

    # User inputs for API details
    api_url = st.text_input("API Endpoint", "https://api.serphouse.com/serp/live")
    api_key = st.text_input("API Key", type="password")

    # Form to modify request parameters
    with st.form("request_parameters"):
        st.subheader("Request Parameters")
        domain = st.text_input("Domain", "google.com")
        loc = st.text_input("Location", "Delhi,India")
        lang = st.text_input("Language", "en")
        device = st.selectbox("Device", ["desktop", "mobile", "tablet"], index=0)
        serp_type = st.selectbox(
            "SERP Type", ["web", "news", "images", "videos"], index=0
        )
        page = st.text_input("Page Number", "1")
        num_results = st.text_input("Number Of Results", "10")
        verbatim = st.text_input("Verbatim", "0")
        batch_size = st.slider("Batch Size", 10, 100, 50)
        submit_params = st.form_submit_button("Set Parameters")

    # Prepare the request payload
    payload = {
        "data": {
            "domain": domain,
            "loc": loc,
            "lang": lang,
            "device": device,
            "serp_type": serp_type,
            "page": page,
            "verbatim": verbatim,
        }
    }

    # File upload and processing
    input_csv = st.file_uploader("Upload CSV with Search Terms", type=["csv"])
    output_csv = st.text_input("Output CSV Filename", "output.csv")

    if st.button("Start Processing"):
        if input_csv and api_url and api_key:
            with st.spinner("Processing..."):
                terms_df = pd.read_csv(input_csv)
                search_terms = terms_df["search_terms"].tolist()
                process_in_batches(
                    api_url, api_key, payload, search_terms, output_csv, batch_size
                )
                st.success("Processing complete!")
                st.download_button(
                    label="Download Output CSV",
                    data=open(output_csv, "rb"),
                    file_name=output_csv,
                    mime="text/csv",
                )
        else:
            st.error("Please provide all required inputs.")


if __name__ == "__main__":
    main()
