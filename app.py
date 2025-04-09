import streamlit as st
import requests
import json
import csv
import os
import time
from datetime import datetime, timedelta
import pandas as pd

# Set page config
st.set_page_config(
    page_title="Gong Wizard 🧙‍♂️",
    page_icon="🧙‍♂️",
    layout="wide",
)

# App header
st.title("Gong Wizard 🧙‍♂️")
st.write("Process your Gong call data")

# Sidebar with configuration
with st.sidebar:
    st.header("Configuration")
    
    # API credentials
    st.subheader("API Credentials")
    access_key = st.text_input("Gong Access Key", type="password")
    secret_key = st.text_input("Gong Secret Key", type="password")
    
    # Date range
    st.subheader("Date Range")
    today = datetime.today()
    default_start = today - timedelta(days=7)
    start_date = st.date_input("From Date", value=default_start)
    end_date = st.date_input("To Date", value=today)
    
    # Process button
    process_button = st.button("Process Data", type="primary")

# Main area
if process_button:
    if not access_key or not secret_key:
        st.error("Please enter your Gong API credentials.")
    else:
        # Configuration
        config = {
            "access_key": access_key,
            "secret_key": secret_key,
            "from_date": start_date.strftime("%Y-%m-%d"),
            "to_date": end_date.strftime("%Y-%m-%d"),
            "output_folder": "."  # Current directory
        }
        
        # Create container for status messages
        status_container = st.container()
        
        with status_container:
            st.subheader("Processing Status")
            status = st.empty()
            
            try:
                # Step 1: Display initial status
                status.info("Starting Gong data fetching process...")
                
                # Create a base URL
                BASE_URL = "https://us-11211.api.gong.io"
                
                # Step 2: Create a simple requests session
                status.info("Connecting to Gong API...")
                session = requests.Session()
                auth = (config['access_key'], config['secret_key'])
                
                # Step 3: Fetch call list
                status.info("Fetching call list...")
                call_progress = st.progress(0)
                
                all_calls = []
                cursor = None
                params = {
                    "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                    "toDateTime": f"{config['to_date']}T23:59:59-00:00"
                }
                
                # First API call to get calls
                while True:
                    if cursor:
                        params["cursor"] = cursor
                    
                    resp = session.get(
                        f"{BASE_URL}/v2/calls", 
                        headers={"Content-Type": "application/json"}, 
                        params=params, 
                        auth=auth, 
                        timeout=30
                    )
                    
                    if resp.status_code != 200:
                        status.error(f"Error fetching call list: {resp.status_code} - {resp.text}")
                        st.stop()
                    
                    data = resp.json()
                    all_calls.extend(data.get("calls", []))
                    status.info(f"Fetched {len(all_calls)} calls so far...")
                    
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor:
                        break
                    time.sleep(1)
                
                status.success(f"✅ Successfully fetched {len(all_calls)} calls")
                
                # Step 4: Create a simple CSV summary
                status.info("Creating summary CSV...")
                
                # Define the date range for filename
                date_range = f"{start_date.strftime('%d%b%y')}-{end_date.strftime('%d%b%y')}"
                summary_csv_path = f"summary_gong_data_{date_range}.csv"
                
                # Write headers and basic call info
                with open(summary_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['CALL_ID', 'TITLE', 'DATE', 'URL'])
                    
                    for call in all_calls:
                        call_id = call.get('id', 'N/A')
                        title = call.get('title', 'N/A')
                        date = call.get('started', 'N/A')
                        url = call.get('url', 'N/A')
                        
                        writer.writerow([call_id, title, date, url])
                
                # Display the CSV as a dataframe
                df = pd.read_csv(summary_csv_path)
                st.subheader("Call Summary")
                st.dataframe(df)
                
                # Create a download button for the CSV
                with open(summary_csv_path, 'r') as file:
                    csv_data = file.read()
                
                st.download_button(
                    label="Download CSV",
                    data=csv_data,
                    file_name=summary_csv_path,
                    mime="text/csv",
                )
                
                # Save fetch stats for reference
                fetch_stats = {
                    "started_at": datetime.now().isoformat(),
                    "completed_at": datetime.now().isoformat(),
                    "total_calls": len(all_calls)
                }
                
                with open("fetch_stats.json", "w") as f:
                    json.dump(fetch_stats, f, indent=2)
                
                status.success("✅ Processing complete!")
                
            except Exception as e:
                status.error(f"Error during processing: {str(e)}")
                st.error(f"An error occurred: {str(e)}")

# Display library status for debugging
st.subheader("System Status")
try:
    import requests
    requests_status = "✅ Installed"
except ImportError:
    requests_status = "❌ Not installed"

st.write(f"Requests library: {requests_status}")