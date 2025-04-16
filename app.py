import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime
import time

# App header
st.title("Gong Metadata Extractor")

# Sidebar for configuration
with st.sidebar:
    access_key = st.text_input("Gong Access Key", type="password")
    secret_key = st.text_input("Gong Secret Key", type="password")
    process_button = st.button("Fetch Metadata", type="primary")

# Function to flatten nested dictionaries
def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep).items())
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}[{i}]", sep).items())
                else:
                    items.append((f"{new_key}[{i}]", item))
        else:
            items.append((new_key, value))
    return dict(items)

# Main processing logic
if process_button:
    if access_key and secret_key:
        try:
            BASE_URL = "https://us-11211.api.gong.io"
            session = requests.Session()
            auth = (access_key, secret_key)
            
            # Date range: from March 15, 2025 to today (April 16, 2025)
            from_date = "2025-03-15T00:00:00-00:00"
            to_date = datetime.today().strftime("%Y-%m-%dT23:59:59-00:00")
            
            all_calls = []
            cursor = None
            params = {
                "fromDateTime": from_date,
                "toDateTime": to_date
            }
            
            # Fetch call list
            st.write("Fetching call list...")
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
                resp.raise_for_status()
                data = resp.json()
                all_calls.extend(data.get("calls", []))
                cursor = data.get("records", {}).get("cursor")
                if not cursor:
                    break
                time.sleep(1)
            
            call_ids = [call["id"] for call in all_calls]
            st.write(f"Found {len(call_ids)} calls since March 15, 2025.")

            # Fetch detailed metadata in batches
            full_data = []
            batch_size = 20
            for i in range(0, len(call_ids), batch_size):
                batch = call_ids[i:i + batch_size]
                request_body = {
                    "filter": {
                        "callIds": batch,
                        "fromDateTime": from_date,
                        "toDateTime": to_date
                    },
                    "contentSelector": {
                        "context": "Extended",
                        "exposedFields": {
                            "parties": True,
                            "content": {
                                "structure": True,
                                "topics": True,
                                "trackers": True,
                                "brief": True,
                                "keyPoints": True,
                                "callOutcome": True
                            },
                            "interaction": {
                                "speakers": True,
                                "personInteractionStats": True,
                                "questions": True,
                                "video": True
                            },
                            "collaboration": {
                                "publicComments": True
                            },
                            "media": True
                        }
                    }
                }
                r = session.post(f"{BASE_URL}/v2/calls/extensive", headers={"Content-Type": "application/json"}, json=request_body, auth=auth, timeout=60)
                r.raise_for_status()
                calls_data = r.json().get("calls", [])
                full_data.extend(calls_data)
                st.write(f"Processed {len(full_data)} of {len(call_ids)} calls...")

            # Flatten the data into a list of dictionaries
            flattened_calls = []
            for call in full_data:
                flattened_call = flatten_dict(call, sep='.')
                flattened_calls.append(flattened_call)

            # Convert to DataFrame
            df = pd.DataFrame(flattened_calls)

            # Ensure CALL_ID is quoted string
            if 'metaData.id' in df.columns:
                df['metaData.id'] = df['metaData.id'].apply(lambda x: f'"{x}"')

            # Save to CSV and provide download
            csv_data = df.to_csv(index=False)
            st.download_button(
                label="Download Metadata CSV",
                data=csv_data,
                file_name="gong_metadata_2025-03-15_to_2025-04-16.csv",
                mime="text/csv",
                key="download_metadata_csv"
            )

            # Display the DataFrame for preview
            st.write("Preview of Metadata:")
            st.dataframe(df)

            st.success("Metadata extraction complete!")

        except requests.exceptions.RequestException as e:
            st.error(f"Failed to fetch data from Gong API: {str(e)}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")