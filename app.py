import streamlit as st
import requests
import json
import csv
import os
import time
from datetime import datetime, timedelta
import pandas as pd
from fuzzywuzzy import fuzz
from urllib.parse import urlparse

# App header
st.title("Gong Wizard")
st.write("Process your Gong call data")

# Sidebar with configuration (webhook mode removed, always manual mode)
with st.sidebar:
    st.header("Configuration")
    access_key = st.text_input("Gong Access Key", type="password")
    secret_key = st.text_input("Gong Secret Key", type="password")

    # Quick date range selection dropdown
    date_range_options = ["Last 7 days", "Last 30 days", "Last 90 days"]
    today = datetime.today().date()  # Calculate once at the start

    # Initialize session state
    if "start_date" not in st.session_state:
        st.session_state.start_date = today - timedelta(days=7)
    if "end_date" not in st.session_state:
        st.session_state.end_date = today

    # Callback to update dates when dropdown changes
    def update_dates():
        selected = st.session_state.quick_range
        if selected == "Last 7 days":
            st.session_state.start_date = today - timedelta(days=7)
            st.session_state.end_date = today
        elif selected == "Last 30 days":
            st.session_state.start_date = today - timedelta(days=30)
            st.session_state.end_date = today
        elif selected == "Last 90 days":
            st.session_state.start_date = today - timedelta(days=90)
            st.session_state.end_date = today

    # Dropdown with callback
    st.selectbox("Quick Date Range", date_range_options, 
                 index=0,  # Default to "Last 7 days"
                 key="quick_range", 
                 on_change=update_dates)

    # Date input fields
    start_date = st.date_input("From Date", value=st.session_state.start_date, key="from_date")
    end_date = st.date_input("To Date", value=st.session_state.end_date, key="to_date")

    # Update session state if dates are manually edited
    st.session_state.start_date = start_date
    st.session_state.end_date = end_date

    process_button = st.button("Process Data", type="primary")

# Load mapping files
def load_normalized_orgs():
    try:
        with open("normalized_orgs.csv", newline='', encoding='utf-8') as csvfile:
            return list(csv.DictReader(csvfile))
    except:
        return []

def load_industry_mapping():
    try:
        with open("industry_mapping.csv", newline='', encoding='utf-8') as csvfile:
            return {row["Industry (API)"]: row["Industry (Normalized)"] for row in csv.DictReader(csvfile)}
    except:
        return {}

normalized_orgs = load_normalized_orgs()
industry_mapping = load_industry_mapping()

# Normalize orgs and industries
def normalize_org(account_name, website, industry_api):
    domain = urlparse(website).netloc.lower() if website and website != 'N/A' else ''
    for org in normalized_orgs:
        if domain and org.get("Primary external org domain", "").lower() == domain:
            return org.get("Org name", account_name), org.get("FINAL", industry_api), industry_api
    
    best_match = None
    highest_score = 0
    for org in normalized_orgs:
        score = fuzz.token_sort_ratio(account_name.lower(), org.get("Org name", "").lower())
        if score > highest_score and score > 80:
            highest_score = score
            best_match = org
    
    if best_match:
        return best_match.get("Org name", account_name), best_match.get("FINAL", industry_api), industry_api
    
    normalized_industry = industry_mapping.get(industry_api, None)
    if normalized_industry:
        return account_name, normalized_industry, industry_api
    
    return account_name, industry_api, industry_api

# Main processing (webhook mode removed, Edit 3)
if process_button:
    if not access_key or not secret_key:
        st.error("Please enter your Gong API credentials.")
        st.stop()
    
    config = {
        "access_key": access_key,
        "secret_key": secret_key,
        "from_date": start_date.strftime("%Y-%m-%d"),
        "to_date": end_date.strftime("%Y-%m-%d"),
        "output_folder": ".",
        "excluded_topics": ["Call Setup", "Small Talk", "Wrap-up"],
        "excluded_affiliations": ["Internal"],
        "min_word_count": 5
    }
    
    status_container = st.container()
    with status_container:
        st.subheader("Processing Status")
        status = st.empty()
        
        try:
            status.info("Starting Gong data fetching process...")
            BASE_URL = "https://us-11211.api.gong.io"
            session = requests.Session()
            auth = (config['access_key'], config['secret_key'])
            
            # Fetch call list
            status.info("Fetching call list...")
            all_calls = []
            cursor = None
            params = {
                "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                "toDateTime": f"{config['to_date']}T23:59:59-00:00"
            }
            
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
            call_ids = [call["id"] for call in all_calls]

            # Fetch detailed metadata and transcripts
            status.info("Fetching metadata and transcripts...")
            full_data = []
            batch_size = 20
            
            for i in range(0, len(call_ids), batch_size):
                batch = call_ids[i:i + batch_size]
                request_body = {
                    "filter": {
                        "callIds": batch,
                        "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                        "toDateTime": f"{config['to_date']}T23:59:59-00:00"
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
                if r.status_code != 200:
                    status.error(f"Error fetching metadata: {r.status_code} - {resp.text}")
                    st.stop()
                calls_data = r.json().get("calls", [])
                call_metadata = {call_data["metaData"]["id"]: call_data for call_data in calls_data if "metaData" in call_data and "id" in call_data["metaData"]}

                transcript_request = {
                    "filter": {
                        "callIds": batch,
                        "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                        "toDateTime": f"{config['to_date']}T23:59:59-00:00"
                    }
                }
                transcript_response = session.post(f"{BASE_URL}/v2/calls/transcript", headers={"Content-Type": "application/json"}, json=transcript_request, auth=auth, timeout=60)
                if transcript_response.status_code != 200:
                    status.error(f"Error fetching transcripts: {transcript_response.status_code} - {transcript_response.text}")
                    st.stop()
                transcripts_batch = {t["callId"]: t["transcript"] for t in transcript_response.json().get("callTranscripts", [])}

                for call_id in batch:
                    if call_id in call_metadata and call_id in transcripts_batch:
                        call = call_metadata[call_id]
                        call_date_str = "unknown-date"
                        if call.get("metaData", {}).get("started"):
                            call_date_obj = datetime.fromisoformat(call["metaData"]["started"].replace('Z', '+00:00'))
                            call_date_str = call_date_obj.strftime("%Y-%m-%d")
                        call_id_prefix = str(call_id)[:5] if call_id and len(str(call_id)) >= 5 else str(call_id)
                        short_call_id = f"{call_id_prefix}_{call_date_str}"
                        utterances_with_short_id = [{**utterance, "short_call_id": short_call_id} for utterance in transcripts_batch[call_id]]
                        call_meta = call_metadata[call_id]
                        if 'parties' in call_meta:
                            for party in call_meta['parties']:
                                if party.get('affiliation') == "Unknown":
                                    party['affiliation'] = "External"
                        call_data = {
                            "call_id": call_id,
                            "short_call_id": short_call_id,
                            "call_metadata": call_meta,
                            "utterances": utterances_with_short_id
                        }
                        full_data.append(call_data)

            # Normalize orgs
            status.info("Normalizing organizations...")
            for call_data in full_data:
                account_context = next((ctx for ctx in call_data['call_metadata'].get('context', []) if any(obj.get('objectType') == 'Account' for obj in ctx.get('objects', []))), {})
                industry = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Industry'), 'N/A')
                website = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Website'), 'N/A')
                account_name = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Name'), 'N/A')
                normalized_account, normalized_industry, industry_api = normalize_org(account_name, website, industry)
                call_data['industry_api'] = industry_api
                call_data['account_api'] = account_name
                call_data['industry_normalized'] = normalized_industry
                call_data['account_normalized'] = normalized_account

            # Save JSON (Edit 1: Add download button)
            status.info("Saving JSON...")
            date_range = f"{config['from_date'].replace('-', '')}-{config['to_date'].replace('-', '')}"
            json_path = f"JSON_gong_data_{date_range}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(full_data, f, indent=4)

            # Add download button for JSON (Edit 1)
            with open(json_path, 'r') as file:
                json_data = file.read()
            st.download_button(
                label="Download Full Transcript JSON",
                data=json_data,
                file_name=json_path,
                mime="application/json",
            )

            # Save quality CSV
            status.info("Saving quality CSV...")
            quality_csv_path = f"quality_gong_data_{date_range}.csv"
            with open(quality_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                headers = [
                    'SHORT_CALL_ID', 'CALL_ID', 'CALL_TITLE', 'CALL_DATE', 'DURATION_SECONDS', 'CALL_URL',
                    'ACCOUNT_API', 'INDUSTRY_API', 'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED',
                    'SPEAKER_ID', 'SPEAKER_EMAIL', 'SPEAKER_NAME', 'SPEAKER_JOB_TITLE', 'SPEAKER_AFFILIATION',
                    'TOPIC', 'UTTERANCE_START', 'UTTERANCE_END', 'UTTERANCE_TEXT'
                ]
                csv_writer.writerow(headers)
                for call_data in full_data:
                    call_id = call_data['call_id']
                    short_call_id = call_data['short_call_id']
                    meta = call_data['call_metadata'].get('metaData', {})
                    call_title = meta.get('title', 'N/A')
                    call_date = meta.get('started', 'N/A')
                    duration = meta.get('duration', 'N/A')
                    call_url = meta.get('url', 'N/A')
                    industry = call_data.get('industry_api', 'N/A')
                    account_name = call_data.get('account_api', 'N/A')
                    normalized_account = call_data.get('account_normalized', 'N/A')
                    normalized_industry = call_data.get('industry_normalized', 'Unknown')
                    parties = call_data['call_metadata'].get('parties', [])
                    speaker_info = {party.get('speakerId'): {
                        'email': party.get('emailAddress', 'N/A'),
                        'name': party.get('name', 'N/A'),
                        'title': party.get('title', 'Unknown'),
                        'affiliation': party.get('affiliation', 'N/A')
                    } for party in parties if party.get('speakerId')}
                    utterances = call_data.get('utterances', [])
                    for utterance in utterances:
                        speaker_id = utterance.get('speakerId', 'N/A')
                        topic = utterance.get('topic', 'N/A')
                        if topic in config["excluded_topics"]:
                            continue
                        sentences = utterance.get('sentences', [])
                        if not sentences:
                            continue
                        utterance_text = " ".join(sentence.get('text', 'N/A') for sentence in sentences)
                        word_count = len(utterance_text.split())
                        if word_count <= config["min_word_count"]:
                            continue
                        speaker = speaker_info.get(speaker_id, {'email': 'N/A', 'name': 'N/A', 'title': 'Unknown', 'affiliation': 'N/A'})
                        if speaker['affiliation'] in config["excluded_affiliations"]:
                            continue
                        start_time = sentences[0].get('start', 'N/A') if sentences else 'N/A'
                        end_time = sentences[-1].get('end', 'N/A') if sentences else 'N/A'
                        row = [
                            str(short_call_id), f'"{call_id}"', str(call_title), str(call_date), str(duration), str(call_url),
                            str(account_name), str(industry), str(normalized_account), str(normalized_industry),
                            str(speaker_id), str(speaker['email']), str(speaker['name']), str(speaker['title']), str(speaker['affiliation']),
                            str(topic), str(start_time), str(end_time), str(utterance_text)
                        ]
                        csv_writer.writerow(row)

            # Save summary CSV
            status.info("Saving summary CSV...")
            summary_csv_path = f"summary_gong_data_{date_range}.csv"
            with open(summary_csv_path, 'w', newline='', encoding='utf-8') as summary_file:
                summary_writer = csv.writer(summary_file, quoting=csv.QUOTE_MINIMAL)
                summary_headers = [
                    'CALL_ID', 'SHORT_CALL_ID', 'CALL_TITLE', 'CALL_START_TIME', 'DURATION', 'MEETING_URL',
                    'INDUSTRY', 'ACCOUNT_NAME', 'INDUSTRY_API', 'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED'
                ]
                summary_writer.writerow(summary_headers)
                for call_data in full_data:
                    call_id = call_data['call_id']
                    short_call_id = call_data['short_call_id']
                    meta = call_data['call_metadata'].get('metaData', {})
                    title = meta.get('title', 'N/A')
                    started = meta.get('started', 'N/A')
                    duration = meta.get('duration', 'N/A')
                    meeting_url = meta.get('meetingUrl', 'N/A')
                    industry = call_data.get('industry_api', 'N/A')
                    account_name = call_data.get('account_api', 'N/A')
                    normalized_account = call_data.get('account_normalized', 'N/A')
                    normalized_industry = call_data.get('industry_normalized', 'Unknown')
                    summary_row = [
                        f'"{call_id}"', str(short_call_id), str(title), str(started), str(duration), str(meeting_url),
                        str(industry), str(account_name), str(industry), str(normalized_account), str(normalized_industry)
                    ]
                    summary_writer.writerow(summary_row)

            # Display the summary CSV
            df = pd.read_csv(summary_csv_path)
            st.subheader("Call Summary")
            st.dataframe(df)
            
            # Download button for summary CSV
            with open(summary_csv_path, 'r') as file:
                csv_data = file.read()
            st.download_button(
                label="Download Summary CSV",
                data=csv_data,
                file_name=summary_csv_path,
                mime="text/csv",
            )

            # Download button for quality CSV
            with open(quality_csv_path, 'r') as file:
                csv_data = file.read()
            st.download_button(
                label="Download Quality CSV",
                data=csv_data,
                file_name=quality_csv_path,
                mime="text/csv",
            )

            # Save fetch stats
            fetch_stats = {
                "started_at": datetime.now().isoformat(),
                "completed_at": datetime.now().isoformat(),
                "total_calls": len(call_ids)
            }
            with open("fetch_stats.json", "w") as f:
                json.dump(fetch_stats, f, indent=2)
            
            status.success("✅ Processing complete!")
            
        except Exception as e:
            status.error(f"Error during processing: {str(e)}")
            st.error(f"An error occurred: {str(e)}")