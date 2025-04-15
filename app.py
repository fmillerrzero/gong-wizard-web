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

# Initialize session state for storing processed data
if "processed_data" not in st.session_state:
    st.session_state.processed_data = {
        "json_data": None,
        "summary_csv": None,
        "utterances_csv": None,
        "start_date_str": None,
        "end_date_str": None,
        "summary_df": None,
        "full_summary_df": None
    }
if "data_processed" not in st.session_state:
    st.session_state.data_processed = False

# Sidebar with configuration
with st.sidebar:
    st.header("Configuration")
    access_key = st.text_input("Gong Access Key", type="password")
    secret_key = st.text_input("Gong Secret Key", type="password")

    # Quick date range selection dropdown
    date_range_options = ["Last 7 days", "Last 30 days", "Last 90 days"]
    today = datetime.today().date()

    if "start_date" not in st.session_state:
        st.session_state.start_date = today - timedelta(days=7)
    if "end_date" not in st.session_state:
        st.session_state.end_date = today

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

    st.selectbox("Quick Date Range", date_range_options, 
                 index=0, key="quick_range", on_change=update_dates)

    start_date = st.date_input("From Date", value=st.session_state.start_date, key="from_date")
    end_date = st.date_input("To Date", value=st.session_state.end_date, key="to_date")

    st.session_state.start_date = start_date
    st.session_state.end_date = end_date

    # Load industry mapping for dropdown
    def load_industry_mapping():
        try:
            with open("industry_mapping.csv", newline='', encoding='utf-8') as csvfile:
                mapping = {row["Industry (API)"]: row["Industry (Normalized)"] for row in csv.DictReader(csvfile)}
                industries = sorted(set(mapping.values()))
                return mapping, industries
        except:
            return {}, []

    industry_mapping, unique_industries = load_industry_mapping()

    # Load products for dropdown
    def load_products():
        try:
            products_df = pd.read_csv("products by account.csv")
            unique_products = sorted(products_df["product"].unique())
            account_products = products_df.groupby("id")["product"].apply(set).to_dict()
            return unique_products, account_products
        except:
            return [], {}

    unique_products, account_products = load_products()

    # Add Industry and Product dropdowns, without "Unknown" in options or default
    industry_options = unique_industries.copy()  # List without "Unknown"
    selected_industries = st.multiselect("Industry", industry_options, default=[])

    product_options = unique_products.copy()  # List without "Unknown"
    selected_products = st.multiselect("Product", product_options, default=[])

    process_button = st.button("Process Data", type="primary")

# Load normalized orgs
def load_normalized_orgs():
    try:
        with open("normalized_orgs.csv", newline='', encoding='utf-8') as csvfile:
            return list(csv.DictReader(csvfile))
    except:
        return []

normalized_orgs = load_normalized_orgs()

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
    
    for org in normalized_orgs:
        if industry_api == org.get("FINAL"):
            return account_name, industry_api, industry_api
    
    normalized_industry = industry_mapping.get(industry_api, None)
    if normalized_industry:
        return account_name, normalized_industry, industry_api
    
    return account_name, industry_api, industry_api

# CSV safe value
def csv_safe_value(value):
    if value is None:
        return '""'
    str_value = str(value)
    if ',' in str_value or '\n' in str_value or '"' in str_value:
        str_value = str_value.replace('"', '""')
        return f'"{str_value}"'
    return str_value

# Format duration
def format_duration(seconds):
    try:
        seconds = int(seconds)
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes} min {remaining_seconds} sec"
    except (ValueError, TypeError):
        return "N/A"

# Main processing logic
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
        "min_word_count": 8
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
                    status.error(f"Error fetching metadata: {r.status_code} - {r.text}")
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
                        account_id = "N/A"
                        account_context = next((ctx for ctx in call_meta.get('context', []) if any(obj.get('objectType') == 'Account' for obj in ctx.get('objects', []))), {})
                        if account_context:
                            account_id = next((obj.get('objectId', 'N/A') for obj in account_context.get('objects', []) if obj.get('objectType') == 'Account'), 'N/A')
                        call_data = {
                            "call_id": str(call_id),
                            "short_call_id": short_call_id,
                            "call_metadata": call_meta,
                            "utterances": utterances_with_short_id,
                            "account_id": account_id
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
                meaningful_account = account_name if account_name.lower() not in ['n/a', 'none', 'unknown', ''] else normalized_account
                meaningful_industry = industry_api if industry_api.lower() not in ['n/a', 'none', 'unknown', ''] else normalized_industry
                call_data['industry_api'] = industry_api
                call_data['account_api'] = account_name
                call_data['industry_normalized'] = meaningful_industry
                call_data['account_normalized'] = meaningful_account

            # Save JSON to session state
            status.info("Preparing JSON data...")
            start_date_str = start_date.strftime("%d%b%y").lower()
            end_date_str = end_date.strftime("%d%b%y").lower()
            json_data = json.dumps(full_data, indent=4)
            st.session_state.processed_data["json_data"] = json_data
            st.session_state.processed_data["start_date_str"] = start_date_str
            st.session_state.processed_data["end_date_str"] = end_date_str

            # Prepare Utterances CSV data
            status.info("Preparing Utterances CSV...")
            utterances_rows = []
            headers = [
                'CALL_ID', 'SHORT_CALL_ID', 'CALL_TITLE', 'CALL_DATE', 
                'ACCOUNT_ID', 'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED', 
                'SPEAKER_JOB_TITLE', 'UTTERANCE_DURATION', 'UTTERANCE_TEXT',
                'TOPIC'
            ]
            utterances_rows.append(headers)
            
            for call_data in full_data:
                call_id = call_data['call_id']
                short_call_id = call_data['short_call_id']
                meta = call_data['call_metadata'].get('metaData', {})
                call_title = meta.get('title', 'N/A')
                call_date = 'N/A'
                if meta.get('started'):
                    try:
                        call_date_obj = datetime.fromisoformat(meta['started'].replace('Z', '+00:00'))
                        call_date = call_date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        call_date = 'N/A'
                account_id = call_data.get('account_id', 'N/A')
                normalized_account = call_data.get('account_normalized', 'N/A')
                normalized_industry = call_data.get('industry_normalized', 'Unknown')
                parties = call_data['call_metadata'].get('parties', [])
                speaker_info = {party.get('speakerId'): {
                    'name': party.get('name', 'N/A'),
                    'title': party.get('title', 'Unknown')
                } for party in parties if party.get('speakerId')}
                utterances = call_data.get('utterances', [])
                for utterance in utterances:
                    speaker_id = utterance.get('speakerId', 'N/A')
                    sentences = utterance.get('sentences', [])
                    if not sentences:
                        continue
                    utterance_text = " ".join(sentence.get('text', 'N/A') for sentence in sentences)
                    word_count = len(utterance_text.split())
                    topic = utterance.get('topic', 'N/A')
                    if topic in config["excluded_topics"] or word_count <= config["min_word_count"]:
                        continue
                    speaker = speaker_info.get(speaker_id, {'name': 'N/A', 'title': 'Unknown'})
                    start_time = sentences[0].get('start', 'N/A') if sentences else 'N/A'
                    end_time = sentences[-1].get('end', 'N/A') if sentences else 'N/A'
                    try:
                        utterance_duration = int(end_time) - int(start_time)
                        utterance_duration_formatted = format_duration(utterance_duration)
                    except (ValueError, TypeError):
                        utterance_duration_formatted = 'N/A'
                    row = [
                        f'"{call_id}"',
                        csv_safe_value(short_call_id),
                        csv_safe_value(call_title),
                        csv_safe_value(call_date),
                        csv_safe_value(account_id),
                        csv_safe_value(normalized_account),
                        csv_safe_value(normalized_industry),
                        csv_safe_value(speaker['title']),
                        csv_safe_value(utterance_duration_formatted),
                        csv_safe_value(utterance_text),
                        csv_safe_value(topic)
                    ]
                    utterances_rows.append(row)

            # Convert Utterances CSV rows to string
            utterances_csv_lines = []
            for row in utterances_rows:
                utterances_csv_lines.append(','.join(row))
            utterances_csv_data = '\n'.join(utterances_csv_lines)
            st.session_state.processed_data["utterances_csv"] = utterances_csv_data

            # Prepare Summary CSV data
            status.info("Preparing Summary CSV...")
            summary_rows = []
            summary_headers = [
                'CALL_ID', 'SHORT_CALL_ID', 'CALL_TITLE', 'CALL_DATE',
                'DURATION', 'MEETING_URL', 'WEBSITE',
                'ACCOUNT_ID', 'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED',
                'OPPORTUNITY_NAME', 'LEAD_SOURCE',
                'DEAL_STAGE', 'FORECAST_CATEGORY',
                'EXTERNAL_PARTICIPANTS', 'INTERNAL_PARTICIPANTS',
                'INTERNAL_SPEAKERS', 'EXTERNAL_SPEAKERS',
                'COMPETITION_TRACKERS', 'NEED_TRACKERS', 'TECHNOLOGY_TRACKERS',
                'PRODUCT_TRACKERS', 'SALES_TRACKERS',
                'PRICING_DURATION', 'NEXT_STEPS_DURATION',
                'CALL_BRIEF', 'KEY_POINTS'
            ]
            summary_rows.append(summary_headers)
            
            # Define tracker mapping
            tracker_mapping = {
                'Competition': {'category': 'Competition', 'topic': 'Competition'},
                'Differentiation': {'category': 'Competition', 'topic': 'Differentiation'},
                'R-Zero competitors': {'category': 'Competition', 'topic': 'Competition'},
                'Customer pain': {'category': 'Need', 'topic': 'Customer Pain Points'},
                'Energy Savings': {'category': 'Need', 'topic': 'Energy Savings'},
                'Install': {'category': 'Technology', 'topic': 'Installation'},
                'air quality': {'category': 'Product', 'topic': 'IAQ Monitoring'},
                'Filter': {'category': 'Product', 'topic': 'SecureAire'},
                'ODCV': {'category': 'Product', 'topic': 'ODCV'},
                'Timing': {'category': 'Sales', 'topic': 'Timing'},
                'Authority': {'category': 'Sales', 'topic': 'Decision Authority'},
                'Budget': {'category': 'Sales', 'topic': 'Budget'},
                'Negative Impact (by Gong)': {'category': 'Sales', 'topic': 'Blocker'}
            }
            
            for call_data in full_data:
                call_id = call_data['call_id']
                short_call_id = call_data['short_call_id']
                meta = call_data['call_metadata'].get('metaData', {})
                call_title = meta.get('title', 'N/A')
                call_date = 'N/A'
                if meta.get('started'):
                    try:
                        call_date_obj = datetime.fromisoformat(meta['started'].replace('Z', '+00:00'))
                        call_date = call_date_obj.strftime("%Y-%m-%d")
                    except ValueError:
                        call_date = 'N/A'
                duration = meta.get('duration', 'N/A')
                duration_formatted = format_duration(duration)
                meeting_url = meta.get('meetingUrl', 'N/A')
                account_id = call_data.get('account_id', 'N/A')
                normalized_account = call_data.get('account_normalized', 'N/A')
                normalized_industry = call_data.get('industry_normalized', 'Unknown')
                account_context = next((ctx for ctx in call_data['call_metadata'].get('context', []) if any(obj.get('objectType') == 'Account' for obj in ctx.get('objects', []))), {})
                website = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Website'), 'N/A')
                opportunity = next((obj for obj in account_context.get('objects', []) if obj.get('objectType') == 'Opportunity'), {})
                opportunity_name = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'Name'), 'N/A')
                lead_source = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'LeadSource'), 'N/A')
                deal_stage = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'StageName'), 'N/A')
                forecast_category = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'ForecastCategoryName'), 'N/A')
                
                # Format participants
                parties = call_data['call_metadata'].get('parties', [])
                speakers = call_data['call_metadata'].get('interaction', {}).get('speakers', [])
                talk_times = {speaker.get('id'): speaker.get('talkTime', 0) for speaker in speakers}
                total_talk_time = sum(talk_times.values())
                internal_participants_list = []
                external_participants_list = []
                all_speakers = []
                for party in parties:
                    if not party.get('speakerId'):
                        continue
                    speaker_id = party.get('speakerId')
                    name = party.get('name', 'N/A')
                    title = party.get('title', 'Unknown')
                    affiliation = party.get('affiliation', 'Unknown')
                    talk_time = talk_times.get(speaker_id, 0)
                    talk_time_pct = round((talk_time / total_talk_time * 100)) if total_talk_time > 0 else 0
                    participant_info = {
                        'name': name,
                        'title': title,
                        'talk_time': talk_time,
                        'talk_time_pct': talk_time_pct,
                        'speaker_id': speaker_id
                    }
                    if affiliation == 'Internal':
                        internal_participants_list.append(participant_info)
                    elif affiliation == 'External':
                        external_participants_list.append(participant_info)
                    all_speakers.append(participant_info)
                all_speakers.sort(key=lambda x: x['talk_time'], reverse=True)
                total_speakers = len(all_speakers)
                speaker_ranks = {speaker['speaker_id']: idx + 1 for idx, speaker in enumerate(all_speakers)}
                internal_participants_list.sort(key=lambda x: x['talk_time'], reverse=True)
                internal_formatted = []
                for participant in internal_participants_list:
                    name = participant['name']
                    title = participant['title']
                    talk_time_pct = participant['talk_time_pct']
                    rank = speaker_ranks[participant['speaker_id']]
                    if title.lower() in ['unknown', 'n/a', '']:
                        participant_str = f"{name} [talk time & rank: {talk_time_pct}% & {rank} of {total_speakers}]"
                    else:
                        participant_str = f"{name} ({title}) [talk time & rank: {talk_time_pct}% & {rank} of {total_speakers}]"
                    internal_formatted.append(participant_str)
                internal_participants = ", ".join(internal_formatted) if internal_formatted else 'N/A'
                external_participants_list.sort(key=lambda x: x['talk_time'], reverse=True)
                external_formatted = []
                for participant in external_participants_list:
                    name = participant['name']
                    title = participant['title']
                    talk_time_pct = participant['talk_time_pct']
                    rank = speaker_ranks[participant['speaker_id']]
                    if title.lower() in ['unknown', 'n/a', '']:
                        participant_str = f"{name} [talk time & rank: {talk_time_pct}% & {rank} of {total_speakers}]"
                    else:
                        participant_str = f"{name} ({title}) [talk time & rank: {talk_time_pct}% & {rank} of {total_speakers}]"
                    external_formatted.append(participant_str)
                external_participants = ", ".join(external_formatted) if external_formatted else 'N/A'
                internal_speakers = len(set(utterance.get('speakerId') for utterance in call_data.get('utterances', []) if utterance.get('speakerId') in [party.get('speakerId') for party in parties if party.get('affiliation') == 'Internal']))
                external_speakers = len(set(utterance.get('speakerId') for utterance in call_data.get('utterances', []) if utterance.get('speakerId') in [party.get('speakerId') for party in parties if party.get('affiliation') == 'External']))
                
                # Process trackers by category
                trackers = call_data['call_metadata'].get('content', {}).get('trackers', [])
                tracker_dict = {}
                for tracker in trackers:
                    tracker_name = tracker.get('name', 'N/A')
                    if tracker_name not in tracker_mapping:
                        continue
                    tracker_count = tracker.get('count', 0)
                    if tracker_name in tracker_dict:
                        tracker_dict[tracker_name] += tracker_count
                    else:
                        tracker_dict[tracker_name] = tracker_count
                
                competition_trackers = []
                need_trackers = []
                technology_trackers = []
                product_trackers = []
                sales_trackers = []
                
                for tracker_name, tracker_count in tracker_dict.items():
                    if tracker_count == 0:
                        continue
                    mapping = tracker_mapping.get(tracker_name, {'category': None, 'topic': None})
                    category = mapping['category']
                    topic = mapping['topic']
                    if not category or not topic:
                        continue
                    tracker_entry = f"{topic}:{tracker_count}"
                    if category == 'Competition':
                        competition_trackers.append(tracker_entry)
                    elif category == 'Need':
                        need_trackers.append(tracker_entry)
                    elif category == 'Technology':
                        technology_trackers.append(tracker_entry)
                    elif category == 'Product':
                        product_trackers.append(tracker_entry)
                    elif category == 'Sales':
                        sales_trackers.append(tracker_entry)
                
                competition_trackers_str = " | ".join(competition_trackers) if competition_trackers else 'N/A'
                need_trackers_str = " | ".join(need_trackers) if need_trackers else 'N/A'
                technology_trackers_str = " | ".join(technology_trackers) if technology_trackers else 'N/A'
                product_trackers_str = " | ".join(product_trackers) if product_trackers else 'N/A'
                sales_trackers_str = " | ".join(sales_trackers) if sales_trackers else 'N/A'
                
                topics = call_data['call_metadata'].get('content', {}).get('topics', [])
                pricing_duration = next((topic.get('duration', 0) for topic in topics if topic.get('name') == 'Pricing'), 0)
                pricing_duration_formatted = format_duration(pricing_duration)
                next_steps_duration = next((topic.get('duration', 0) for topic in topics if topic.get('name') == 'Next Steps'), 0)
                next_steps_duration_formatted = format_duration(next_steps_duration)
                call_brief = call_data['call_metadata'].get('content', {}).get('brief', 'N/A')
                key_points = call_data['call_metadata'].get('content', {}).get('keyPoints', [])
                key_points_str = ";".join([point.get('text', 'N/A') for point in key_points]) if key_points else 'N/A'
                summary_row = [
                    f'"{call_id}"',
                    csv_safe_value(short_call_id),
                    csv_safe_value(call_title),
                    csv_safe_value(call_date),
                    csv_safe_value(duration_formatted),
                    csv_safe_value(meeting_url),
                    csv_safe_value(website),
                    csv_safe_value(account_id),
                    csv_safe_value(normalized_account),
                    csv_safe_value(normalized_industry),
                    csv_safe_value(opportunity_name),
                    csv_safe_value(lead_source),
                    csv_safe_value(deal_stage),
                    csv_safe_value(forecast_category),
                    csv_safe_value(external_participants),
                    csv_safe_value(internal_participants),
                    csv_safe_value(internal_speakers),
                    csv_safe_value(external_speakers),
                    csv_safe_value(competition_trackers_str),
                    csv_safe_value(need_trackers_str),
                    csv_safe_value(technology_trackers_str),
                    csv_safe_value(product_trackers_str),
                    csv_safe_value(sales_trackers_str),
                    csv_safe_value(pricing_duration_formatted),
                    csv_safe_value(next_steps_duration_formatted),
                    csv_safe_value(call_brief),
                    csv_safe_value(key_points_str)
                ]
                summary_rows.append(summary_row)

            # Convert Summary CSV rows to string
            summary_csv_lines = []
            for row in summary_rows:
                summary_csv_lines.append(','.join(row))
            summary_csv_data = '\n'.join(summary_csv_lines)
            st.session_state.processed_data["summary_csv"] = summary_csv_data

            # Store the full Summary table data in session state
            df = pd.DataFrame([row for row in summary_rows[1:]], columns=summary_headers)
            st.session_state.processed_data["full_summary_df"] = df

            # Mark processing as complete
            st.session_state.data_processed = True
            status.success("✅ Processing complete!")
            
        except Exception as e:
            status.error(f"Error during processing: {str(e)}")
            st.error(f"An error occurred: {str(e)}")
            st.stop()

# Apply filters and display the filtered Summary table
if st.session_state.data_processed and st.session_state.processed_data["full_summary_df"] is not None:
    df = st.session_state.processed_data["full_summary_df"].copy()
    
    # Create a mask that starts with all rows selected
    include_mask = pd.Series(True, index=df.index)
    filter_applied = False
    
    # Apply Industry filter if selected
    if selected_industries:  # If user selected industries, filter accordingly
        filter_applied = True
        industry_mask = df['INDUSTRY_NORMALIZED'].fillna('Unknown').str.lower().isin([ind.lower() for ind in selected_industries])
    else:  # If no industries selected, treat as if "Unknown" is selected
        filter_applied = True
        industry_mask = (
            df['INDUSTRY_NORMALIZED'].isna() | 
            df['INDUSTRY_NORMALIZED'].str.lower().isin(['n/a', 'unknown', 'none', ''])
        )
    
    include_mask = include_mask & industry_mask

    # Apply Product filter if selected
    if selected_products:  # If user selected products, filter accordingly
        filter_applied = True
        matching_account_ids = set()
        for account_id, products in account_products.items():
            if any(product in selected_products for product in products):
                matching_account_ids.add(account_id)
        product_mask = df['ACCOUNT_ID'].isin(matching_account_ids)
    else:  # If no products selected, treat as if "Unknown" is selected
        filter_applied = True
        product_mask = (
            df['ACCOUNT_ID'].isna() | 
            df['ACCOUNT_ID'].str.lower().isin(['n/a', 'unknown', 'none', ''])
        )
    
    include_mask = include_mask & product_mask
    
    # Apply the combined filter mask
    if filter_applied:
        filtered_df = df[include_mask]
    else:
        filtered_df = df
    
    st.session_state.processed_data["summary_df"] = filtered_df
    
    # Display filtering statistics
    if filter_applied:
        total_calls = len(df)
        shown_calls = len(filtered_df)
        percentage = (shown_calls / total_calls * 100) if total_calls > 0 else 0
        st.info(f"Showing {shown_calls} of {total_calls} calls ({percentage:.1f}%)")

    # Display the filtered data
    st.subheader("Call Summary")
    if filtered_df.empty:
        st.write("No data matches your filters.")
    else:
        st.dataframe(filtered_df)

# Display download buttons only if data is processed
if st.session_state.data_processed:
    start_date_str = st.session_state.processed_data["start_date_str"]
    end_date_str = st.session_state.processed_data["end_date_str"]
    
    # Download Full Summary CSV
    st.download_button(
        label="Download Full Summary CSV",
        data=st.session_state.processed_data["summary_csv"],
        file_name=f"full_summary_gong_{start_date_str}_to_{end_date_str}.csv",
        mime="text/csv",
        key="download_full_summary_csv"
    )

    # Download Filtered Summary CSV
    if st.session_state.processed_data["summary_df"] is not None:
        filtered_csv = st.session_state.processed_data["summary_df"].to_csv(index=False)
        st.download_button(
            label="Download Filtered Summary CSV",
            data=filtered_csv,
            file_name=f"filtered_summary_gong_{start_date_str}_to_{end_date_str}.csv",
            mime="text/csv",
            key="download_filtered_summary_csv"
        )

    # Download Utterances CSV
    st.download_button(
        label="Download Utterances CSV",
        data=st.session_state.processed_data["utterances_csv"],
        file_name=f"utterances_gong_{start_date_str}_to_{end_date_str}.csv",
        mime="text/csv",
        key="download_utterances_csv"
    )

    # Download Full Transcript JSON
    st.download_button(
        label="Download Full Transcript JSON",
        data=st.session_state.processed_data["json_data"],
        file_name=f"json_gong_{start_date_str}_to_{end_date_str}.json",
        mime="application/json",
        key="download_json"
    )