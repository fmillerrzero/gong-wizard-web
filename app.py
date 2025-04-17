import streamlit as st
import pandas as pd
import requests
import base64
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gong API base URL
GONG_API_BASE = "https://us-11211.api.gong.io"

# Tracker renaming dictionary
TRACKER_RENAMES = {
    "Competition": "General Competitors",
    "Differentiation": "Differentiation",
    "R-Zero competitors": "Occupancy Analytics Competitors",
    "Install": "Sensor Installation",
    "air quality": "IAQ Monitoring",
    "Filter": "SecureAire",
    "Timing": "Project Timing",
    "Authority": "Decision Authority",
    "Negative Impact (by Gong)": "Deal Blocker"
}

# Load CSV data with caching
@st.cache_data
def load_csv_data():
    try:
        products = pd.read_csv("products_by_account.csv")
        return products
    except Exception as e:
        st.error(f"Cannot find products CSV: {str(e)}.")
        logger.error(f"CSV load error: {str(e)}")
        return None

# Helper functions
def create_auth_header(access_key: str, secret_key: str) -> Dict[str, str]:
    credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
    return {"Authorization": f"Basic {credentials}"}

def fetch_call_list(session: requests.Session, from_date: str, to_date: str, max_attempts: int = 3) -> List[str]:
    url = f"{GONG_API_BASE}/v2/calls"
    params = {"fromDateTime": from_date, "toDateTime": to_date}
    call_ids = []
    for attempt in range(max_attempts):
        try:
            while True:
                response = session.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    call_ids.extend(call["id"] for call in data.get("calls", []))
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor:
                        break
                    params["cursor"] = cursor
                    time.sleep(1)
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                    time.sleep(wait_time)
                    continue
                else:
                    st.error(f"Failed to fetch calls: {response.status_code} - {response.text}")
                    return call_ids
            break
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep((2 ** attempt) * 1)
            else:
                st.error(f"Call list error: {str(e)}")
                logger.error(f"Call list error: {str(e)}")
    return call_ids

def fetch_call_details(session: requests.Session, call_ids: List[str], max_attempts: int = 3) -> List[Dict[str, Any]]:
    url = f"{GONG_API_BASE}/v2/calls/extensive"
    request_body = {
        "filter": {"callIds": call_ids},
        "contentSelector": {
            "context": "Extended",
            "exposedFields": {
                "parties": True,
                "content": {"structure": True, "topics": True, "trackers": True, "brief": True, "keyPoints": True, "callOutcome": True},
                "interaction": {"speakers": True, "personInteractionStats": True, "questions": True, "video": True},
                "collaboration": {"publicComments": True},
                "media": True
            }
        }
    }
    for attempt in range(max_attempts):
        try:
            response = session.post(url, json=request_body, timeout=60)
            if response.status_code == 200:
                return response.json().get("calls", [])
            elif response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                time.sleep(wait_time)
            else:
                logger.warning(f"Call details fetch failed: {response.status_code} - {response.text}")
                break
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep((2 ** attempt) * 1)
            else:
                logger.warning(f"Error fetching call details: {str(e)}")
                break
    return []

def fetch_transcript(session: requests.Session, call_ids: List[str], max_attempts: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    url = f"{GONG_API_BASE}/v2/calls/transcript"
    request_body = {"filter": {"callIds": call_ids}}
    for attempt in range(max_attempts):
        try:
            response = session.post(url, json=request_body, timeout=60)
            if response.status_code == 200:
                transcripts = response.json().get("callTranscripts", [])
                return {t["callId"]: t.get("transcript", []) for t in transcripts}
            elif response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                time.sleep(wait_time)
            else:
                logger.warning(f"Transcript fetch failed: {response.status_code} - {response.text}")
                break
        except Exception as e:
            if attempt < max_attempts - 1:
                time.sleep((2 ** attempt) * 1)
            else:
                logger.warning(f"Error fetching transcripts: {str(e)}")
                break
    return {call_id: [] for call_id in call_ids}

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]], account_products: Dict[str, List[str]]) -> Dict[str, Any]:
    if not call_data:
        return {}
    try:
        account_context = next((ctx for ctx in call_data.get("context", []) if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
        account_name = next((field.get("value", "Unknown") for obj in account_context.get("objects", []) for field in obj.get("fields", []) if field.get("name") == "Name"), "Unknown")
        account_id = next((obj.get("objectId", "Unknown") for obj in account_context.get("objects", []) if obj.get("objectType") == "Account"), "Unknown")
        account_website = next((field.get("value", "Unknown") for obj in account_context.get("objects", []) for field in obj.get("fields", []) if field.get("name") == "Website"), "Unknown")
        call_data["account_name"] = account_name
        call_data["account_id"] = account_id
        call_data["account_website"] = account_website
        call_data["utterances"] = transcript
        
        # Debug log the account ID
        logger.info(f"Normalizing call for Account ID from API: {account_id}")
        
        # Get products with EXACT MATCHING ONLY for Salesforce IDs
        products = []
        matched_key = None
        
        if account_id and account_id != "Unknown":
            # Clean up the ID
            clean_id = str(account_id).strip('"')
            
            # Try direct match ONLY
            if clean_id in account_products:
                products = account_products[clean_id]
                matched_key = clean_id
                logger.info(f"Direct match found: {clean_id}")
            else:
                # No match found, log for QA
                logger.warning(f"No product mapping found for account ID: {account_id}")
        
        call_data["products"] = products
        call_data["matched_account_id"] = matched_key  # Store the matched key for debugging
        
        return call_data
    except Exception as e:
        logger.error(f"Normalization error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return call_data

def format_duration(seconds):
    try:
        seconds = int(seconds)
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes} min {remaining_seconds} sec"
    except (ValueError, TypeError):
        return "N/A"

def prepare_summary_df(calls: List[Dict[str, Any]]) -> pd.DataFrame:
    summary_data = []
    for call in calls:
        if not call:
            continue
        try:
            meta = call.get("metaData", {})
            parties = call.get("parties", [])
            
            # Simplified speaker handling - just collect those who spoke
            internal_speakers = []
            external_speakers = []
            unknown_speakers = []
            
            # Process all parties (people on the call)
            for party in parties:
                name = party.get("name", "")
                if name and name != "N/A":
                    title = party.get("title", "")
                    affiliation = party.get("affiliation", "Unknown")
                    
                    speaker_str = f"{name}" + (f", {title}" if title else "")
                    
                    if affiliation == "Internal":
                        internal_speakers.append(speaker_str)
                    elif affiliation == "External":
                        external_speakers.append(speaker_str)
                    else:
                        unknown_speakers.append(speaker_str)
            
            # Default to "None" if no speakers in group
            if not internal_speakers:
                internal_speakers = ["None"]
            if not external_speakers:
                external_speakers = ["None"]
            if not unknown_speakers:
                unknown_speakers = ["None"]
            
            # Process trackers
            trackers = call.get("content", {}).get("trackers", [])
            tracker_list = []
            for tracker in trackers:
                count = tracker.get("count", 0)
                if count > 0:
                    name = TRACKER_RENAMES.get(tracker.get("name", ""), tracker.get("name", "Unknown"))
                    tracker_list.append((name, count))
            tracker_list.sort(key=lambda x: x[1], reverse=True)
            tracker_str = "|".join(f"{name}:{count}" for name, count in tracker_list if name != "Unknown")
            
            # Get products
            products = call.get("products", [])
            products_str = "|".join(products) if products else "Unmapped"
            
            # Raw account ID for debugging
            raw_account_id = call.get("account_id", "N/A")
            matched_account_id = call.get("matched_account_id", "None")
            
            summary_data.append({
                "call_id": f'"{meta.get("id", "N/A")}"',
                "call_title": meta.get("title", "N/A"),
                "call_date": datetime.fromisoformat(meta.get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d") if meta.get("started") else "N/A",
                "duration": format_duration(meta.get("duration", 0)),
                "meeting_url": meta.get("meetingUrl", "N/A"),
                "account_id": raw_account_id,
                "matched_account_id": matched_account_id,
                "account_name": call.get("account_name", "N/A"),
                "account_website": call.get("account_website", "N/A"),
                "products": products_str,
                "trackers": tracker_str,
                "internal_speakers": "|".join(internal_speakers),
                "external_speakers": "|".join(external_speakers),
                "unknown_speakers": "|".join(unknown_speakers)
            })
        except Exception as e:
            logger.error(f"Summary prep error: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    return pd.DataFrame(summary_data)

def prepare_utterances_df(calls: List[Dict[str, Any]]) -> pd.DataFrame:
    utterances_data = []
    for call in calls:
        if not call:
            continue
        try:
            call_id = str(call.get("metaData", {}).get("id", ""))
            call_title = call.get("metaData", {}).get("title", "N/A")
            call_date = datetime.fromisoformat(call.get("metaData", {}).get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d") if call.get("metaData", {}).get("started") else "N/A"
            account_id = call.get("account_id", "N/A")
            account_name = call.get("account_name", "N/A")
            account_website = call.get("account_website", "N/A")
            products = call.get("products", [])
            products_str = "|".join(products) if products else "Unmapped"
            parties = call.get("parties", [])
            
            # Create speaker info dictionary with fallback to "Unknown" for missing values
            speaker_info = {}
            for party in parties:
                speaker_id = party.get("speakerId", "")
                if speaker_id:
                    speaker_info[speaker_id] = {
                        "name": party.get("name", "Unknown"),
                        "title": party.get("title", ""),
                        "affiliation": party.get("affiliation", "Unknown")
                    }
            
            for utterance in call.get("utterances", []):
                sentences = utterance.get("sentences", [])
                if not sentences:
                    continue
                text = " ".join(s.get("text", "N/A") for s in sentences)
                word_count = len(text.split())
                topic = utterance.get("topic", "N/A")
                speaker_id = utterance.get("speakerId", "")
                
                # Use fallback for missing speaker information
                speaker = speaker_info.get(speaker_id, {"name": "Unknown", "title": "", "affiliation": "Unknown"})
                
                if speaker["affiliation"] == "Internal" or word_count < 8 or topic in ["Call Setup", "Small Talk", "Wrap-up"]:
                    continue
                start_time = sentences[0].get("start", 0)
                end_time = sentences[-1].get("end", 0)
                duration = format_duration(end_time - start_time) if end_time and start_time else "N/A"
                utterances_data.append({
                    "call_id": f'"{call_id}"',
                    "call_title": call_title,
                    "call_date": call_date,
                    "account_id": account_id,
                    "account_name": account_name,
                    "account_website": account_website,
                    "products": products_str,
                    "speaker_name": speaker["name"],
                    "speaker_job_title": speaker["title"] if speaker["title"] else "",
                    "speaker_affiliation": speaker["affiliation"],
                    "utterance_duration": duration,
                    "utterance_text": text,
                    "topic": topic
                })
        except Exception as e:
            logger.error(f"Utterance prep error: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    return pd.DataFrame(utterances_data)

def apply_filters(df: pd.DataFrame, selected_products: List[str], account_products: Dict[str, set]) -> pd.DataFrame:
    if not selected_products or "Select All" in selected_products:
        return df.copy()
    try:
        # Create a filtered DataFrame
        filtered_df = df.copy()
        
        # Only filter out accounts if we're 100% sure they don't match any selected product
        known_accounts = set(account_products.keys())
        
        # Mark rows where account_id is in our known list but doesn't have any of the selected products
        exclude_account_ids = {aid for aid in known_accounts 
                               if aid in account_products 
                               and not any(p in selected_products for p in account_products[aid])}
        
        # Apply the filter:
        # 1. Keep rows where matched_account_id is not None
        # 2. AND matched_account_id is not in the exclude list
        if "matched_account_id" in df.columns:
            # First, convert None/NaN values to "None" string for consistent filtering
            filtered_df["matched_account_id"] = filtered_df["matched_account_id"].fillna("None")
            # Keep only rows with valid matched account IDs not in the exclude list
            filtered_df = filtered_df[
                (filtered_df["matched_account_id"] != "None") & 
                (~filtered_df["matched_account_id"].isin(exclude_account_ids))
            ]
        else:
            # Fallback to using account_id if matched_account_id column doesn't exist
            filtered_df = filtered_df[~filtered_df["account_id"].isin(exclude_account_ids)]
        
        return filtered_df
    except Exception as e:
        st.error(f"Filtering error: {str(e)}")
        logger.error(f"Filtering error: {str(e)}")
        return df.copy()

def download_csv(df: pd.DataFrame, filename: str, label: str):
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(label, data=csv, file_name=filename, mime="text/csv")

def download_json(data: Any, filename: str, label: str):
    json_data = json.dumps(data, indent=4, ensure_ascii=False, default=str)
    st.download_button(label, data=json_data, file_name=filename, mime="application/json")

# Main app
def main():
    st.title("📞 Gong Wizard")

    # Load CSVs
    products_df = load_csv_data()
    if products_df is None:
        return

    # Debug the products data
    logger.info(f"Product data loaded: {len(products_df)} rows")
    if len(products_df) > 0:
        logger.info(f"Sample account IDs: {products_df['Account ID'].head(5).tolist()}")
        logger.info(f"Sample products: {products_df['Product'].head(5).tolist()}")
    
    # Create account to products mapping (an account may have multiple products across different rows)
    account_products = {}
    for _, row in products_df.iterrows():
        account_id = str(row['Account ID'])
        product = row['Product']
        if account_id not in account_products:
            account_products[account_id] = []
        if product not in account_products[account_id]:
            account_products[account_id].append(product)
    
    # Debug the mapping
    logger.info(f"Account to product mapping created: {len(account_products)} accounts")
    if account_products:
        sample_keys = list(account_products.keys())[:3]
        for key in sample_keys:
            logger.info(f"Sample mapping - Account {key}: Products {account_products[key]}")
    
    unique_products = sorted(products_df["Product"].unique())
    logger.info(f"Unique products: {unique_products}")

    # Sidebar
    with st.sidebar:
        st.header("Configuration")
        access_key = st.text_input("Gong Access Key", type="password")
        secret_key = st.text_input("Gong Secret Key", type="password")
        headers = create_auth_header(access_key, secret_key) if access_key and secret_key else {}
        
        today = datetime.today().date()
        if "start_date" not in st.session_state:
            st.session_state.start_date = today - timedelta(days=7)
        if "end_date" not in st.session_state:
            st.session_state.end_date = today

        st.session_state.start_date = st.date_input("From Date", value=st.session_state.start_date)
        st.session_state.end_date = st.date_input("To Date", value=st.session_state.end_date)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Last 7 Days"):
                st.session_state.start_date = today - timedelta(days=7)
                st.session_state.end_date = today
                st.rerun()
        with col2:
            if st.button("Last 30 Days"):
                st.session_state.start_date = today - timedelta(days=30)
                st.session_state.end_date = today
                st.rerun()

        selected_products = st.multiselect("Product", ["Select All"] + unique_products, default=["Select All"])
        
        # Add debug mode option
        debug_mode = st.checkbox("Debug Mode", value=False)

    # Validate headers
    if not headers:
        st.warning("Please provide your Gong Access Key and Secret Key.")
        return

    # Normalize selections
    if "Select All" in selected_products:
        selected_products = unique_products

    # Fetch calls and display results
    with st.spinner("Fetching calls..."):
        session = requests.Session()
        session.headers.update(headers)
        call_ids = fetch_call_list(session, st.session_state.start_date.isoformat() + "T00:00:00Z", st.session_state.end_date.isoformat() + "T23:59:59Z")
        if not call_ids:
            st.error("No calls found.")
            return

        full_data = []
        batch_size = 50
        for i in range(0, len(call_ids), batch_size):
            batch = call_ids[i:i + batch_size]
            details = fetch_call_details(session, batch)
            transcripts = fetch_transcript(session, batch)
            
            # Add enhanced debug mode display
            if debug_mode and i == 0 and details:
                st.subheader("Debug Information")
                if call_ids:
                    st.write(f"Number of calls found: {len(call_ids)}")
                
                # Display the first call's raw API data if available
                if details and len(details) > 0:
                    st.subheader("First Call Raw Data")
                    # Display account ID from API
                    account_context = next((ctx for ctx in details[0].get("context", []) if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
                    account_id = next((obj.get("objectId", "Unknown") for obj in account_context.get("objects", []) if obj.get("objectType") == "Account"), "Unknown")
                    st.write(f"Account ID from API: {account_id}")
                    
                    # Display parties (speakers) data separately for easier debugging
                    parties = details[0].get("parties", [])
                    if parties:
                        st.subheader("Parties/Speakers Data")
                        st.json(parties)
            
            for call in details:
                call_id = str(call.get("metaData", {}).get("id", ""))
                call_transcript = transcripts.get(call_id, [])
                normalized_data = normalize_call_data(call, call_transcript, account_products)
                if normalized_data:
                    full_data.append(normalized_data)

        if not full_data:
            st.error("No call details fetched.")
            return

        summary_df = prepare_summary_df(full_data)
        utterances_df = prepare_utterances_df(full_data)

        filtered_summary_df = apply_filters(summary_df, selected_products, account_products)
        filtered_utterances_df = utterances_df[utterances_df["call_id"].isin(filtered_summary_df["call_id"])]
        
        # Improved matching for filtered JSON
        call_id_set = set(id.strip('"') for id in filtered_summary_df["call_id"])
        filtered_json = [call for call in full_data if call.get("metaData", {}).get("id", "N/A") in call_id_set]

    st.subheader("Filtered Calls")
    st.dataframe(filtered_summary_df)
    
    if debug_mode:
        st.subheader("Debug: Product Mapping")
        # Show a sample of the account_products mapping
        sample_mapping = {k: v for i, (k, v) in enumerate(account_products.items()) if i < 5}
        st.json(sample_mapping)
        
        # Show data from the first few rows of the filtered summary
        st.subheader("Debug: First 5 rows of filtered summary")
        st.write(filtered_summary_df[["account_id", "matched_account_id", "products"]].head(5))
        
        # Show how many accounts are mapped vs unmapped
        if len(filtered_summary_df) > 0:
            mapped = filtered_summary_df[filtered_summary_df["products"] != "Unmapped"].shape[0]
            unmapped = filtered_summary_df[filtered_summary_df["products"] == "Unmapped"].shape[0]
            st.write(f"Mapped accounts: {mapped}, Unmapped accounts: {unmapped}")
            
            # Show account IDs that are unmapped
            if unmapped > 0:
                unmapped_accounts = filtered_summary_df[filtered_summary_df["products"] == "Unmapped"]["account_id"].unique()
                st.write(f"Sample unmapped account IDs: {list(unmapped_accounts)[:5]}")
                
            # Test direct matching on unmapped accounts
                st.subheader("Testing direct matching on unmapped accounts")
                for acc_id in list(unmapped_accounts)[:5]:
                    acc_id_str = str(acc_id).strip('"')
                    direct_match = acc_id_str in account_products
                    st.write(f"Account ID: {acc_id_str}")
                    st.write(f"Direct match exists: {direct_match}")
                    if direct_match:
                        st.write(f"Products for match: {account_products[acc_id_str]}")

    start_date_str = st.session_state.start_date.strftime("%d%b%y").lower()
    end_date_str = st.session_state.end_date.strftime("%Y-%m-%d")

    st.subheader("Download Options")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Unfiltered Data**")
        download_csv(summary_df, f"unfiltered_summary_gong_{start_date_str}_to_{end_date_str}.csv", "Download Unfiltered Calls CSV")
        download_csv(utterances_df, f"unfiltered_utterances_gong_{start_date_str}_to_{end_date_str}.csv", "Download Unfiltered Utterances CSV")
        download_json(full_data, f"unfiltered_json_gong_{start_date_str}_to_{end_date_str}.json", "Download Unfiltered JSON")
    with col2:
        st.markdown("**Filtered Data**")
        download_csv(filtered_summary_df, f"filtered_summary_gong_{start_date_str}_to_{end_date_str}.csv", "Download Filtered Calls CSV")
        download_csv(filtered_utterances_df, f"filtered_utterances_gong_{start_date_str}_to_{end_date_str}.csv", "Download Filtered Utterances CSV")
        download_json(filtered_json, f"filtered_json_gong_{start_date_str}_to_{end_date_str}.json", "Download Filtered JSON")

if __name__ == "__main__":
    main()
