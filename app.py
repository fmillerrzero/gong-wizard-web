import streamlit as st
import pandas as pd
import requests
import base64
import json
import time
from rapidfuzz import process
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gong API base URL
GONG_API_BASE = "https://us-11211.api.gong.io"

# Load CSV data with caching
@st.cache_data
def load_csv_data():
    try:
        normalized_orgs = pd.read_csv("normalized_orgs.csv")
        products = pd.read_csv("products by account.csv")
        return normalized_orgs, products
    except Exception as e:
        st.error(f"Cannot find CSV files: {str(e)}.")
        logger.error(f"CSV load error: {str(e)}")
        return None, None

# Helper functions
def normalize_org(org_name: str, normalized_orgs: pd.DataFrame, threshold: int = 80) -> Optional[str]:
    if normalized_orgs.empty or not org_name:
        return None
    try:
        match = process.extractOne(org_name, normalized_orgs["Org name"], score_cutoff=threshold)
        return normalized_orgs[normalized_orgs["Org name"] == match[0]]["FINAL"].values[0] if match else None
    except Exception as e:
        logger.warning(f"Org normalization error for {org_name}: {str(e)}")
        return None

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

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]], normalized_orgs: pd.DataFrame) -> Dict[str, Any]:
    if not call_data:
        return {}
    try:
        account_context = next((ctx for ctx in call_data.get("context", []) if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
        account_name = next((field.get("value", "Unknown") for obj in account_context.get("objects", []) for field in obj.get("fields", []) if field.get("name") == "Name"), "Unknown")
        account_id = next((obj.get("objectId", "Unknown") for obj in account_context.get("objects", []) if obj.get("objectType") == "Account"), "Unknown")
        normalized_account = normalize_org(account_name, normalized_orgs) or account_name
        call_data["account_api"] = account_name
        call_data["account_normalized"] = normalized_account
        call_data["account_id"] = account_id
        call_data["utterances"] = transcript
        return call_data
    except Exception as e:
        logger.error(f"Normalization error: {str(e)}")
        return {}

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
            account_context = next((ctx for ctx in call.get("context", []) if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
            summary_data.append({
                "CALL_ID": meta.get("id", "N/A"),
                "CALL_TITLE": meta.get("title", "N/A"),
                "CALL_DATE": datetime.fromisoformat(meta.get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d") if meta.get("started") else "N/A",
                "DURATION": format_duration(meta.get("duration", 0)),
                "MEETING_URL": meta.get("meetingUrl", "N/A"),
                "ACCOUNT_ID": call.get("account_id", "N/A"),
                "ACCOUNT_NORMALIZED": call.get("account_normalized", "N/A"),
            })
        except Exception as e:
            logger.error(f"Summary prep error: {str(e)}")
    return pd.DataFrame(summary_data)

def prepare_utterances_df(calls: List[Dict[str, Any]]) -> pd.DataFrame:
    utterances_data = []
    for call in calls:
        if not call:
            continue
        try:
            call_id = call.get("metaData", {}).get("id", "N/A")
            call_title = call.get("metaData", {}).get("title", "N/A")
            call_date = datetime.fromisoformat(call.get("metaData", {}).get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d") if call.get("metaData", {}).get("started") else "N/A"
            account_id = call.get("account_id", "N/A")
            normalized_account = call.get("account_normalized", "N/A")
            parties = call.get("parties", [])
            speaker_info = {p.get("speakerId", ""): {"name": p.get("name", "N/A"), "title": p.get("title", "Unknown")} for p in parties}
            for utterance in call.get("utterances", []):
                sentences = utterance.get("sentences", [])
                if not sentences:
                    continue
                text = " ".join(s.get("text", "N/A") for s in sentences)
                word_count = len(text.split())
                topic = utterance.get("topic", "N/A")
                if topic in ["Call Setup", "Small Talk", "Wrap-up"] or word_count < 8:
                    continue
                speaker_id = utterance.get("speakerId", "N/A")
                speaker = speaker_info.get(speaker_id, {"name": "N/A", "title": "Unknown"})
                start_time = sentences[0].get("start", 0)
                end_time = sentences[-1].get("end", 0)
                duration = format_duration(end_time - start_time) if end_time and start_time else "N/A"
                utterances_data.append({
                    "CALL_ID": call_id,
                    "CALL_TITLE": call_title,
                    "CALL_DATE": call_date,
                    "ACCOUNT_ID": account_id,
                    "ACCOUNT_NORMALIZED": normalized_account,
                    "SPEAKER_JOB_TITLE": speaker["title"],
                    "UTTERANCE_DURATION": duration,
                    "UTTERANCE_TEXT": text,
                    "TOPIC": topic
                })
        except Exception as e:
            logger.error(f"Utterance prep error: {str(e)}")
    return pd.DataFrame(utterances_data)

def apply_filters(df: pd.DataFrame, selected_products: List[str], account_products: Dict[str, set]) -> pd.DataFrame:
    filtered_df = df.copy()
    if "ACCOUNT_ID" not in filtered_df.columns:
        filtered_df["ACCOUNT_ID"] = "Unknown"
    if not selected_products:
        return filtered_df
    try:
        # Only exclude calls if they have an account ID that matches products NOT in the selected products
        exclude_account_ids = {aid for aid, prods in account_products.items() if not any(p in selected_products for p in prods)}
        filtered_df = filtered_df[~filtered_df["ACCOUNT_ID"].isin(exclude_account_ids)]
        return filtered_df
    except Exception as e:
        st.error(f"Filtering error: {str(e)}")
        logger.error(f"Filtering error: {str(e)}")
        return df

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
    normalized_orgs, products_df = load_csv_data()
    if normalized_orgs is None:
        return

    # Initialize mappings
    account_products = products_df.groupby("id")["product"].apply(set).to_dict()
    unique_products = sorted(products_df["product"].unique())

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
            for call in details:
                call_id = call.get("metaData", {}).get("id", "")
                call_transcript = transcripts.get(call_id, [])
                normalized_data = normalize_call_data(call, call_transcript, normalized_orgs)
                if normalized_data:
                    full_data.append(normalized_data)

        if not full_data:
            st.error("No call details fetched.")
            return

        summary_df = prepare_summary_df(full_data)
        utterances_df = prepare_utterances_df(full_data)

        filtered_summary_df = apply_filters(summary_df, selected_products, account_products)
        filtered_utterances_df = utterances_df[utterances_df["CALL_ID"].isin(filtered_summary_df["CALL_ID"])]
        filtered_json = [call for call in full_data if call.get("metaData", {}).get("id", "N/A") in filtered_summary_df["CALL_ID"].values]

    st.subheader("Calls")
    st.dataframe(filtered_summary_df)

    st.subheader("Utterances")
    st.dataframe(filtered_utterances_df)

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