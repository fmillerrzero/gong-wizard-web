import streamlit as st
import pandas as pd
import requests
import base64
import json
import time
from rapidfuzz import process
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gong API base URL
GONG_API_BASE = "https://us-11211.api.gong.io"

# Initialize session state
if "step" not in st.session_state:
    st.session_state.step = 1
if "processed_data" not in st.session_state:
    st.session_state.processed_data = {}
if "config" not in st.session_state:
    st.session_state.config = {
        "min_word_count": 8,
        "max_attempts": 3,
        "excluded_topics": ["Call Setup", "Small Talk", "Wrap-up"],
        "excluded_affiliations": ["Internal"]
    }

# Load CSV data with caching
@st.cache_data
def load_csv_data():
    try:
        industry_mapping = pd.read_csv("industry_mapping.csv")
        ui_industries = pd.read_csv("Industry UI - Sheet17.csv")
        normalized_orgs = pd.read_csv("normalized_orgs.csv")
        products = pd.read_csv("products by account.csv")
        return industry_mapping, ui_industries, normalized_orgs, products
    except Exception as e:
        st.error(f"Cannot find CSV files: {str(e)}. Please check your folder.")
        logger.error(f"CSV load error: {str(e)}")
        return None, None, None, None

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

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]], normalized_orgs: pd.DataFrame, api_to_normalized: Dict[str, str]) -> Dict[str, Any]:
    if not call_data:
        return {}
    try:
        account_context = next((ctx for ctx in call_data.get("context", []) if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
        industry_api = next((field.get("value", "Unknown") for obj in account_context.get("objects", []) for field in obj.get("fields", []) if field.get("name") == "Industry"), "Unknown")
        account_name = next((field.get("value", "Unknown") for obj in account_context.get("objects", []) for field in obj.get("fields", []) if field.get("name") == "Name"), "Unknown")
        account_id = next((obj.get("objectId", "Unknown") for obj in account_context.get("objects", []) if obj.get("objectType") == "Account"), "Unknown")
        normalized_industry = api_to_normalized.get(industry_api, industry_api)
        normalized_account = normalize_org(account_name, normalized_orgs) or account_name
        call_data["industry_api"] = industry_api
        call_data["account_api"] = account_name
        call_data["industry_normalized"] = normalized_industry
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

def csv_safe_value(value):
    if value is None:
        return '""'
    str_value = str(value)
    if ',' in str_value or '\n' in str_value or '"' in str_value:
        str_value = str_value.replace('"', '""')
        return f'"{str_value}"'
    return str_value

def prepare_summary_df(calls: List[Dict[str, Any]]) -> pd.DataFrame:
    summary_data = []
    for call in calls:
        if not call:
            continue
        try:
            meta = call.get("metaData", {})
            account_context = next((ctx for ctx in call.get("context", []) if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
            opportunity = next((obj for obj in account_context.get("objects", []) if obj.get("objectType") == "Opportunity"), {})
            parties = call.get("parties", [])
            speakers = call.get("interaction", {}).get("speakers", [])
            talk_times = {speaker.get("id", ""): speaker.get("talkTime", 0) for speaker in speakers}
            total_talk_time = sum(talk_times.values())
            internal_participants = []
            external_participants = []
            for party in parties:
                speaker_id = party.get("speakerId")
                if not speaker_id:
                    continue
                name = party.get("name", "N/A")
                title = party.get("title", "Unknown")
                affiliation = party.get("affiliation", "Unknown")
                talk_time = talk_times.get(speaker_id, 0)
                talk_time_pct = round((talk_time / total_talk_time * 100)) if total_talk_time > 0 else 0
                participant_info = f"{name} ({title}) [talk time: {talk_time_pct}%]" if title.lower() not in ["unknown", "n/a", ""] else f"{name} [talk time: {talk_time_pct}%]"
                if affiliation in st.session_state.config["excluded_affiliations"]:
                    internal_participants.append(participant_info)
                elif affiliation == "External":
                    external_participants.append(participant_info)
            trackers = call.get("content", {}).get("trackers", [])
            tracker_dict = {tracker.get("name", "N/A"): tracker.get("count", 0) for tracker in trackers}
            sales_trackers = [f"{name}:{count}" for name, count in tracker_dict.items() if count > 0]
            topics = call.get("content", {}).get("topics", [])
            summary_data.append({
                "CALL_ID": meta.get("id", "N/A"),
                "SHORT_CALL_ID": f"{str(meta.get('id', 'N/A'))[:5]}_{meta.get('started', 'unknown').split('T')[0]}" if meta.get("started") else "N/A",
                "CALL_TITLE": meta.get("title", "N/A"),
                "CALL_DATE": datetime.fromisoformat(meta.get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d") if meta.get("started") else "N/A",
                "DURATION": format_duration(meta.get("duration", 0)),
                "MEETING_URL": meta.get("meetingUrl", "N/A"),
                "WEBSITE": next((field.get("value", "N/A") for obj in account_context.get("objects", []) for field in obj.get("fields", []) if field.get("name") == "Website"), "N/A"),
                "ACCOUNT_ID": call.get("account_id", "N/A"),
                "ACCOUNT_NORMALIZED": call.get("account_normalized", "N/A"),
                "INDUSTRY_NORMALIZED": call.get("industry_normalized", "Unknown"),
                "OPPORTUNITY_NAME": next((field.get("value", "N/A") for field in opportunity.get("fields", []) if field.get("name") == "Name"), "N/A"),
                "LEAD_SOURCE": next((field.get("value", "N/A") for field in opportunity.get("fields", []) if field.get("name") == "LeadSource"), "N/A"),
                "DEAL_STAGE": next((field.get("value", "N/A") for field in opportunity.get("fields", []) if field.get("name") == "StageName"), "N/A"),
                "FORECAST_CATEGORY": next((field.get("value", "N/A") for field in opportunity.get("fields", []) if field.get("name") == "ForecastCategoryName"), "N/A"),
                "EXTERNAL_PARTICIPANTS": ", ".join(external_participants) or "N/A",
                "INTERNAL_PARTICIPANTS": ", ".join(internal_participants) or "N/A",
                "INTERNAL_SPEAKERS": len(set(u.get("speakerId", "") for u in call.get("utterances", []) if u.get("speakerId") in [p.get("speakerId", "") for p in parties if p.get("affiliation") in st.session_state.config["excluded_affiliations"]])),
                "EXTERNAL_SPEAKERS": len(set(u.get("speakerId", "") for u in call.get("utterances", []) if u.get("speakerId") in [p.get("speakerId", "") for p in parties if p.get("affiliation") == "External"])),
                "SALES_TRACKERS": " | ".join(sales_trackers) or "N/A",
                "PRICING_DURATION": format_duration(next((t.get("duration", 0) for t in topics if t.get("name") == "Pricing"), 0)),
                "NEXT_STEPS_DURATION": format_duration(next((t.get("duration", 0) for t in topics if t.get("name") == "Next Steps"), 0)),
                "CALL_BRIEF": call.get("content", {}).get("brief", "N/A"),
                "KEY_POINTS": ";".join(p.get("text", "N/A") for p in call.get("content", {}).get("keyPoints", [])) or "N/A"
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
            short_call_id = f"{str(call_id)[:5]}_{call.get('metaData', {}).get('started', 'unknown').split('T')[0]}" if call.get('metaData', {}).get('started') else "N/A"
            call_title = call.get("metaData", {}).get("title", "N/A")
            call_date = datetime.fromisoformat(call.get("metaData", {}).get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d") if call.get("metaData", {}).get("started") else "N/A"
            account_id = call.get("account_id", "N/A")
            normalized_account = call.get("account_normalized", "N/A")
            normalized_industry = call.get("industry_normalized", "Unknown")
            parties = call.get("parties", [])
            speaker_info = {p.get("speakerId", ""): {"name": p.get("name", "N/A"), "title": p.get("title", "Unknown")} for p in parties}
            for utterance in call.get("utterances", []):
                sentences = utterance.get("sentences", [])
                if not sentences:
                    continue
                text = " ".join(s.get("text", "N/A") for s in sentences)
                word_count = len(text.split())
                topic = utterance.get("topic", "N/A")
                if topic in st.session_state.config["excluded_topics"] or word_count < st.session_state.config["min_word_count"]:
                    continue
                speaker_id = utterance.get("speakerId", "N/A")
                speaker = speaker_info.get(speaker_id, {"name": "N/A", "title": "Unknown"})
                start_time = sentences[0].get("start", 0)
                end_time = sentences[-1].get("end", 0)
                duration = format_duration(end_time - start_time) if end_time and start_time else "N/A"
                utterances_data.append({
                    "CALL_ID": call_id,
                    "SHORT_CALL_ID": short_call_id,
                    "CALL_TITLE": call_title,
                    "CALL_DATE": call_date,
                    "ACCOUNT_ID": account_id,
                    "ACCOUNT_NORMALIZED": normalized_account,
                    "INDUSTRY_NORMALIZED": normalized_industry,
                    "SPEAKER_JOB_TITLE": speaker["title"],
                    "UTTERANCE_DURATION": duration,
                    "UTTERANCE_TEXT": text,
                    "TOPIC": topic
                })
        except Exception as e:
            logger.error(f"Utterance prep error: {str(e)}")
    return pd.DataFrame(utterances_data)

def get_normalized_industries(categories: List[str], category_to_normalized: Dict[str, List[str]]) -> List[str]:
    return list(set(sum([category_to_normalized.get(cat, []) for cat in categories], [])))

def apply_filters(df: pd.DataFrame, industries: List[str], products: List[str], account_products: Dict[str, set]) -> pd.DataFrame:
    if not industries and not products:
        return df
    filtered_df = df.copy()
    if "INDUSTRY_NORMALIZED" not in filtered_df.columns:
        filtered_df["INDUSTRY_NORMALIZED"] = "Unknown"
    if "ACCOUNT_ID" not in filtered_df.columns:
        filtered_df["ACCOUNT_ID"] = "Unknown"
    try:
        if industries:
            industries_lower = [i.lower() for i in industries]
            filtered_df = filtered_df[
                filtered_df["INDUSTRY_NORMALIZED"].str.lower().isin(industries_lower) |
                filtered_df["INDUSTRY_NORMALIZED"].str.lower().isin(["unknown", "n/a", ""])
            ]
        if products:
            matching_account_ids = {aid for aid, prods in account_products.items() if any(p in products for p in prods)}
            filtered_df = filtered_df[
                filtered_df["ACCOUNT_ID"].isin(matching_account_ids) |
                filtered_df["ACCOUNT_ID"].str.lower().isin(["unknown", "n/a", ""])
            ]
        return filtered_df
    except Exception as e:
        st.error(f"Filtering error: {str(e)}")
        logger.error(f"Filtering error: {str(e)}")
        return df

def download_csv(df: pd.DataFrame, filename: str):
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(f"Download {filename}", data=csv, file_name=filename, mime="text/csv")

def download_json(data: Any, filename: str):
    json_data = json.dumps(data, indent=4, ensure_ascii=False, default=str)
    st.download_button(f"Download {filename}", data=json_data, file_name=filename, mime="application/json")

# Main app
def main():
    st.title("📞 Gong Wizard")
    st.markdown(
        """
        <style>
        [data-testid="stMultiSelect"] { min-width: 300px; }
        [data-testid="stMultiSelect"] div[role="button"] { white-space: normal; }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Load CSVs
    industry_mapping, ui_industries, normalized_orgs, products_df = load_csv_data()
    if industry_mapping is None:
        return

    # Initialize mappings
    api_to_normalized = dict(zip(industry_mapping["Industry (API)"], industry_mapping["Industry (Normalized)"]))
    category_to_normalized = {category: ui_industries[ui_industries["Category"] == category]["Industry (CSVs)"].unique().tolist() 
                             for category in ui_industries["Category"].unique()}
    account_products = products_df.groupby("id")["product"].apply(set).to_dict()
    unique_products = sorted(products_df["product"].unique())

    # Sidebar
    with st.sidebar:
        st.header("Configuration")
        access_key = st.text_input("Gong Access Key", type="password")
        secret_key = st.text_input("Gong Secret Key", type="password")
        headers = create_auth_header(access_key, secret_key) if access_key and secret_key else {}
        
        # Date range selection with quick-select buttons
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

        industry_categories = list(category_to_normalized.keys())
        selected_categories = st.multiselect("Industry", ["Select All"] + industry_categories, default=["Select All"])
        selected_products = st.multiselect("Product", ["Select All"] + unique_products, default=["Select All"])
        if st.button("Reset", type="secondary"):
            st.session_state.clear()
            st.session_state.step = 1
            st.rerun()

    # Validate headers
    if not headers:
        st.warning("Please provide your Gong Access Key and Secret Key.")
        return

    # Normalize selections
    if "Select All" in selected_categories:
        selected_categories = industry_categories
    if "Select All" in selected_products:
        selected_products = unique_products

    # Step 1: Fetch call list
    if st.session_state.step == 1:
        st.markdown("### Step 1: Fetch Call List")
        if st.button("Fetch Call List", type="primary", key="fetch_call_list"):
            with st.spinner("Fetching call list..."):
                session = requests.Session()
                session.headers.update(headers)
                call_ids = fetch_call_list(session, st.session_state.start_date.isoformat() + "T00:00:00Z", st.session_state.end_date.isoformat() + "T23:59:59Z")
                if call_ids:
                    st.session_state.processed_data["call_ids"] = call_ids
                    st.session_state.processed_data["start_date_str"] = st.session_state.start_date.strftime("%d%b%y").lower()
                    st.session_state.processed_data["end_date_str"] = st.session_state.end_date.strftime("%Y-%m-%d")
                    st.session_state.step = 2  # Move to Step 2
                    st.success(f"Found {len(call_ids)} calls.")
                st.rerun()

    # Step 2: Fetch call details
    elif st.session_state.step == 2:
        st.markdown("### Step 2: Fetch Call Details")
        if "call_ids" not in st.session_state.processed_data:
            st.warning("Please fetch the call list first.")
            st.session_state.step = 1
            st.rerun()
        else:
            if st.button("Fetch Call Details", type="primary", key="fetch_call_details"):
                with st.spinner("Fetching call details..."):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    session = requests.Session()
                    session.headers.update(headers)
                    call_ids = st.session_state.processed_data["call_ids"]
                    full_data = []
                    total = len(call_ids)
                    processed = 0
                    batch_size = 50
                    for i in range(0, len(call_ids), batch_size):
                        batch = call_ids[i:i + batch_size]
                        details = fetch_call_details(session, batch)
                        transcripts = fetch_transcript(session, batch)
                        for call in details:
                            call_id = call.get("metaData", {}).get("id", "")
                            call_transcript = transcripts.get(call_id, [])
                            normalized_data = normalize_call_data(call, call_transcript, normalized_orgs, api_to_normalized)
                            if normalized_data:
                                full_data.append(normalized_data)
                        processed += len(batch)
                        progress = processed / total
                        progress_bar.progress(progress)
                        status_text.text(f"Processed {processed}/{total} calls")
                    progress_bar.empty()
                    status_text.empty()
                    if full_data:
                        st.session_state.processed_data["full_data"] = full_data
                        st.session_state.processed_data["summary_df"] = prepare_summary_df(full_data)
                        st.session_state.processed_data["utterances_df"] = prepare_utterances_df(full_data)
                        st.session_state.processed_data["json_data"] = json.dumps(full_data, indent=4, ensure_ascii=False, default=str)
                        st.session_state.step = 3  # Move to Step 3
                        st.success(f"Processed {len(full_data)} calls.")
                    else:
                        st.error("No call details fetched.")
                    st.rerun()
            if st.button("Back to Step 1"):
                st.session_state.step = 1
                st.rerun()

    # Step 3: Filter and analyze
    elif st.session_state.step == 3:
        st.markdown("### Step 3: Filter and Analyze")
        if "summary_df" not in st.session_state.processed_data:
            st.warning("Please fetch call details first.")
            st.session_state.step = 2
            st.rerun()
        else:
            summary_df = st.session_state.processed_data["summary_df"]
            utterances_df = st.session_state.processed_data["utterances_df"]
            full_data = st.session_state.processed_data["full_data"]
            normalized_industries = get_normalized_industries(selected_categories, category_to_normalized)
            filtered_df = apply_filters(summary_df, normalized_industries, selected_products, account_products)
            included_call_ids = set(filtered_df["CALL_ID"])
            excluded_df = summary_df[~summary_df["CALL_ID"].isin(included_call_ids)].copy()
            excluded_df["EXCLUSION_REASON"] = "Other"
            if selected_categories and "Select All" not in selected_categories:
                industry_mask = (~excluded_df["INDUSTRY_NORMALIZED"].str.lower().isin([i.lower() for i in normalized_industries]) & 
                                ~excluded_df["INDUSTRY_NORMALIZED"].str.lower().isin(["unknown", "n/a", ""]))
                excluded_df.loc[industry_mask, "EXCLUSION_REASON"] = "Industry"
            if selected_products and "Select All" not in selected_products:
                matching_account_ids = {aid for aid, prods in account_products.items() if any(p in products for p in prods)}
                product_mask = (~excluded_df["ACCOUNT_ID"].isin(matching_account_ids) & 
                               ~excluded_df["ACCOUNT_ID"].str.lower().isin(["unknown", "n/a", ""]))
                excluded_df.loc[product_mask & (excluded_df["EXCLUSION_REASON"] == "Industry"), "EXCLUSION_REASON"] = "Industry and Product"
                excluded_df.loc[product_mask & (excluded_df["EXCLUSION_REASON"] == "Other"), "EXCLUSION_REASON"] = "Product"
            st.subheader("Included Calls")
            st.dataframe(filtered_df)
            st.subheader("Excluded Calls")
            st.dataframe(excluded_df)
            st.subheader("Utterances")
            st.dataframe(utterances_df[utterances_df["CALL_ID"].isin(filtered_df["CALL_ID"])])
            start_date_str = st.session_state.processed_data["start_date_str"]
            end_date_str = st.session_state.processed_data["end_date_str"]
            st.subheader("Download Options")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Unfiltered Data**")
                download_csv(summary_df, f"unfiltered_summary_gong_{start_date_str}_to_{end_date_str}.csv")
                download_csv(utterances_df, f"unfiltered_utterances_gong_{start_date_str}_to_{end_date_str}.csv")
                download_json(full_data, f"unfiltered_json_gong_{start_date_str}_to_{end_date_str}.json")
            with col2:
                st.markdown("**Filtered Data**")
                download_csv(filtered_df, f"filtered_summary_gong_{start_date_str}_to_{end_date_str}.csv")
                filtered_utterances = utterances_df[utterances_df["CALL_ID"].isin(filtered_df["CALL_ID"])]
                download_csv(filtered_utterances, f"filtered_utterances_gong_{start_date_str}_to_{end_date_str}.csv")
                filtered_json = [call for call in full_data if call.get("metaData", {}).get("id", "N/A") in filtered_df["CALL_ID"].values]
                download_json(filtered_json, f"filtered_json_gong_{start_date_str}_to_{end_date_str}.json")
            if st.button("Back to Step 2"):
                st.session_state.step = 2
                st.rerun()

if __name__ == "__main__":
    main()