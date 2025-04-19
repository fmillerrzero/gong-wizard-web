import streamlit as st
import pandas as pd
import requests
import base64
import json
from datetime import datetime, timedelta, date
import logging
from typing import Dict, List, Optional, Tuple, Any
import pytz
import re

# Configure logging - simple setup to avoid overhead
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
GONG_API_BASE = "https://us-11211.api.gong.io/v2"
SF_TZ = pytz.timezone('America/Los_Angeles')

# Product mappings - simplified
PRODUCT_MAPPINGS = {
    "IAQ Monitoring": ["Air Quality"],
    "ODCV": ["ODCV"],
    "Secure Air": ["Filter", "Filtration"],
    "Occupancy Analytics": ["capacity", "dashboard", "occupancy", "utilization", "heat map"]
}
ALL_PRODUCT_TAGS = list(PRODUCT_MAPPINGS.keys())

# Custom exception for API errors
class GongAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Gong API Error {status_code}: {message}")

def create_auth_header(access_key: str, secret_key: str) -> Dict[str, str]:
    """Create Basic Auth header for Gong API."""
    if not access_key or not secret_key:
        raise ValueError("Access key and secret key must not be empty")
    credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
    return {"Authorization": f"Basic {credentials}"}

def convert_to_sf_time(utc_time: str) -> str:
    """Convert UTC timestamp to SF time in MM/DD/YY format."""
    try:
        utc_dt = datetime.fromisoformat(utc_time.replace("Z", "+00:00"))
        sf_dt = utc_dt.astimezone(SF_TZ)
        return sf_dt.strftime("%m/%d/%y")
    except Exception as e:
        logger.warning(f"Invalid timestamp {utc_time}")
        return "N/A"

def fetch_call_list(session: requests.Session, from_date: str, to_date: str) -> List[str]:
    """Fetch list of call IDs from Gong API."""
    url = f"{GONG_API_BASE}/calls"
    params = {"fromDateTime": from_date, "toDateTime": to_date}
    call_ids = []
    
    try:
        response = session.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            call_ids.extend(str(call["id"]) for call in data.get("calls", []))
            # Only process first page to keep things fast
        else:
            st.error(f"API error: {response.status_code}")
    except Exception as e:
        st.error(f"Network error: {str(e)}")
    
    return call_ids[:20]  # Limit to 20 calls for faster processing

def fetch_call_details(session: requests.Session, call_ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch detailed information for call IDs."""
    url = f"{GONG_API_BASE}/calls/extensive"
    call_details = []
    batch_size = 10  # Smaller batch size
    
    for i in range(0, len(call_ids), batch_size):
        batch_ids = call_ids[i:i + batch_size]
        request_body = {
            "filter": {"callIds": batch_ids},
            "contentSelector": {
                "context": "Extended",
                "exposedFields": {
                    "parties": True,
                    "content": {"trackers": True, "brief": True},
                    "media": True,
                    "crmAssociations": True
                }
            }
        }
        
        try:
            response = session.post(url, json=request_body, timeout=30)
            if response.status_code == 200:
                data = response.json()
                call_details.extend(data.get("calls", []))
            else:
                st.error(f"API error: {response.status_code}")
        except Exception as e:
            st.error(f"Network error: {str(e)}")
    
    return call_details

def apply_occupancy_analytics_tags(call: Dict[str, Any]) -> bool:
    """Apply Occupancy Analytics tags based on simplified keyword matching."""
    fields = [
        call.get("metaData", {}).get("title", ""),
        call.get("content", {}).get("brief", ""),
    ]
    
    for field in fields:
        if field is None:
            continue
        text_lower = field.lower()
        for keyword in PRODUCT_MAPPINGS["Occupancy Analytics"]:
            if keyword in text_lower:
                return True
    return False

def normalize_call_data(call_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize call data with product tagging."""
    if not call_data or "metaData" not in call_data:
        return None

    processed_data = {
        "metaData": call_data.get("metaData", {}),
        "context": call_data.get("context", []),
        "content": call_data.get("content", {"trackers": [], "brief": ""}),
        "parties": call_data.get("parties", []),
        "products": [],
        "other_topics": [],
        "account_industry": "",
        "account_name": "Unknown",
        "account_id": "Unknown",
        "account_website": "Unknown"
    }

    # Extract account information
    account_context = next((ctx for ctx in processed_data["context"] if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
    for obj in account_context.get("objects", []):
        if obj.get("objectType") == "Account":
            processed_data["account_id"] = str(obj.get("objectId", "Unknown"))
            for field in obj.get("fields", []):
                if field.get("name") == "Name":
                    processed_data["account_name"] = field.get("value", "Unknown")
                if field.get("name") == "Website":
                    processed_data["account_website"] = field.get("value", "Unknown")
                if field.get("name") == "Industry":
                    processed_data["account_industry"] = field.get("value", "")

    # Apply product tags
    trackers = processed_data.get("content", {}).get("trackers", [])
    tracker_counts = {t.get("name", ""): t.get("count", 0) for t in trackers if isinstance(t, dict)}
    for product, trackers_or_patterns in PRODUCT_MAPPINGS.items():
        if product == "Occupancy Analytics":
            if apply_occupancy_analytics_tags(call_data):
                processed_data["products"].append(product)
        else:
            for tracker in trackers_or_patterns:
                if tracker in tracker_counts and tracker_counts[tracker] > 0:
                    processed_data["products"].append(product)
                    break

    return processed_data

def format_speaker(speaker: Dict[str, Any]) -> str:
    """Format speaker as 'Name, Title'."""
    name = speaker.get("name", "").strip()
    title = speaker.get("jobTitle", "").strip()
    if name and title:
        return f"{name}, {title}"
    return name or title or ""

def get_primary_speakers(call: Dict[str, Any]) -> Tuple[str, str, str]:
    """Determine primary speakers by utterance count."""
    internal_speaker = external_speaker = unknown_speaker = ""
    
    for party in call.get("parties", []):
        affiliation = party.get("affiliation", "unknown").lower()
        if affiliation == "internal":
            internal_speaker = format_speaker(party)
            break
    
    for party in call.get("parties", []):
        affiliation = party.get("affiliation", "unknown").lower()
        if affiliation == "external":
            external_speaker = format_speaker(party)
            break

    return internal_speaker, external_speaker, unknown_speaker

def prepare_call_summary_df(calls: List[Dict[str, Any]], selected_products: List[str]) -> pd.DataFrame:
    """Prepare call summary dataframe."""
    data = []
    for call in calls:
        if not call or "metaData" not in call:
            continue

        call_id = f'"{str(call["metaData"].get("id", ""))}"'
        call_date = convert_to_sf_time(call["metaData"].get("started", ""))
        products = sorted(set(p for p in call.get("products", []) if p in ALL_PRODUCT_TAGS))
        products_str = "|".join(products) if products else "none"
        other_topics = sorted(call.get("other_topics", []), key=lambda x: x["count"], reverse=True)
        other_topics_str = "|".join(t["name"] for t in other_topics) if other_topics else "none"
        internal_speaker, external_speaker, unknown_speaker = get_primary_speakers(call)

        filtered_out = "yes" if products and not any(p in selected_products for p in products) and "Select All" not in selected_products else "no"

        data.append({
            "call_id": call_id,
            "call_date": call_date,
            "filtered_out": filtered_out,
            "product_tags": products_str,
            "other_topics": other_topics_str,
            "primary_internal_speaker": internal_speaker,
            "primary_external_speaker": external_speaker,
            "account_name": call.get("account_name", "N/A"),
            "account_website": call.get("account_website", "N/A")
        })

    df = pd.DataFrame(data) if data else pd.DataFrame()
    if not df.empty:
        df = df.sort_values("call_date", ascending=False)
    return df

def main():
    st.set_page_config(page_title="Gong Wizard", layout="wide")

    # Safe Session State Initialization
    defaults = {
        "start_date": date.today() - timedelta(days=7),
        "end_date": date.today(),
        "selected_products": [],
        "step": "config",  # New step-based navigation
        "calls_data": None
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    st.title("📞 Gong Data Processor")

    if st.session_state.step == "config":
        with st.form("config_form"):
            st.subheader("API Configuration")
            access_key = st.text_input("Gong Access Key", type="password")
            secret_key = st.text_input("Gong Secret Key", type="password")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                time_range = st.radio("Time Range", ["Last 7 Days", "Last 30 Days", "Custom"])
            
            if time_range == "Last 7 Days":
                start_date = date.today() - timedelta(days=7)
                end_date = date.today()
            elif time_range == "Last 30 Days":
                start_date = date.today() - timedelta(days=30)
                end_date = date.today()
            else:
                with col2:
                    start_date = st.date_input("From Date", value=st.session_state.start_date)
                with col3:
                    end_date = st.date_input("To Date", value=st.session_state.end_date)
            
            st.session_state.start_date = start_date
            st.session_state.end_date = end_date
            
            select_all = st.checkbox("Select All Products", value=True)
            if select_all:
                st.session_state.selected_products = ["Select All"]
                st.multiselect("Products", ["Select All"] + ALL_PRODUCT_TAGS, default=["Select All"], disabled=True)
            else:
                st.info("Calls with no product tags are included by default.")
                st.session_state.selected_products = st.multiselect("Products", ALL_PRODUCT_TAGS, default=[])
            
            submit = st.form_submit_button("Submit")
            
            if submit:
                if not access_key or not secret_key:
                    st.error("Please provide both Gong Access Key and Secret Key.")
                    return
                
                if st.session_state.start_date > st.session_state.end_date:
                    st.error("Start date cannot be after end date.")
                    return
                
                st.session_state.step = "processing"
                st.rerun()
    
    elif st.session_state.step == "processing":
        # Show processing UI
        st.info("Processing calls... This may take a few moments.")
        
        try:
            session = requests.Session()
            session.headers.update(create_auth_header(st.session_state.get('access_key', ''), 
                                                     st.session_state.get('secret_key', '')))
            
            call_ids = fetch_call_list(
                session,
                st.session_state.start_date.isoformat() + "T00:00:00Z",
                st.session_state.end_date.isoformat() + "T23:59:59Z"
            )
            
            if not call_ids:
                st.warning("No calls found for the selected date range.")
                st.session_state.step = "config"
                return
            
            progress_bar = st.progress(0)
            st.write(f"Found {len(call_ids)} calls. Processing details...")
            
            details = fetch_call_details(session, call_ids)
            
            full_data = []
            for i, call in enumerate(details):
                normalized_data = normalize_call_data(call)
                if normalized_data:
                    full_data.append(normalized_data)
                
                progress_bar.progress(min((i + 1) / len(details), 1.0))
            
            progress_bar.empty()
            
            if not full_data:
                st.error("No valid call data retrieved.")
                st.session_state.step = "config"
                return
            
            st.session_state.calls_data = full_data
            st.session_state.step = "results"
            st.rerun()
            
        except Exception as e:
            st.error(f"Error during processing: {str(e)}")
            st.session_state.step = "config"
    
    elif st.session_state.step == "results":
        # Show results UI
        if not st.session_state.calls_data:
            st.error("No data available. Please try again.")
            st.session_state.step = "config"
            st.rerun()
            
        st.subheader("Results")
        st.write(f"Total calls processed: {len(st.session_state.calls_data)}")
        
        call_summary_df = prepare_call_summary_df(st.session_state.calls_data, st.session_state.selected_products)
        
        if not call_summary_df.empty:
            st.write("Call Summary:")
            st.dataframe(call_summary_df)
            
            csv = call_summary_df.to_csv(index=False, encoding='utf-8-sig')
            start_date_str = st.session_state.start_date.strftime("%d%b%y").lower()
            end_date_str = st.session_state.end_date.strftime("%d%b%y").lower()
            
            st.download_button(
                "Download Call Summary CSV",
                data=csv,
                file_name=f"call_summary_gong_{start_date_str}_to_{end_date_str}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No calls match your filter criteria.")
        
        if st.button("Start Over"):
            st.session_state.step = "config"
            st.rerun()

if __name__ == "__main__":
    main()