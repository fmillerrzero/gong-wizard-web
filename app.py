import streamlit as st
import pandas as pd
import requests
import base64
from datetime import datetime, timedelta
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

# Helper functions
def create_auth_header(access_key: str, secret_key: str) -> dict:
    credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
    return {"Authorization": f"Basic {credentials}"}

def fetch_call_list(session: requests.Session, from_date: str, to_date: str) -> list:
    url = f"{GONG_API_BASE}/v2/calls"
    params = {"fromDateTime": from_date, "toDateTime": to_date}
    call_ids = []
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
            else:
                st.error(f"Failed to fetch calls: {response.status_code} - {response.text}")
                break
    except Exception as e:
        st.error(f"Call list error: {str(e)}")
        logger.error(f"Call list error: {str(e)}")
    return call_ids

def fetch_call_details(session: requests.Session, call_ids: list) -> list:
    url = f"{GONG_API_BASE}/v2/calls/extensive"
    request_body = {
        "filter": {"callIds": call_ids},
        "contentSelector": {
            "context": "Extended",
            "exposedFields": {
                "parties": True,
                "content": {"trackers": True},
                "interaction": {"speakers": True},
                "media": True
            }
        }
    }
    try:
        response = session.post(url, json=request_body, timeout=60)
        if response.status_code == 200:
            return response.json().get("calls", [])
        else:
            st.error(f"Call details fetch failed: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        st.error(f"Error fetching call details: {str(e)}")
        logger.error(f"Error fetching call details: {str(e)}")
        return []

def normalize_call_data(call_data: dict) -> dict:
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
        return call_data
    except Exception as e:
        logger.error(f"Normalization error: {str(e)}")
        return call_data

def format_duration(seconds):
    try:
        seconds = int(seconds)
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes} min {remaining_seconds} sec"
    except (ValueError, TypeError):
        return "N/A"

def get_speaker_talk_time(call: dict) -> dict:
    talk_times = {}
    try:
        speakers = call.get("interaction", {}).get("speakers", [])
        total_duration = call.get("metaData", {}).get("duration", 0)
        
        if total_duration <= 0:
            return talk_times
        
        # Calculate talk time percentages
        for speaker in speakers:
            speaker_id = speaker.get("id")
            talk_time = speaker.get("talkTime", 0)
            if speaker_id and talk_time > 0:
                talk_times[speaker_id] = (talk_time / total_duration) * 100
        
        return talk_times
    except Exception as e:
        logger.error(f"Error calculating talk time: {str(e)}")
        return {}

def prepare_summary_df(calls: list) -> pd.DataFrame:
    summary_data = []
    for call in calls:
        if not call:
            continue
        try:
            meta = call.get("metaData", {})
            parties = call.get("parties", [])
            
            # Calculate talk time percentages
            talk_times = get_speaker_talk_time(call)
            
            # Create speaker info
            internal_speakers = []
            external_speakers = []
            for party in parties:
                name = party.get("name", "").strip()
                if not name:
                    continue
                title = party.get("title", "").strip()
                affiliation = party.get("affiliation", "Unknown")
                speaker_id = party.get("speakerId")
                talk_pct = f", {round(talk_times.get(speaker_id, 0))}%" if speaker_id in talk_times else ""
                
                speaker_str = f"{name}" + (f", {title}" if title else "") + talk_pct
                if affiliation == "Internal":
                    internal_speakers.append(speaker_str)
                else:
                    external_speakers.append(speaker_str)
            
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
            
            summary_data.append({
                "call_id": f'"{meta.get("id", "N/A")}"',
                "call_title": meta.get("title", "N/A"),
                "call_date": datetime.fromisoformat(meta.get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d") if meta.get("started") else "N/A",
                "duration": format_duration(meta.get("duration", 0)),
                "meeting_url": meta.get("meetingUrl", "N/A"),
                "account_id": call.get("account_id", "N/A"),
                "account_name": call.get("account_name", "N/A"),
                "account_website": call.get("account_website", "N/A"),
                "trackers": tracker_str,
                "internal_speakers": "|".join(internal_speakers) if internal_speakers else "None",
                "external_speakers": "|".join(external_speakers) if external_speakers else "None"
            })
        except Exception as e:
            logger.error(f"Summary prep error: {str(e)}")
    return pd.DataFrame(summary_data)

def download_csv(df: pd.DataFrame, filename: str, label: str):
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(label, data=csv, file_name=filename, mime="text/csv")

# Main app
def main():
    st.title("📞 Gong Wizard")

    # Sidebar configuration
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

    # Validate headers
    if not headers:
        st.warning("Please provide your Gong Access Key and Secret Key.")
        return

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
            for call in details:
                normalized_data = normalize_call_data(call)
                if normalized_data:
                    full_data.append(normalized_data)

        if not full_data:
            st.error("No call details fetched.")
            return

        summary_df = prepare_summary_df(full_data)

    st.subheader("Filtered Calls")
    st.dataframe(summary_df)

    start_date_str = st.session_state.start_date.strftime("%d%b%y").lower()
    end_date_str = st.session_state.end_date.strftime("%Y-%m-%d")

    st.subheader("Download Options")
    col1, _ = st.columns(2)
    with col1:
        st.markdown("**Unfiltered Data**")
        download_csv(summary_df, f"summary_gong_{start_date_str}_to_{end_date_str}.csv", "Download Calls CSV")

if __name__ == "__main__":
    main()