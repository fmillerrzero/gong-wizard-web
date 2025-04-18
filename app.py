import streamlit as st
import pandas as pd
import requests
import base64
import json
import time
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GONG_API_BASE = "https://us-11211.api.gong.io/v2"
PRODUCT_TAG_TRACKERS = {"ODCV": "ODCV", "Filter": "Filter", "air quality": "air quality", "Connect": "Connect"}
ALL_PRODUCT_TAGS = ["ODCV", "Filter", "air quality", "Connect"]

def create_auth_header(access_key: str, secret_key: str) -> dict:
    credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
    return {"Authorization": f"Basic {credentials}"}

def fetch_call_list(session: requests.Session, from_date: str, to_date: str) -> list:
    url = f"{GONG_API_BASE}/calls"
    params = {"fromDateTime": from_date, "toDateTime": to_date}
    call_ids = []
    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            page_params = dict(params)
            while True:
                response = session.get(url, params=page_params, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    call_ids.extend(call["id"] for call in data.get("calls", []))
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor:
                        break
                    page_params["cursor"] = cursor
                    time.sleep(1)
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                    time.sleep(wait_time)
                    continue
                else:
                    st.error(f"Call list error {response.status_code}: {response.text}")
                    return call_ids
            break
        except Exception as e:
            if attempt == max_attempts - 1:
                st.error(f"Call list fetch failed: {str(e)}")
    return call_ids

def fetch_call_details(session: requests.Session, call_ids: list) -> list:
    url = f"{GONG_API_BASE}/calls/extensive"
    call_details = []
    cursor = None
    max_attempts = 2
    while True:
        request_body = {
            "filter": {"callIds": call_ids},
            "contentSelector": {"context": "Extended", "exposedFields": {"parties": True, "content": {"trackers": True, "brief": True, "keyPoints": True}}}
        }
        if cursor:
            request_body["cursor"] = cursor
        for attempt in range(max_attempts):
            try:
                response = session.post(url, json=request_body, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    call_details.extend(data.get("calls", []))
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor:
                        break
                    time.sleep(1)
                    break
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                    time.sleep(wait_time)
                    continue
                else:
                    st.error(f"Call details error {response.status_code}: {response.text}")
                    return call_details
            except Exception as e:
                if attempt == max_attempts - 1:
                    st.error(f"Details fetch failed: {str(e)}")
        if not cursor:
            break
    return call_details

def fetch_transcript(session: requests.Session, call_ids: list) -> dict:
    url = f"{GONG_API_BASE}/calls/transcript"
    result = {}
    cursor = None
    max_attempts = 2
    while True:
        request_body = {"filter": {"callIds": call_ids}}
        if cursor:
            request_body["cursor"] = cursor
        for attempt in range(max_attempts):
            try:
                response = session.post(url, json=request_body, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    for t in data.get("callTranscripts", []):
                        if t.get("callId"):
                            result[t["callId"]] = t.get("transcript", [])
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor:
                        break
                    time.sleep(1)
                    break
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                    time.sleep(wait_time)
                    continue
                else:
                    st.error(f"Transcript error {response.status_code}: {response.text}")
                    return {call_id: [] for call_id in call_ids}
            except Exception as e:
                if attempt == max_attempts - 1:
                    st.error(f"Transcript fetch failed: {str(e)}")
        if not cursor:
            break
    return result

def normalize_call_data(call_data: dict, transcript: list) -> dict:
    call_data = {
        "metaData": call_data.get("metaData", {}),
        "context": call_data.get("context", []),
        "content": call_data.get("content", {"trackers": [], "brief": "", "keyPoints": []}),
        "parties": call_data.get("parties", []),
        "utterances": transcript or [],
        "products": [],
        "tracker_matches": [],
        "partial_data": False
    }
    try:
        account_context = next((ctx for ctx in call_data["context"] if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
        account_name = "Unknown"
        account_id = "Unknown"
        account_website = "Unknown"
        for obj in account_context.get("objects", []):
            if obj.get("objectType") == "Account":
                account_id = obj.get("objectId", "Unknown")
                for field in obj.get("fields", []):
                    if field.get("name") == "Name":
                        account_name = field.get("value", "Unknown")
                    if field.get("name") == "Website":
                        account_website = field.get("value", "Unknown")
        call_data["account_name"] = account_name
        call_data["account_id"] = account_id
        call_data["account_website"] = account_website
        
        for tracker in call_data["content"].get("trackers", []):
            tracker_name = tracker.get("name", "")
            count = tracker.get("count", 0)
            if count > 0 and tracker_name in PRODUCT_TAG_TRACKERS:
                call_data["products"].append(PRODUCT_TAG_TRACKERS[tracker_name])
        call_data["products"] = list(set(call_data["products"]))
        return call_data
    except Exception as e:
        st.error(f"Data normalization error: {str(e)}")
        call_data["partial_data"] = True
        return call_data

def format_duration(seconds):
    try:
        seconds = int(seconds)
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes} min {remaining_seconds} sec"
    except (ValueError, TypeError):
        return "N/A"

def prepare_call_tables(calls: list, selected_products: list) -> tuple:
    included_data = []
    excluded_data = []
    for call in calls:
        if not call or "metaData" not in call:
            continue
        call_id = call["metaData"].get("id", "")
        call_title = call["metaData"].get("title", "N/A")
        call_date = datetime.fromisoformat(call["metaData"].get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d")
        account_name = call.get("account_name", "N/A")
        products = call.get("products", [])
        products_str = "|".join(products) if products else "None"
        brief = call["content"].get("brief", "N/A")
        key_points = "; ".join(call["content"].get("keyPoints", [])) or "N/A"
        
        if not selected_products or "Select All" in selected_products:
            included_data.append({"call_id": call_id, "call_title": call_title, "call_date": call_date, "account_name": account_name, "products": products_str, "brief": brief, "keyPoints": key_points, "reason": "No product filter applied"})
        else:
            matched_products = [p for p in products if p in selected_products]
            if matched_products or not products:
                included_data.append({"call_id": call_id, "call_title": call_title, "call_date": call_date, "account_name": account_name, "products": products_str, "brief": brief, "keyPoints": key_points, "reason": f"Matched products: {('|'.join(matched_products) or 'None')}"})
            else:
                excluded_data.append({"call_id": call_id, "call_title": call_title, "call_date": call_date, "account_name": account_name, "products": products_str, "brief": brief, "keyPoints": key_points, "reason": "No matching products"})
    
    return pd.DataFrame(included_data), pd.DataFrame(excluded_data)

def prepare_utterances_df(calls: list) -> pd.DataFrame:
    utterances_data = []
    for call in calls:
        if not call or "metaData" not in call:
            continue
        call_id = call["metaData"].get("id", "")
        call_title = call["metaData"].get("title", "N/A")
        call_date = datetime.fromisoformat(call["metaData"].get("started", "1970-01-01T00:00:00Z").replace("Z", "+00:00")).strftime("%Y-%m-%d")
        account_id = call.get("account_id", "N/A")
        account_name = call.get("account_name", "N/A")
        account_website = call.get("account_website", "N/A")
        products = call.get("products", [])
        products_str = "|".join(products) if products else "None"
        parties = call.get("parties", [])
        partial_data = call.get("partial_data", False)
        
        speaker_info = {party.get("speakerId", ""): {"name": party.get("name", "Unknown"), "title": party.get("title", ""), "affiliation": party.get("affiliation", "Unknown")} for party in parties}
        
        for utterance in call.get("utterances", []):
            sentences = utterance.get("sentences", [])
            if not sentences:
                continue
            text = " ".join(s.get("text", "N/A") for s in sentences)
            word_count = len(text.split())
            topic = utterance.get("topic", "N/A")
            speaker_id = utterance.get("speakerId", "")
            speaker = speaker_info.get(speaker_id, {"name": "Unknown", "title": "", "affiliation": "External"})
            quality = "high"
            if partial_data:
                quality = "partial_data"
            elif speaker["affiliation"] == "Unknown":
                quality = "unknown_speaker"
            elif speaker["affiliation"] == "Internal":
                quality = "internal"
            elif topic in ["Call Setup", "Small Talk", "Wrap-up"]:
                quality = "low_quality_topic"
            elif word_count < 8 and speaker["affiliation"] == "External":
                quality = "short"
            start_time = sentences[0].get("start", 0)
            end_time = sentences[-1].get("end", 0)
            duration = format_duration(end_time - start_time) if end_time and start_time else "N/A"
            utterances_data.append({
                "call_id": call_id, "call_title": call_title, "call_date": call_date, "account_id": account_id, "account_name": account_name,
                "account_website": account_website, "products": products_str, "speaker_name": speaker["name"], "speaker_job_title": speaker["title"] or "",
                "speaker_affiliation": speaker["affiliation"], "utterance_duration": duration, "utterance_text": text, "topic": topic, "quality": quality
            })
    return pd.DataFrame(utterances_data)

def download_csv(df: pd.DataFrame, filename: str, label: str):
    if not df.empty:
        csv = df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(label, data=csv, file_name=filename, mime="text/csv")

def download_json(data: any, filename: str, label: str):
    if data:
        json_data = json.dumps(data, indent=4, ensure_ascii=False, default=str)
        st.download_button(label, data=json_data, file_name=filename, mime="application/json")

def main():
    st.title("📞 Gong Wizard")
    with st.sidebar:
        st.header("Configuration")
        access_key = st.text_input("Gong Access Key", type="password")
        secret_key = st.text_input("Gong Secret Key", type="password")
        today = datetime.today().date()
        start_date = st.date_input("From Date", value=today - timedelta(days=7))
        end_date = st.date_input("To Date", value=today)
        select_all = st.checkbox("Select All Products", value=True)
        selected_products = ALL_PRODUCT_TAGS if select_all else st.multiselect("Product", ALL_PRODUCT_TAGS, default=[])
        submit = st.button("Submit")
    
    if submit:
        if not access_key or not secret_key:
            st.error("Need both API keys, dude.")
            return
        if start_date > end_date:
            st.error("Start date can’t be after end date.")
            return
        with st.spinner("Grabbing calls..."):
            session = requests.Session()
            session.headers.update(create_auth_header(access_key, secret_key))
            call_ids = fetch_call_list(session, start_date.isoformat() + "T00:00:00Z", end_date.isoformat() + "T23:59:59Z")
            if not call_ids:
                st.error("No calls found. Check your dates or keys.")
                return
            full_data = []
            batch_size = 50
            for i in range(0, len(call_ids), batch_size):
                batch = call_ids[i:i + batch_size]
                details = fetch_call_details(session, batch)
                transcripts = fetch_transcript(session, batch)
                for call in details:
                    call_id = call.get("metaData", {}).get("id", "")
                    normalized_data = normalize_call_data(call, transcripts.get(call_id, []))
                    if normalized_data:
                        full_data.append(normalized_data)
            if not full_data:
                st.error("No call details pulled. Something’s off.")
                return
            utterances_df = prepare_utterances_df(full_data)
            included_calls_df, excluded_calls_df = prepare_call_tables(full_data, selected_products)
            st.subheader("Included Calls")
            st.dataframe(included_calls_df)
            st.subheader("Excluded Calls")
            st.dataframe(excluded_calls_df)
            st.subheader("Utterances")
            st.dataframe(utterances_df)
            start_date_str = start_date.strftime("%d%b%y").lower()
            end_date_str = end_date.strftime("%d%b%y").lower()
            download_csv(included_calls_df, f"included_calls_{start_date_str}_to_{end_date_str}.csv", "Download Included Calls CSV")
            download_csv(excluded_calls_df, f"excluded_calls_{start_date_str}_to_{end_date_str}.csv", "Download Excluded Calls CSV")
            download_csv(utterances_df, f"utterances_{start_date_str}_to_{end_date_str}.csv", "Download Utterances CSV")
            download_json(full_data, f"full_data_{start_date_str}_to_{end_date_str}.json", "Download Full Data JSON")

if __name__ == "__main__":
    main()