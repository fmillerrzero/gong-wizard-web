import streamlit as st
import pandas as pd
import requests
import base64
import json
import time
import re
from urllib.parse import urlparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Tuple
import logging
from difflib import SequenceMatcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gong API base URL
GONG_API_BASE = "https://us-11211.api.gong.io/v2"

# Tracker names to product tags mapping
PRODUCT_TAG_TRACKERS = {
    "ODCV": "ODCV",
    "Filter": "Filter",
    "air quality": "air quality",
    "Connect": "Connect"
}

# Tracker renaming dictionary (for display in UI/CSV)
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

# All possible product tags for UI filtering
ALL_PRODUCT_TAGS = [
    "ODCV",
    "Filter",
    "air quality",
    "Connect",
    "Occupancy Analytics (Tenant)",
    "Owner Offering"
]

# Fuzzy matching threshold
FUZZY_MATCH_THRESHOLD = 85

# Load domain lists for product tagging
@st.cache_data
def load_domain_lists():
    """Load domain lists from CSVs for Occupancy Analytics and Owner Offering."""
    try:
        occupancy_df = pd.read_csv("Occupancy Analytics Tenant Customers Gong Bot Sheet3.csv", header=None, names=["domain"])
        owner_df = pd.read_csv("Owner Orgs Gong Bot Sheet3.csv", header=None, names=["domain"])
        occupancy_domains = set(occupancy_df["domain"].str.lower().dropna().tolist())
        owner_domains = set(owner_df["domain"].str.lower().dropna().tolist())
        logger.info(f"Loaded {len(occupancy_domains)} Occupancy Analytics domains and {len(owner_domains)} Owner Offering domains")
        return {
            "occupancy_analytics": occupancy_domains,
            "owner_offering": owner_domains
        }
    except Exception as e:
        st.error(f"Error loading domain lists: {str(e)}")
        logger.error(f"Domain list load error: {str(e)}")
        return {
            "occupancy_analytics": set(),
            "owner_offering": set()
        }

# Extract domain from URL
def extract_domain(url: str) -> str:
    """Extract and normalize domain from a URL."""
    if not url or url in ["Unknown", "N/A"]:
        return ""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        domain = re.sub(r'^www\.', '', domain)
        return domain.lower()
    except Exception as e:
        logger.warning(f"Error extracting domain from {url}: {str(e)}")
        return ""

# Fuzzy domain matching
def fuzzy_match_domain(domain: str, domain_list: Set[str]) -> Tuple[bool, str]:
    """Check if domain matches any in domain_list using fuzzy matching."""
    if not domain:
        return False, ""
    if domain in domain_list:
        return True, domain
    best_match = None
    best_ratio = 0
    for list_domain in domain_list:
        ratio = SequenceMatcher(None, domain, list_domain).ratio() * 100
        if ratio > best_ratio and ratio >= FUZZY_MATCH_THRESHOLD:
            best_ratio = ratio
            best_match = list_domain
    if best_match:
        logger.info(f"Fuzzy matched domain {domain} to {best_match} with ratio {best_ratio:.2f}")
        return True, best_match
    return False, ""

# Helper functions for Gong API
def create_auth_header(access_key: str, secret_key: str) -> Dict[str, str]:
    """Create Basic Auth header for Gong API."""
    credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
    return {"Authorization": f"Basic {credentials}"}

def fetch_call_list(session: requests.Session, from_date: str, to_date: str, max_attempts: int = 3) -> List[str]:
    """Fetch call IDs from Gong API within date range."""
    url = f"{GONG_API_BASE}/calls"
    params = {"fromDateTime": from_date, "toDateTime": to_date}
    call_ids = []
    for attempt in range(max_attempts):
        try:
            page_params = dict(params)
            while True:
                response = session.get(url, params=page_params, timeout=30)
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
                    st.error(f"Failed to fetch calls: {response.status_code} - {response.text}")
                    logger.error(f"Call list fetch failed: {response.status_code} - {response.text}")
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
    """Fetch detailed call info from Gong API."""
    url = f"{GONG_API_BASE}/calls/extensive"
    call_details = []
    cursor = None
    while True:
        request_body = {
            "filter": {"callIds": call_ids},
            "contentSelector": {
                "context": "Extended",
                "exposedFields": {
                    "parties": True,
                    "content": {"structure": True, "topics": True, "trackers": True, "trackerOccurrences": True, "brief": True, "keyPoints": True, "callOutcome": True},
                    "interaction": {"speakers": True, "personInteractionStats": True, "questions": True, "video": True},
                    "collaboration": {"publicComments": True},
                    "media": True
                }
            }
        }
        if cursor:
            request_body["cursor"] = cursor
        for attempt in range(max_attempts):
            try:
                response = session.post(url, json=request_body, timeout=60)
                if response.status_code == 200:
                    data = response.json()
                    call_details.extend(data.get("calls", []))
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor:
                        return call_details
                    break
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"Call details fetch failed: {response.status_code} - {response.text}")
                    return call_details
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep((2 ** attempt) * 1)
                else:
                    logger.warning(f"Error fetching call details: {str(e)}")
                    return call_details
        if not cursor:
            break
    return call_details

def fetch_transcript(session: requests.Session, call_ids: List[str], max_attempts: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch call transcripts from Gong API."""
    url = f"{GONG_API_BASE}/calls/transcript"
    result = {}
    cursor = None
    while True:
        request_body = {"filter": {"callIds": call_ids}}
        if cursor:
            request_body["cursor"] = cursor
        for attempt in range(max_attempts):
            try:
                response = session.post(url, json=request_body, timeout=60)
                if response.status_code == 200:
                    data = response.json()
                    transcripts = data.get("callTranscripts", [])
                    for t in transcripts:
                        if t.get("callId"):
                            result[t["callId"]] = t.get("transcript", [])
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor:
                        return result
                    break
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"Transcript fetch failed: {response.status_code} - {response.text}")
                    return {call_id: [] for call_id in call_ids}
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep((2 ** attempt) * 1)
                else:
                    logger.warning(f"Error fetching transcripts: {str(e)}")
                    return {call_id: [] for call_id in call_ids}
        if not cursor:
            break
    return result

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]], domain_lists: Dict[str, Set[str]]) -> Dict[str, Any]:
    """Process call data and apply product tags based on trackers and domain matching."""
    if not call_data:
        return {}
    call_data["partial_data"] = False
    try:
        account_context = {}
        for ctx in call_data.get("context", []):
            if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", [])):
                account_context = ctx
                break
        
        account_name = "Unknown"
        account_id = "Unknown"
        account_website = "Unknown"
        try:
            objects = account_context.get("objects", [])
            for obj in objects:
                if obj.get("objectType") == "Account":
                    account_id = obj.get("objectId", "Unknown")
                    for field in obj.get("fields", []):
                        if field.get("name") == "Name":
                            account_name = field.get("value", "Unknown")
                        if field.get("name") == "Website":
                            account_website = field.get("value", "Unknown")
        except Exception as e:
            logger.warning(f"Error extracting account info: {str(e)}")
            call_data["partial_data"] = True
        
        call_data["account_name"] = account_name
        call_data["account_id"] = account_id
        call_data["account_website"] = account_website
        call_data["utterances"] = transcript
        
        # Extract domain
        domain = extract_domain(account_website)
        call_data["domain"] = domain
        
        # Initialize product tags and debug info
        products = []
        domain_matches = []
        tracker_matches = []
        
        # Process trackers
        trackers = call_data.get("content", {}).get("trackers", [])
        for tracker in trackers:
            tracker_name = tracker.get("name", "")
            count = tracker.get("count", 0)
            if count > 0 and tracker_name in PRODUCT_TAG_TRACKERS:
                product_tag = PRODUCT_TAG_TRACKERS[tracker_name]
                products.append(product_tag)
                tracker_matches.append({
                    "tracker_name": tracker_name,
                    "count": count,
                    "product_tag": product_tag
                })
                logger.info(f"Applied product tag '{product_tag}' based on tracker '{tracker_name}' with count {count}")
        
        # Domain matching
        if domain:
            oa_match, oa_matched_domain = fuzzy_match_domain(domain, domain_lists["occupancy_analytics"])
            if oa_match:
                products.append("Occupancy Analytics (Tenant)")
                domain_matches.append({
                    "domain": domain,
                    "matched_domain": oa_matched_domain,
                    "list": "occupancy_analytics",
                    "product_tag": "Occupancy Analytics (Tenant)"
                })
                logger.info(f"Applied 'Occupancy Analytics (Tenant)' tag - matched domain '{domain}' to '{oa_matched_domain}'")
            
            owner_match, owner_matched_domain = fuzzy_match_domain(domain, domain_lists["owner_offering"])
            if owner_match:
                products.append("Owner Offering")
                domain_matches.append({
                    "domain": domain,
                    "matched_domain": owner_matched_domain,
                    "list": "owner_offering",
                    "product_tag": "Owner Offering"
                })
                logger.info(f"Applied 'Owner Offering' tag - matched domain '{domain}' to '{owner_matched_domain}'")
        
        call_data["products"] = products
        call_data["domain_matches"] = domain_matches
        call_data["tracker_matches"] = tracker_matches
        
        return call_data
    except Exception as e:
        logger.error(f"Normalization error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        call_data["partial_data"] = True
        return call_data

def format_duration(seconds):
    """Format duration in seconds to 'X min Y sec'."""
    try:
        seconds = int(seconds)
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes} min {remaining_seconds} sec"
    except (ValueError, TypeError):
        return "N/A"

def prepare_summary_df(calls: List[Dict[str, Any]], high_quality_call_ids: Set[str]) -> pd.DataFrame:
    """Prepare summary DataFrame for calls with high-quality utterances."""
    summary_data = []
    for call in calls:
        if not call:
            continue
        call_id = str(call.get("metaData", {}).get("id", ""))
        if call_id not in high_quality_call_ids:
            continue
        try:
            meta = call.get("metaData", {})
            parties = call.get("parties", [])
            
            internal_speakers = []
            external_speakers = []
            unknown_speakers = []
            
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
            
            internal_speakers = internal_speakers or ["None"]
            external_speakers = external_speakers or ["None"]
            unknown_speakers = unknown_speakers or ["None"]
            
            trackers = call.get("content", {}).get("trackers", [])
            tracker_list = []
            for tracker in trackers:
                count = tracker.get("count", 0)
                if count > 0:
                    name = TRACKER_RENAMES.get(tracker.get("name", ""), tracker.get("name", "Unknown"))
                    tracker_list.append((name, count))
            tracker_list.sort(key=lambda x: x[1], reverse=True)
            tracker_str = "|".join(f"{name}:{count}" for name, count in tracker_list if name != "Unknown")
            
            products = call.get("products", [])
            products_str = "|".join(products) if products else "None"
            
            call_date = "N/A"
            try:
                started = meta.get("started", "1970-01-01T00:00:00Z")
                call_date = datetime.fromisoformat(started.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp in call {call_id}: {str(e)}")
            
            summary_data.append({
                "call_id": call_id,
                "call_title": meta.get("title", "N/A"),
                "call_date": call_date,
                "duration": format_duration(meta.get("duration", 0)),
                "meeting_url": meta.get("meetingUrl", "N/A"),
                "account_id": call.get("account_id", "N/A"),
                "account_name": call.get("account_name", "N/A"),
                "account_website": call.get("account_website", "N/A"),
                "domain": call.get("domain", "N/A"),
                "products": products_str,
                "trackers": tracker_str,
                "internal_speakers": "|".join(internal_speakers),
                "external_speakers": "|".join(external_speakers),
                "unknown_speakers": "|".join(unknown_speakers)
            })
        except Exception as e:
            logger.error(f"Summary prep error for call {call_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            summary_data.append({
                "call_id": call_id,
                "call_title": "N/A",
                "call_date": "N/A",
                "duration": "N/A",
                "meeting_url": "N/A",
                "account_id": "N/A",
                "account_name": "N/A",
                "account_website": "N/A",
                "domain": "N/A",
                "products": "None",
                "trackers": "",
                "internal_speakers": "None",
                "external_speakers": "None",
                "unknown_speakers": "None"
            })
    return pd.DataFrame(summary_data)

def prepare_call_tables(calls: List[Dict[str, Any]], selected_products: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare DataFrames for included and excluded calls based on product filtering."""
    included_data = []
    excluded_data = []
    
    for call in calls:
        if not call:
            continue
        try:
            call_id = str(call.get("metaData", {}).get("id", ""))
            call_title = call.get("metaData", {}).get("title", "N/A")
            call_date = "N/A"
            try:
                started = call.get("metaData", {}).get("started", "1970-01-01T00:00:00Z")
                call_date = datetime.fromisoformat(started.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp in call {call_id}: {str(e)}")
            
            account_name = call.get("account_name", "N/A")
            products = call.get("products", [])
            products_str = "|".join(products) if products else "None"
            brief = call.get("content", {}).get("brief", "N/A")
            key_points = "; ".join(call.get("content", {}).get("keyPoints", [])) if call.get("content", {}).get("keyPoints", []) else "N/A"
            
            # Product filtering logic
            if not selected_products or "Select All" in selected_products:
                reason = "No product filter applied"
                included_data.append({
                    "call_id": call_id,
                    "call_title": call_title,
                    "call_date": call_date,
                    "account_name": account_name,
                    "products": products_str,
                    "brief": brief,
                    "keyPoints": key_points,
                    "reason": reason
                })
            else:
                matched_products = [p for p in products if p in selected_products]
                if matched_products or not products:  # Include None calls per inclusion-by-design
                    reason = f"Matched products: {('|'.join(matched_products) or 'None')}"
                    included_data.append({
                        "call_id": call_id,
                        "call_title": call_title,
                        "call_date": call_date,
                        "account_name": account_name,
                        "products": products_str,
                        "brief": brief,
                        "keyPoints": key_points,
                        "reason": reason
                    })
                else:
                    reason = "No matching products"
                    excluded_data.append({
                        "call_id": call_id,
                        "call_title": call_title,
                        "call_date": call_date,
                        "account_name": account_name,
                        "products": products_str,
                        "brief": brief,
                        "keyPoints": key_points,
                        "reason": reason
                    })
        except Exception as e:
            logger.error(f"Call table prep error for call {call_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    included_df = pd.DataFrame(included_data)
    excluded_df = pd.DataFrame(excluded_data)
    return included_df, excluded_df

def prepare_utterances_df(calls: List[Dict[str, Any]]) -> pd.DataFrame:
    """Prepare utterances DataFrame with quality labels."""
    utterances_data = []
    for call in calls:
        if not call:
            continue
        try:
            call_id = str(call.get("metaData", {}).get("id", ""))
            call_title = call.get("metaData", {}).get("title", "N/A")
            call_date = "N/A"
            try:
                started = call.get("metaData", {}).get("started", "1970-01-01T00:00:00Z")
                call_date = datetime.fromisoformat(started.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp in call {call_id}: {str(e)}")
            
            account_id = call.get("account_id", "N/A")
            account_name = call.get("account_name", "N/A")
            account_website = call.get("account_website", "N/A")
            domain = call.get("domain", "N/A")
            products = call.get("products", [])
            products_str = "|".join(products) if products else "None"
            parties = call.get("parties", [])
            partial_data = call.get("partial_data", False)
            
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
                
                speaker = speaker_info.get(speaker_id, {"name": "Unknown", "title": "", "affiliation": "Unknown"})
                
                quality = "high"
                if partial_data:
                    quality = "partial_data"
                elif speaker_id not in speaker_info:
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
                    "call_id": call_id,
                    "call_title": call_title,
                    "call_date": call_date,
                    "account_id": account_id,
                    "account_name": account_name,
                    "account_website": account_website,
                    "domain": domain,
                    "products": products_str,
                    "speaker_name": speaker["name"],
                    "speaker_job_title": speaker["title"] if speaker["title"] else "",
                    "speaker_affiliation": speaker["affiliation"],
                    "utterance_duration": duration,
                    "utterance_text": text,
                    "topic": topic,
                    "quality": quality
                })
        except Exception as e:
            logger.error(f"Utterance prep error for call {call_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    return pd.DataFrame(utterances_data)

def download_csv(df: pd.DataFrame, filename: str, label: str):
    """Generate download button for CSV."""
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(label, data=csv, file_name=filename, mime="text/csv")

def download_json(data: Any, filename: str, label: str):
    """Generate download button for JSON."""
    json_data = json.dumps(data, indent=4, ensure_ascii=False, default=str)
    st.download_button(label, data=json_data, file_name=filename, mime="application/json")

def main():
    st.title("📞 Gong Wizard")

    # Sidebar
    with st.sidebar:
        st.header("Configuration")
        access_key = st.text_input("Gong Access Key", type="password")
        secret_key = st.text_input("Gong Secret Key", type="password")
        
        if not access_key or not secret_key:
            st.error("Please provide both Gong Access Key and Secret Key.")
            st.stop()
        
        headers = create_auth_header(access_key, secret_key)
        
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

        select_all = st.checkbox("Select All Products", value=True)
        if select_all:
            selected_products = ALL_PRODUCT_TAGS
            st.multiselect("Product", ["Select All"] + ALL_PRODUCT_TAGS, default=["Select All"], disabled=True, help="Deselect 'Select All Products' to choose specific products.")
        else:
            selected_products = st.multiselect("Product", ALL_PRODUCT_TAGS, default=[])
        
        debug_mode = st.checkbox("Debug Mode", value=False)
        
        submit = st.button("Submit")

    domain_lists = load_domain_lists()

    if submit:
        if st.session_state.start_date > st.session_state.end_date:
            st.error("Start date must be before or equal to end date.")
            return
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
                
                if debug_mode and i == 0 and details:
                    st.subheader("Debug Information")
                    st.write(f"Number of calls found: {len(call_ids)}")
                    if details:
                        st.subheader("First Call Raw Data")
                        account_context = next((ctx for ctx in details[0].get("context", []) if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
                        account_id = next((obj.get("objectId", "Unknown") for obj in account_context.get("objects", []) if obj.get("objectType") == "Account"), "Unknown")
                        st.write(f"Account ID from API: {account_id}")
                        st.subheader("Parties/Speakers Data")
                        st.json(details[0].get("parties", [])[:3])
                
                for call in details:
                    call_id = str(call.get("metaData", {}).get("id", ""))
                    call_transcript = transcripts.get(call_id, [])
                    normalized_data = normalize_call_data(call, call_transcript, domain_lists)
                    if normalized_data:
                        full_data.append(normalized_data)

            if not full_data:
                st.error("No call details fetched.")
                return

            utterances_df = prepare_utterances_df(full_data)
            high_quality_call_ids = set(utterances_df[utterances_df["quality"] == "high"]["call_id"])
            summary_df = prepare_summary_df(full_data, high_quality_call_ids)
            utterances_filtered_df = utterances_df[utterances_df["quality"] == "high"]

            included_calls_df, excluded_calls_df = prepare_call_tables(full_data, selected_products)

            if debug_mode:
                st.subheader("Debug: Quality Distribution")
                quality_counts = utterances_df["quality"].value_counts()
                quality_dist = {q: f"{count} ({count/len(utterances_df)*100:.2f}%)" for q, count in quality_counts.items()}
                st.json(quality_dist)
                
                st.subheader("Debug: Product Tagging")
                tag_counts = pd.Series([tag for call in full_data for tag in call.get("products", ["None"])]).value_counts()
                tag_dist = {tag: f"{count} ({count/len(full_data)*100:.2f}%)" for tag, count in tag_counts.items()}
                st.json(tag_dist)
                
                st.subheader("Debug: Tracker Matches")
                tracker_samples = [call["tracker_matches"][:3] for call in full_data if call.get("tracker_matches", [])][:3]
                st.json(tracker_samples)
                
                st.subheader("Debug: Domain Matches")
                domain_samples = [call["domain_matches"][:3] for call in full_data if call.get("domain_matches", [])][:3]
                st.json(domain_samples)
                
                st.subheader("Debug: Sample Calls")
                sample_calls = [{k: call.get(k, "N/A") for k in ["call_id", "account_name", "domain", "products", "partial_data"]} for call in full_data[:3]]
                st.json(sample_calls)

            st.subheader("INCLUDED CALLS (Product Filter)")
            st.write("Note: Calls in 'Included Calls' are based on product filtering. Only calls with high-quality utterances (`quality == 'high'`) are included in `summary.csv`.")
            st.dataframe(included_calls_df)

            st.subheader("EXCLUDED CALLS (Product Filter)")
            st.dataframe(excluded_calls_df)

            st.subheader("Utterance Processing Stats")
            total_utterances = len(utterances_df)
            excluded_utterances = len(utterances_df[utterances_df["quality"] != "high"])
            excluded_pct = (excluded_utterances / total_utterances * 100) if total_utterances > 0 else 0
            st.write(f"Total Utterances Processed: {total_utterances}")
            st.write(f"Excluded from Filtered CSV: {excluded_utterances} ({excluded_pct:.2f}%)")
            st.write("Exclusions by Reason:")
            for reason in ["partial_data", "unknown_speaker", "internal", "low_quality_topic", "short"]:
                count = len(utterances_df[utterances_df["quality"] == reason])
                pct = (count / total_utterances * 100) if total_utterances > 0 else 0
                st.write(f"- {reason}: {count} ({pct:.2f}%)")

            start_date_str = st.session_state.start_date.strftime("%d%b%y").lower()
            end_date_str = st.session_state.end_date.strftime("%d%b%y").lower()

            st.subheader("Download Options")
            col1, col2 = st.columns(2)
            with col1:
                download_csv(utterances_df, f"utterances_full_gong_{start_date_str}_to_{end_date_str}.csv", "Download Utterances Full CSV")
                download_csv(utterances_filtered_df, f"utterances_filtered_gong_{start_date_str}_to_{end_date_str}.csv", "Download Utterances Filtered CSV")
            with col2:
                download_csv(summary_df, f"summary_gong_{start_date_str}_to_{end_date_str}.csv", "Download Summary CSV")
                download_json(full_data, f"calls_full_gong_{start_date_str}_to_{end_date_str}.json", "Download Calls Full JSON")

if __name__ == "__main__":
    main()