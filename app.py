import streamlit as st
import pandas as pd
import requests
import base64
import json
import time
import os
from urllib.parse import urlparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Tuple
import logging
import tldextract
from rapidfuzz import fuzz

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

# Synthetic domain lists for testing
TEST_DOMAIN_LISTS = {
    "occupancy_analytics": {"example0.com", "example1.com"},
    "owner_offering": {"example2.com", "example3.com"}
}

# Synthetic call and transcript data (abridged for brevity)
SYNTHETIC_CALLS = [
    {
        "metaData": {"id": "call_0", "title": "Test Call 0", "started": "2025-04-17T15:45:36.540364Z"},
        "context": [{
            "objects": [{
                "objectType": "Account",
                "objectId": "acc_0",
                "fields": [
                    {"name": "Name", "value": "Account 0"},
                    {"name": "Website", "value": "https://www.example0.com"}
                ]
            }]
        }],
        "content": {
            "trackers": [{"name": "ODCV", "count": 2}, {"name": "Filter", "count": 1}],
            "topics": ["Call Setup", "Business Value", "Wrap-up"],
            "brief": "Brief summary",
            "keyPoints": ["Key point 0"],
            "callOutcome": "Advanced"
        },
        "parties": [
            {"speakerId": "speaker_ext_0", "name": "External User 0", "title": "VP of Operations", "affiliation": "External"},
            {"speakerId": "speaker_int_0", "name": "Internal Rep 0", "title": "AE", "affiliation": "Internal"}
        ]
    }
]

SYNTHETIC_TRANSCRIPTS = {
    "call_0": [
        {
            "speakerId": "speaker_ext_0",
            "topic": "Business Value",
            "sentences": [{"text": "This is a high-quality external utterance.", "start": 0, "end": 10}]
        },
        {
            "speakerId": "speaker_int_0",
            "topic": "Call Setup",
            "sentences": [{"text": "Internal speaker saying hello.", "start": 10, "end": 20}]
        }
    ]
}

# Synthetic edge cases (abridged)
SYNTHETIC_EDGE_CALLS = [
    {
        "metaData": {"id": "call_1", "title": "Test Call 1", "started": "2025-04-17T15:45:36.540364Z"},
        "context": [{
            "objects": [{
                "objectType": "Account",
                "objectId": "acc_1",
                "fields": [
                    {"name": "Name", "value": "Account 1"},
                    {"name": "Website", "value": "https://www.example1.com"}
                ]
            }]
        }],
        "content": {
            "trackers": [{"name": "ODCV", "count": 1}],
            "topics": ["Business Value"],
            "brief": "Brief summary",
            "keyPoints": ["Key point 1"],
            "callOutcome": "Advanced"
        },
        "parties": [
            {"speakerId": "speaker_ext_1", "name": "External User 1", "title": "Manager", "affiliation": "External"}
        ]
    }
]

SYNTHETIC_EDGE_TRANSCRIPTS = {
    "call_2": [
        {
            "speakerId": "speaker_ext_2",
            "topic": "Business Value",
            "sentences": [{"text": "This is another high-quality external utterance.", "start": 0, "end": 10}]
        }
    ]
}

# Load domain lists for product tagging
@st.cache_data
def load_domain_lists():
    """Load domain lists from CSVs for Occupancy Analytics and Owner Offering."""
    domain_lists = {
        "occupancy_analytics": set(),
        "owner_offering": set()
    }
    
    # Log file existence check
    occupancy_file = "Occupancy Analytics Tenant Customers Gong Bot Sheet3.csv"
    owner_file = "Owner Orgs Gong Bot Sheet3.csv"
    
    logger.info(f"Current directory: {os.getcwd()}")
    logger.info(f"Files in directory: {os.listdir('.')}")
    logger.info(f"Checking for file existence - Occupancy: {os.path.exists(occupancy_file)}, Owner: {os.path.exists(owner_file)}")
    
    try:
        if os.path.exists(occupancy_file):
            logger.info(f"Reading occupancy file: {occupancy_file}")
            # Try to read the first few lines to debug
            try:
                with open(occupancy_file, 'r', encoding='utf-8') as f:
                    first_lines = [next(f) for _ in range(5) if _ < 5]
                    logger.info(f"First lines of occupancy file: {first_lines}")
            except Exception as e:
                logger.warning(f"Could not read occupancy file directly: {str(e)}")
            
            try:
                occupancy_df = pd.read_csv(occupancy_file, header=None, names=["domain"], encoding='utf-8')
                logger.info(f"Occupancy DataFrame shape: {occupancy_df.shape}")
                logger.info(f"Occupancy DataFrame head: {occupancy_df.head().to_dict()}")
                
                if "domain" in occupancy_df.columns:
                    occupancy_domains = set(occupancy_df["domain"].str.lower().dropna().apply(extract_domain).tolist())
                    domain_lists["occupancy_analytics"] = occupancy_domains
                    logger.info(f"Loaded {len(domain_lists['occupancy_analytics'])} Occupancy Analytics domains")
                else:
                    logger.warning(f"Columns in occupancy DF: {occupancy_df.columns.tolist()}")
            except Exception as e:
                logger.error(f"Error processing occupancy file: {str(e)}")
                st.sidebar.error(f"Error processing occupancy file: {str(e)}")
        else:
            logger.warning(f"Occupancy file not found: {occupancy_file}")
            st.sidebar.warning(f"Occupancy file not found: {occupancy_file}")
        
        if os.path.exists(owner_file):
            logger.info(f"Reading owner file: {owner_file}")
            try:
                with open(owner_file, 'r', encoding='utf-8') as f:
                    first_lines = [next(f) for _ in range(5) if _ < 5]
                    logger.info(f"First lines of owner file: {first_lines}")
            except Exception as e:
                logger.warning(f"Could not read owner file directly: {str(e)}")
            
            try:
                owner_df = pd.read_csv(owner_file, header=None, names=["domain"], encoding='utf-8')
                logger.info(f"Owner DataFrame shape: {owner_df.shape}")
                logger.info(f"Owner DataFrame head: {owner_df.head().to_dict()}")
                
                if "domain" in owner_df.columns:
                    owner_domains = set(owner_df["domain"].str.lower().dropna().apply(extract_domain).tolist())
                    domain_lists["owner_offering"] = owner_domains
                    logger.info(f"Loaded {len(domain_lists['owner_offering'])} Owner Offering domains")
                else:
                    logger.warning(f"Columns in owner DF: {owner_df.columns.tolist()}")
            except Exception as e:
                logger.error(f"Error processing owner file: {str(e)}")
                st.sidebar.error(f"Error processing owner file: {str(e)}")
        else:
            logger.warning(f"Owner file not found: {owner_file}")
            st.sidebar.warning(f"Owner file not found: {owner_file}")
        
        # Check for overlaps
        overlaps = domain_lists["occupancy_analytics"].intersection(domain_lists["owner_offering"])
        if overlaps:
            logger.warning(f"Domain overlaps detected: {overlaps}. Calls may be tagged with both products.")
            st.sidebar.warning(f"Domain overlaps detected. Calls may be tagged with both products.")
    except Exception as e:
        logger.error(f"Domain list load error: {str(e)}", exc_info=True)  # Include full traceback
        st.sidebar.error(f"Domain list load error: {str(e)}")
    
    if not domain_lists["occupancy_analytics"] and not domain_lists["owner_offering"]:
        st.sidebar.warning("Domain lists are empty. Using test data for domain-based product tagging.")
        return TEST_DOMAIN_LISTS
    
    return domain_lists

def extract_domain(url: str) -> str:
    """Extract and normalize domain from a URL using tldextract."""
    if not url or url in ["Unknown", "N/A"]:
        return ""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        parsed = urlparse(url)
        hostname = parsed.netloc.split(":")[0]
        url = f"https://{hostname}"
        extracted = tldextract.extract(url)
        domain = extracted.registered_domain.lower()
        return domain if domain and domain != "." else ""
    except Exception as e:
        logger.warning(f"Error extracting domain from {url}: {str(e)}")
        return ""

def fuzzy_match_domain(domain: str, domain_list: Set[str], debug_info: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """Check if domain matches any in domain_list using rapidfuzz."""
    if not domain:
        return False, ""
    best_match = None
    best_ratio = 0
    match_type = "none"
    for list_domain in domain_list:
        ratio = fuzz.ratio(domain, list_domain)
        if ratio > best_ratio and ratio >= FUZZY_MATCH_THRESHOLD:
            best_ratio = ratio
            best_match = list_domain
            match_type = "fuzzy" if ratio < 100 else "exact"
    if best_match:
        logger.info(f"Matched domain '{domain}' to '{best_match}' with ratio {best_ratio:.2f} ({match_type})")
        debug_info.append({
            "domain": domain,
            "matched_domain": best_match,
            "ratio": best_ratio,
            "match_type": match_type
        })
        return True, best_match
    return False, ""

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
                    page_calls = data.get("calls", [])
                    call_ids.extend(call["id"] for call in page_calls)
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
                    st.error(f"Gong API error {response.status_code}: {response.text}")
                    logger.error(f"Call list fetch failed: {response.status_code} - {response.text}")
                    raise RuntimeError(f"Gong API failure: {response.status_code}")
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
                    page_calls = data.get("calls", [])
                    call_details.extend(page_calls)
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
                    logger.error(f"Call details fetch failed: {response.status_code} - {response.text}")
                    raise RuntimeError(f"Call details API failure: {response.status_code}")
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep((2 ** attempt) * 1)
                else:
                    logger.error(f"Error fetching call details: {str(e)}")
                    return call_details
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
                        break
                    time.sleep(1)
                    break
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 1))
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Transcript fetch failed: {response.status_code} - {response.text}")
                    raise RuntimeError(f"Transcript API failure: {response.status_code}")
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep((2 ** attempt) * 1)
                else:
                    logger.error(f"Error fetching transcripts: {str(e)}")
                    return {call_id: [] for call_id in call_ids}
    return result

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]], domain_lists: Dict[str, Set[str]], debug_domain_matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process call data and apply product tags based on trackers and domain matching."""
    if not call_data:
        logger.warning("Call data is empty or None")
        return {}
    
    call_data = {
        "metaData": call_data.get("metaData", {}),
        "context": call_data.get("context", []),
        "content": call_data.get("content", {
            "structure": [],
            "topics": [],
            "trackers": [],
            "trackerOccurrences": [],
            "brief": "",
            "keyPoints": [],
            "callOutcome": ""
        }),
        "parties": call_data.get("parties", []),
        "utterances": transcript if transcript is not None else [],
        "products": [],
        "domain_matches": [],
        "tracker_matches": [],
        "partial_data": False
    }
    
    if "id" not in call_data["metaData"]:
        logger.error("Call metaData missing id")
        return {}
    
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
        call_data["domain"] = extract_domain(account_website)
        
        for tracker in call_data["content"].get("trackers", []):
            tracker_name = tracker.get("name", "")
            count = tracker.get("count", 0)
            if count > 0 and tracker_name in PRODUCT_TAG_TRACKERS:
                product_tag = PRODUCT_TAG_TRACKERS[tracker_name]
                call_data["products"].append(product_tag)
                call_data["tracker_matches"].append({
                    "tracker_name": tracker_name,
                    "count": count,
                    "product_tag": product_tag
                })
                logger.info(f"Applied product tag '{product_tag}' based on tracker '{tracker_name}'")
        
        if call_data["domain"]:
            oa_match, oa_matched_domain = fuzzy_match_domain(call_data["domain"], domain_lists["occupancy_analytics"], debug_domain_matches)
            if oa_match:
                call_data["products"].append("Occupancy Analytics (Tenant)")
                call_data["domain_matches"].append({
                    "domain": call_data["domain"],
                    "matched_domain": oa_matched_domain,
                    "list": "occupancy_analytics",
                    "product_tag": "Occupancy Analytics (Tenant)"
                })
            
            owner_match, owner_matched_domain = fuzzy_match_domain(call_data["domain"], domain_lists["owner_offering"], debug_domain_matches)
            if owner_match:
                call_data["products"].append("Owner Offering")
                call_data["domain_matches"].append({
                    "domain": call_data["domain"],
                    "matched_domain": owner_matched_domain,
                    "list": "owner_offering",
                    "product_tag": "Owner Offering"
                })
        
        call_data["products"] = list(set(call_data["products"]))
        return call_data
    except Exception as e:
        logger.error(f"Normalization error: {str(e)}")
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

def prepare_call_tables(calls: List[Dict[str, Any]], selected_products: List[str], high_quality_call_ids: Set[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare DataFrames for included and excluded calls."""
    included_data = []
    excluded_data = []
    
    for call in calls:
        if not call or "metaData" not in call:
            continue
        call_id = call["metaData"].get("id", "")
        if not call_id:
            logger.error(f"Call ID missing in metaData: {call.get('metaData', {})}")
            continue
        call_title = call["metaData"].get("title", "N/A")
        call_date = "N/A"
        try:
            started = call["metaData"].get("started", "1970-01-01T00:00:00Z")
            call_date = datetime.fromisoformat(started.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning(f"Invalid timestamp in call {call_id}")
        
        account_name = call.get("account_name", "N/A")
        products = call.get("products", [])
        products_str = "|".join(products) if products else "None"
        brief = call["content"].get("brief", "N/A")
        key_points = "; ".join(call["content"].get("keyPoints", [])) if call["content"].get("keyPoints", []) else "N/A"
        
        product_reason = "No product filter applied" if not selected_products or "Select All" in selected_products else (
            f"Matched products: {('|'.join([p for p in products if p in selected_products]))}" if any(p in selected_products for p in products) else
            "No product tags (included by design)" if not products else "No matching products"
        )
        quality_reason = "" if call_id in high_quality_call_ids else "No high-quality utterances"
        
        row = {
            "call_id": call_id,
            "call_title": call_title,
            "call_date": call_date,
            "account_name": account_name,
            "products": products_str,
            "brief": brief,
            "keyPoints": key_points
        }
        
        if product_reason == "No matching products" or quality_reason:
            row["reason"] = f"{product_reason}{' but excluded due to ' + quality_reason.lower() if quality_reason else ''}"
            excluded_data.append(row)
        else:
            row["reason"] = product_reason
            included_data.append(row)
    
    return pd.DataFrame(included_data), pd.DataFrame(excluded_data)

def prepare_utterances_df(calls: List[Dict[str, Any]]) -> pd.DataFrame:
    """Prepare utterances DataFrame with quality labels, deduplicating utterances."""
    utterances_data = []
    debug_speaker_info = []
    seen_utterances = set()
    
    for call in calls:
        if not call or "metaData" not in call:
            continue
        call_id = call["metaData"].get("id", "")
        if not call_id:
            logger.error(f"Call ID missing in metaData: {call.get('metaData', {})}")
            continue
        call_title = call["metaData"].get("title", "N/A")
        call_date = "N/A"
        try:
            started = call["metaData"].get("started", "1970-01-01T00:00:00Z")
            call_date = datetime.fromisoformat(started.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning(f"Invalid timestamp in call {call_id}")
        
        account_id = call.get("account_id", "N/A")
        account_name = call.get("account_name", "N/A")
        account_website = call.get("account_website", "N/A")
        domain = call.get("domain", "N/A")
        products = call.get("products", [])
        products_str = "|".join(products) if products else "None"
        parties = call.get("parties", [])
        partial_data = call.get("partial_data", False)
        
        speaker_info = {party.get("speakerId", ""): {
            "name": party.get("name", "Unknown"),
            "title": party.get("title", ""),
            "affiliation": party.get("affiliation", "Unknown")
        } for party in parties if party.get("speakerId", "")}
        
        for idx, utterance in enumerate(call.get("utterances", [])):
            sentences = utterance.get("sentences", [])
            if not sentences:
                continue
            text = " ".join(s.get("text", "N/A") for s in sentences)
            start_time = sentences[0].get("start", 0)
            end_time = sentences[-1].get("end", 0)
            
            utterance_key = (start_time, end_time, text)
            if utterance_key in seen_utterances:
                logger.debug(f"Deduplicated utterance in call {call_id}: {text[:20]}")
                continue
            seen_utterances.add(utterance_key)
            
            word_count = len(text.split())
            topic = utterance.get("topic", "N/A")
            speaker_id = utterance.get("speakerId", "")
            
            speaker = speaker_info.get(speaker_id, None)
            fallback_attempted = False
            speaker_name_in_utterance = None
            
            if not speaker:
                logger.warning(f"Unmatched speakerId: {speaker_id} in call {call_id}, utterance {idx}")
                for sentence in sentences:
                    if "speaker" in sentence and "name" in sentence["speaker"]:
                        speaker_name_in_utterance = sentence["speaker"]["name"]
                        break
                if speaker_name_in_utterance:
                    matched = next((p for p in parties if p.get("name") == speaker_name_in_utterance or p.get("emailAddress") == speaker_name_in_utterance), None)
                    if matched:
                        speaker = {
                            "name": matched.get("name", "Unknown"),
                            "title": matched.get("title", ""),
                            "affiliation": matched.get("affiliation", "Unknown")
                        }
                    fallback_attempted = True
                if not speaker:
                    speaker = {
                        "name": speaker_name_in_utterance or "Unknown",
                        "title": "",
                        "affiliation": "External"
                    }
            
            debug_speaker_info.append({
                "call_id": call_id,
                "utterance_index": idx,
                "speaker_id": speaker_id,
                "speaker_name_in_utterance": speaker_name_in_utterance,
                "matched_speaker": speaker,
                "fallback_attempted": fallback_attempted
            })
            
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
                "speaker_job_title": speaker["title"] or "",
                "speaker_affiliation": speaker["affiliation"],
                "utterance_duration": duration,
                "utterance_text": text,
                "topic": topic,
                "quality": quality
            })
    
    st.session_state.debug_speaker_info = debug_speaker_info
    return pd.DataFrame(utterances_data)

def download_csv(df: pd.DataFrame, filename: str, label: str):
    """Generate download button for CSV."""
    if df.empty:
        st.warning(f"Cannot download {filename} - DataFrame is empty")
        return
    try:
        csv = df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(label, data=csv, file_name=filename, mime="text/csv")
    except Exception as e:
        st.error(f"Error creating CSV download: {str(e)}")
        logger.error(f"CSV download error for {filename}: {str(e)}")

def download_json(data: Any, filename: str, label: str):
    """Generate download button for JSON."""
    if not data:
        st.warning(f"Cannot download {filename} - No data available")
        return
    try:
        json_data = json.dumps(data, indent=4, ensure_ascii=False, default=str)
        st.download_button(label, data=json_data, file_name=filename, mime="application/json")
    except Exception as e:
        st.error(f"Error creating JSON download: {str(e)}")
        logger.error(f"JSON download error for {filename}: {str(e)}")

def run_test_harness(selected_products: List[str], debug_mode: bool = False):
    """Run test harness with synthetic data."""
    st.subheader("Running Test Harness with Synthetic Data")
    
    domain_lists = TEST_DOMAIN_LISTS
    debug_domain_matches = []
    
    all_synthetic_calls = SYNTHETIC_CALLS + SYNTHETIC_EDGE_CALLS
    all_synthetic_transcripts = SYNTHETIC_TRANSCRIPTS.copy()
    all_synthetic_transcripts.update(SYNTHETIC_EDGE_TRANSCRIPTS)
    
    full_data = []
    dropped_calls_count = 0
    for call in all_synthetic_calls:
        call_id = call.get("metaData", {}).get("id", "")
        if not call_id:
            dropped_calls_count += 1
            continue
        call_transcript = all_synthetic_transcripts.get(call_id, [])
        normalized_data = normalize_call_data(call, call_transcript, domain_lists, debug_domain_matches)
        if normalized_data and normalized_data.get("metaData"):
            full_data.append(normalized_data)
        else:
            dropped_calls_count += 1
            logger.warning(f"Dropped synthetic call {call_id} due to normalization failure")
    
    if not full_data:
        st.error("No synthetic call details processed.")
        return
    
    utterances_df = prepare_utterances_df(full_data)
    high_quality_call_ids = set(utterances_df[utterances_df["quality"] == "high"]["call_id"])
    included_calls_df, excluded_calls_df = prepare_call_tables(full_data, selected_products, high_quality_call_ids)
    
    utterances_filtered_df = pd.DataFrame(columns=utterances_df.columns) if included_calls_df.empty else (
        utterances_df[utterances_df["quality"] == "high"][utterances_df["call_id"].isin(set(included_calls_df["call_id"]))] if "call_id" in included_calls_df.columns else pd.DataFrame(columns=utterances_df.columns)
    )
    
    st.subheader("Test Harness Results")
    st.write("**Full Data (Normalized Calls):**")
    st.json(full_data[:3])
    st.write("**Utterances DataFrame:**")
    st.dataframe(utterances_df)
    st.write("**Utterances Filtered DataFrame:**")
    st.dataframe(utterances_filtered_df)
    st.write("**Included Calls DataFrame:**")
    st.dataframe(included_calls_df)
    st.write("**Excluded Calls DataFrame:**")
    st.dataframe(excluded_calls_df)
    
    if dropped_calls_count > 0:
        st.warning(f"⚠️ {dropped_calls_count} synthetic calls were dropped due to normalization failures.")
    
    if debug_mode:
        st.subheader("Debug: Quality Distribution")
        if not utterances_df.empty:
            quality_counts = utterances_df["quality"].value_counts()
            st.json({q: f"{count} ({count/len(utterances_df)*100:.2f}%)" for q, count in quality_counts.items()})
        else:
            st.write("No utterances to analyze.")
        
        st.subheader("Debug: Internal Speaker Breakdown")
        internal_utterances = utterances_df[utterances_df["quality"] == "internal"]
        if not internal_utterances.empty:
            internal_speaker_counts = internal_utterances.groupby("speaker_name").size().sort_values(ascending=False).head(10)
            st.json({name: f"{count} ({count/len(internal_utterances)*100:.2f}%)" for name, count in internal_speaker_counts.items()})
        else:
            st.write("No internal utterances found.")
        
        st.subheader("Debug: Product Tagging")
        if full_data:
            tag_counts = pd.Series([tag for call in full_data for tag in call.get("products", ["None"])]).value_counts()
            st.json({tag: f"{count} ({count/len(full_data)*100:.2f}%)" for tag, count in tag_counts.items()})
        else:
            st.write("No call data to analyze.")
        
        st.subheader("Debug: Tracker Matches")
        st.json([call["tracker_matches"][:3] for call in full_data if call.get("tracker_matches", [])][:3])
        
        st.subheader("Debug: Domain Matches")
        st.json(debug_domain_matches[:3])
        
        st.subheader("Debug: Sample Calls")
        st.json([{k: call.get(k, "N/A") for k in ["call_id", "account_name", "domain", "products", "partial_data"]} for call in full_data[:3]])
        
        if "debug_speaker_info" in st.session_state:
            st.subheader("Debug: Speaker Matching Info")
            st.json(st.session_state.debug_speaker_info[:10])

def main():
    try:
        logger.info("💡 Entered main()")
        st.write("✅ App started")  # Early UI test
        
        # Add more debug output at the very beginning
        st.write("📊 Debug Info")
        st.write(f"Current directory: {os.getcwd()}")
        st.write(f"Files in directory: {os.listdir('.')}")
        
        # Startup test mode option
        startup_test = st.sidebar.checkbox("Startup Test Mode", value=True)
        
        if startup_test:
            st.title("📞 Gong Wizard - Startup Diagnostic")
            st.write("Running in startup test mode to diagnose issues")
            
            # Test individual components
            st.subheader("1. Environment Check")
            st.write(f"Python version: {os.sys.version}")
            st.write(f"Working directory: {os.getcwd()}")
            
            st.subheader("2. File System Check")
            files = os.listdir('.')
            st.write(f"Files in directory: {files}")
            
            # Check CSV files specifically
            csv_files = [f for f in files if f.endswith('.csv')]
            st.write(f"CSV files found: {csv_files}")
            
            for csv_file in csv_files:
                st.write(f"Details for {csv_file}:")
                try:
                    stat_info = os.stat(csv_file)
                    st.write(f"- Size: {stat_info.st_size} bytes")
                    st.write(f"- Last modified: {datetime.fromtimestamp(stat_info.st_mtime)}")
                    
                    # Try to read first few lines
                    try:
                        with open(csv_file, 'r', encoding='utf-8') as f:
                            first_lines = [next(f) for _ in range(5) if _ < 5]
                            st.write("- First lines:")
                            for i, line in enumerate(first_lines):
                                st.code(f"Line {i+1}: {line.strip()}")
                    except Exception as e:
                        st.write(f"- Could not read file content: {str(e)}")
                except Exception as e:
                    st.write(f"- Error getting file stats: {str(e)}")
            
            # Try to load domain lists
            st.subheader("3. Domain List Loading Test")
            try:
                domain_lists = load_domain_lists()
                st.write(f"Domain lists loaded: {domain_lists.keys()}")
                st.write(f"Occupancy Analytics domains: {len(domain_lists['occupancy_analytics'])}")
                st.write(f"Owner Offering domains: {len(domain_lists['owner_offering'])}")
                
                # Show sample domains
                if domain_lists['occupancy_analytics']:
                    st.write("Sample Occupancy Analytics domains:")
                    st.write(list(domain_lists['occupancy_analytics'])[:5])
                
                if domain_lists['owner_offering']:
                    st.write("Sample Owner Offering domains:")
                    st.write(list(domain_lists['owner_offering'])[:5])
            except Exception as e:
                st.error(f"Error loading domain lists: {str(e)}")
            
            # Test synthetic data
            st.subheader("4. Synthetic Data Test")
            st.write("Testing with synthetic data...")
            try:
                test_harness_btn = st.button("Run Synthetic Data Test")
                if test_harness_btn:
                    run_test_harness(ALL_PRODUCT_TAGS, True)
            except Exception as e:
                st.error(f"Error in test harness: {str(e)}")
            
            # End of test mode
            st.success("Startup diagnostic completed")
            return
        
        st.title("📞 Gong Wizard")
        
        with st.sidebar:
            st.header("Configuration")
            access_key = st.text_input("Gong Access Key", type="password")
            secret_key = st.text_input("Gong Secret Key", type="password")
            
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
            selected_products = ALL_PRODUCT_TAGS if select_all else st.multiselect("Product", ALL_PRODUCT_TAGS, default=[])
            if select_all:
                st.multiselect("Product", ["Select All"] + ALL_PRODUCT_TAGS, default=["Select All"], disabled=True, help="Deselect 'Select All Products' to choose specific products.")
            
            debug_mode = st.checkbox("Debug Mode", value=False)
            run_test = st.checkbox("Run Test Harness", value=False)
            submit = st.button("Submit")
        
        domain_lists = load_domain_lists()
        debug_domain_matches = []
        
        if run_test:
            run_test_harness(selected_products, debug_mode)
            return
        
        if not submit:
            st.info("Configure settings and click Submit to process Gong data.")
            return
        
        if not access_key or not secret_key:
            st.error("Please provide both Gong Access Key and Secret Key.")
            return
        
        if st.session_state.start_date > st.session_state.end_date:
            st.error("Start date must be before or equal to end date.")
            return
        
        required_keys = ["utterances_df", "utterances_filtered_df", "included_calls_df", "excluded_calls_df", "full_data"]
        if not all(k in st.session_state for k in required_keys):
            with st.spinner("Fetching calls..."):
                session = requests.Session()
                headers = create_auth_header(access_key, secret_key)
                session.headers.update(headers)
                try:
                    call_ids = fetch_call_list(session, st.session_state.start_date.isoformat() + "T00:00:00Z", st.session_state.end_date.isoformat() + "T23:59:59Z")
                    if not call_ids:
                        st.error("No calls found in the selected date range.")
                        return
                except Exception as e:
                    st.error(f"Failed to fetch call list: {str(e)}")
                    return
                
                full_data = []
                dropped_calls_count = 0
                batch_size = 50
                for i in range(0, len(call_ids), batch_size):
                    batch = call_ids[i:i + batch_size]
                    try:
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
                            call_id = call.get("metaData", {}).get("id", "")
                            call_transcript = transcripts.get(call_id, [])
                            normalized_data = normalize_call_data(call, call_transcript, domain_lists, debug_domain_matches)
                            if normalized_data and normalized_data.get("metaData"):
                                full_data.append(normalized_data)
                            else:
                                dropped_calls_count += 1
                                logger.warning(f"Dropped call {call_id} due to normalization failure")
                    except Exception as e:
                        st.error(f"Error processing batch starting with call ID {batch[0]}: {str(e)}")
                        continue
                
                if not full_data:
                    st.error("No call details processed. Check API credentials and try again.")
                    return
                
                utterances_df = prepare_utterances_df(full_data)
                high_quality_call_ids = set(utterances_df[utterances_df["quality"] == "high"]["call_id"])
                included_calls_df, excluded_calls_df = prepare_call_tables(full_data, selected_products, high_quality_call_ids)
                
                utterances_filtered_df = pd.DataFrame(columns=utterances_df.columns) if included_calls_df.empty else (
                    utterances_df[utterances_df["quality"] == "high"][utterances_df["call_id"].isin(set(included_calls_df["call_id"]))] if "call_id" in included_calls_df.columns else pd.DataFrame(columns=utterances_df.columns)
                )
                
                st.session_state.utterances_df = utterances_df
                st.session_state.utterances_filtered_df = utterances_filtered_df
                st.session_state.included_calls_df = included_calls_df
                st.session_state.excluded_calls_df = excluded_calls_df
                st.session_state.full_data = full_data
                st.session_state.debug_domain_matches = debug_domain_matches
                st.session_state.dropped_calls_count = dropped_calls_count
        
        utterances_df = st.session_state.utterances_df
        utterances_filtered_df = st.session_state.utterances_filtered_df
        included_calls_df = st.session_state.included_calls_df
        excluded_calls_df = st.session_state.excluded_calls_df
        full_data = st.session_state.full_data
        debug_domain_matches = st.session_state.debug_domain_matches
        dropped_calls_count = st.session_state.dropped_calls_count
        
        if dropped_calls_count > 0:
            st.warning(f"⚠️ {dropped_calls_count} calls were dropped due to normalization failures.")
        
        st.subheader("🔍 Auto QA Checks")
        if "call_id" not in included_calls_df.columns or "call_id" not in excluded_calls_df.columns:
            st.error("❌ ERROR: One or both DataFrames are missing the 'call_id' column.")
        else:
            included_ids = set(included_calls_df["call_id"])
            excluded_ids = set(excluded_calls_df["call_id"])
            overlap_ids = included_ids.intersection(excluded_ids)
            st.success("✅ No overlap between included and excluded summary CSVs.") if not overlap_ids else st.error(f"❌ ERROR: {len(overlap_ids)} call(s) in both included and excluded summary!")
            
            included_high_utterance_ids = set(utterances_filtered_df["call_id"]) if "call_id" in utterances_filtered_df.columns else set()
            included_but_missing_utterance_ids = included_ids - included_high_utterance_ids
            st.success("✅ All included calls have at least one high-quality utterance.") if not included_but_missing_utterance_ids else st.error(f"❌ ERROR: {len(included_but_missing_utterance_ids)} included calls have no high-quality utterances!")
            
            excluded_check = excluded_calls_df[
                (excluded_calls_df["products"] != "None") &
                (~excluded_calls_df["reason"].str.contains("no high-quality utterances", case=False))
            ]
            st.success("✅ Excluded calls with product tags correctly labeled.") if excluded_check.empty else st.error(f"❌ ERROR: {len(excluded_check)} excluded calls with product tags are mislabeled.")
        
        if debug_mode:
            st.subheader("Debug: Quality Distribution")
            if not utterances_df.empty:
                quality_counts = utterances_df["quality"].value_counts()
                st.json({q: f"{count} ({count/len(utterances_df)*100:.2f}%)" for q, count in quality_counts.items()})
            
            st.subheader("Debug: Internal Speaker Breakdown")
            internal_utterances = utterances_df[utterances_df["quality"] == "internal"]
            if not internal_utterances.empty:
                internal_speaker_counts = internal_utterances.groupby("speaker_name").size().sort_values(ascending=False).head(10)
                st.json({name: f"{count} ({count/len(internal_utterances)*100:.2f}%)" for name, count in internal_speaker_counts.items()})
            
            st.subheader("Debug: Product Tagging")
            if full_data:
                tag_counts = pd.Series([tag for call in full_data for tag in call.get("products", ["None"])]).value_counts()
                st.json({tag: f"{count} ({count/len(full_data)*100:.2f}%)" for tag, count in tag_counts.items()})
            
            st.subheader("Debug: Tracker Matches")
            st.json([call["tracker_matches"][:3] for call in full_data if call.get("tracker_matches", [])][:3])
            
            st.subheader("Debug: Domain Matches")
            st.json(debug_domain_matches[:3])
            
            st.subheader("Debug: Sample Calls")
            st.json([{k: call.get(k, "N/A") for k in ["call_id", "account_name", "domain", "products", "partial_data"]} for call in full_data[:3]])
            
            if "debug_speaker_info" in st.session_state:
                st.subheader("Debug: Speaker Matching Info")
                st.json(st.session_state.debug_speaker_info[:10])
        
        st.subheader("INCLUDED CALLS (Product Filter)")
        st.write("Calls with no product tags are included by design.")
        st.dataframe(included_calls_df)
        
        st.subheader("EXCLUDED CALLS (Product Filter)")
        st.dataframe(excluded_calls_df)
        
        st.subheader("Utterance Processing Stats")
        total_utterances = len(utterances_df)
        if total_utterances > 0:
            excluded_utterances = len(utterances_df[utterances_df["quality"] != "high"])
            excluded_pct = excluded_utterances / total_utterances * 100
            st.write(f"Total Utterances Processed: {total_utterances}")
            st.write(f"Excluded from Filtered CSV: {excluded_utterances} ({excluded_pct:.2f}%)")
            st.write("Exclusions by Reason:")
            for reason in ["partial_data", "unknown_speaker", "internal", "low_quality_topic", "short"]:
                count = len(utterances_df[utterances_df["quality"] == reason])
                pct = count / total_utterances * 100
                st.write(f"- {reason}: {count} ({pct:.2f}%)")
        
        with st.expander("Download Options"):
            start_date_str = st.session_state.start_date.strftime("%d%b%y").lower()
            end_date_str = st.session_state.end_date.strftime("%d%b%y").lower()
            col1, col2 = st.columns(2)
            with col1:
                download_csv(st.session_state.utterances_df, f"utterances_full_gong_{start_date_str}_to_{end_date_str}.csv", "Utterances - Full CSV")
                download_csv(st.session_state.utterances_filtered_df, f"utterances_filtered_gong_{start_date_str}_to_{end_date_str}.csv", "Utterances - Filtered CSV")
            with col2:
                download_csv(st.session_state.included_calls_df, f"summary_included_gong_{start_date_str}_to_{end_date_str}.csv", "Summary - Included CSV")
                download_csv(st.session_state.excluded_calls_df, f"summary_excluded_gong_{start_date_str}_to_{end_date_str}.csv", "Summary - Excluded CSV")
                download_json(st.session_state.full_data, f"calls_full_gong_{start_date_str}_to_{end_date_str}.json", "Calls - Full JSON")
    
    except Exception as e:
        logger.error(f"Critical error in main: {str(e)}", exc_info=True)
        st.error(f"Application error: {str(e)}")
        st.write("Please check the logs for more details and contact support.")

if __name__ == "__main__":
    main()