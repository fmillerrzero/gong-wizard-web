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
import tldextract  # Added for proper domain normalization

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

# Synthetic call and transcript data
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

# Synthetic edge cases
SYNTHETIC_EDGE_CALLS = [
    # 1. Missing metaData
    {
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
    },
    # 2. Mismatched Subdomain
    {
        "metaData": {"id": "call_2", "title": "Test Call 2", "started": "2025-04-17T16:00:00Z"},
        "context": [{
            "objects": [{
                "objectType": "Account",
                "objectId": "acc_2",
                "fields": [
                    {"name": "Name", "value": "Account 2"},
                    {"name": "Website", "value": "https://www.sub.example3.com"}
                ]
            }]
        }],
        "content": {
            "trackers": [],
            "topics": ["Business Value"],
            "brief": "Brief summary",
            "keyPoints": ["Key point 2"],
            "callOutcome": "Advanced"
        },
        "parties": [
            {"speakerId": "speaker_ext_2", "name": "External User 2", "title": "Director", "affiliation": "External"}
        ]
    },
    # 3. All Internal / Low-Quality Utterances
    {
        "metaData": {"id": "call_3", "title": "Test Call 3", "started": "2025-04-17T16:15:00Z"},
        "context": [{
            "objects": [{
                "objectType": "Account",
                "objectId": "acc_3",
                "fields": [
                    {"name": "Name", "value": "Account 3"},
                    {"name": "Website", "value": "https://www.example3.com"}
                ]
            }]
        }],
        "content": {
            "trackers": [{"name": "ODCV", "count": 1}],
            "topics": ["Small Talk"],
            "brief": "Brief summary",
            "keyPoints": ["Key point 3"],
            "callOutcome": "Advanced"
        },
        "parties": [
            {"speakerId": "speaker_int_3", "name": "Internal Rep 3", "title": "AE", "affiliation": "Internal"}
        ]
    },
    # 4. No Product Tags / No Domain Match
    {
        "metaData": {"id": "call_4", "title": "Test Call 4", "started": "2025-04-17T16:30:00Z"},
        "context": [{
            "objects": [{
                "objectType": "Account",
                "objectId": "acc_4",
                "fields": [
                    {"name": "Name", "value": "Account 4"},
                    {"name": "Website", "value": "https://unmatched-domain.com"}
                ]
            }]
        }],
        "content": {
            "trackers": [],
            "topics": ["Business Value"],
            "brief": "Brief summary",
            "keyPoints": ["Key point 4"],
            "callOutcome": "Advanced"
        },
        "parties": [
            {"speakerId": "speaker_ext_4", "name": "External User 4", "title": "CEO", "affiliation": "External"}
        ]
    },
    # 5. Malformed Party Entry
    {
        "metaData": {"id": "call_5", "title": "Test Call 5", "started": "2025-04-17T16:45:00Z"},
        "context": [{
            "objects": [{
                "objectType": "Account",
                "objectId": "acc_5",
                "fields": [
                    {"name": "Name", "value": "Account 5"},
                    {"name": "Website", "value": "https://www.example5.com"}
                ]
            }]
        }],
        "content": {
            "trackers": [{"name": "Filter", "count": 1}],
            "topics": ["Business Value"],
            "brief": "Brief summary",
            "keyPoints": ["Key point 5"],
            "callOutcome": "Advanced"
        },
        "parties": [
            {"name": "Unknown User 5", "title": "Unknown Title"}  # Missing speakerId and affiliation
        ]
    },
    # 6. Malformed Timestamp + Duplicate Speakers
    {
        "metaData": {"id": "call_6", "title": "Test Call 6 - Timestamp & Speaker Edge Case", "started": "invalid-timestamp"},
        "context": [{
            "objects": [{
                "objectType": "Account",
                "objectId": "acc_6",
                "fields": [
                    {"name": "Name", "value": "Account 6"},
                    {"name": "Website", "value": "https://www.example6.com"}
                ]
            }]
        }],
        "content": {
            "trackers": [],
            "topics": ["Business Value"],
            "brief": "Brief summary",
            "keyPoints": ["Key point 6"],
            "callOutcome": "Advanced"
        },
        "parties": [
            {"speakerId": "dup_speaker", "name": "Person A", "title": "CTO", "affiliation": "External"},
            {"speakerId": "dup_speaker", "name": "Person B", "title": "Engineer", "affiliation": "Internal"}  # Duplicate ID with conflicting affiliation
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
    ],
    "call_3": [
        {
            "speakerId": "speaker_int_3",
            "topic": "Small Talk",
            "sentences": [{"text": "Internal speaker discussing small talk.", "start": 0, "end": 10}]
        }
    ],
    "call_4": [
        {
            "speakerId": "speaker_ext_4",
            "topic": "Business Value",
            "sentences": [{"text": "This is a high-quality external utterance with no product tags.", "start": 0, "end": 10}]
        }
    ],
    "call_5": [
        {
            "speakerId": "",  # Missing speakerId
            "topic": "Business Value",
            "sentences": [{"text": "This utterance should default to External.", "start": 0, "end": 10}]
        }
    ],
    "call_6": [
        {
            "speakerId": "dup_speaker",
            "topic": "Business Value",
            "sentences": [{"text": "This tests speaker conflict and bad timestamp.", "start": 0, "end": 10}]
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
    try:
        # Validate Occupancy Analytics file
        occupancy_df = pd.read_csv("Occupancy Analytics Tenant Customers Gong Bot Sheet3.csv", header=None, names=["domain"])
        if "domain" not in occupancy_df.columns:
            raise ValueError("Occupancy Analytics CSV missing 'domain' column")
        # Normalize domains in the list using tldextract
        occupancy_domains = set(occupancy_df["domain"].str.lower().dropna().apply(extract_domain).tolist())
        domain_lists["occupancy_analytics"] = occupancy_domains
        
        # Validate Owner Offering file
        owner_df = pd.read_csv("Owner Orgs Gong Bot Sheet3.csv", header=None, names=["domain"])
        if "domain" not in owner_df.columns:
            raise ValueError("Owner Orgs CSV missing 'domain' column")
        # Normalize domains in the list using tldextract
        owner_domains = set(owner_df["domain"].str.lower().dropna().apply(extract_domain).tolist())
        domain_lists["owner_offering"] = owner_domains
        
        logger.info(f"Loaded {len(domain_lists['occupancy_analytics'])} Occupancy Analytics domains and {len(domain_lists['owner_offering'])} Owner Offering domains")
    except FileNotFoundError as e:
        st.sidebar.error(f"Domain list file not found: {str(e)}. Please ensure the files are present.")
        logger.error(f"Domain list file not found: {str(e)}")
    except ValueError as e:
        st.sidebar.error(f"Domain list file error: {str(e)}. Please check the file format.")
        logger.error(f"Domain list file error: {str(e)}")
    except Exception as e:
        st.sidebar.error(f"Error loading domain lists: {str(e)}")
        logger.error(f"Domain list load error: {str(e)}")
    
    # Check if domain lists are empty and notify user
    if not domain_lists["occupancy_analytics"] and not domain_lists["owner_offering"]:
        st.error("Domain lists are empty. Domain-based product tagging will not function. Please check the domain list files.")
        logger.warning("Both domain lists are empty after loading. Domain tagging will be skipped.")
    elif not domain_lists["occupancy_analytics"]:
        st.warning("Occupancy Analytics domain list is empty. Tagging for 'Occupancy Analytics (Tenant)' will not function.")
        logger.warning("Occupancy Analytics domain list is empty after loading.")
    elif not domain_lists["owner_offering"]:
        st.warning("Owner Offering domain list is empty. Tagging for 'Owner Offering' will not function.")
        logger.warning("Owner Offering domain list is empty after loading.")
    
    return domain_lists

# Extract and normalize domain from URL using tldextract
def extract_domain(url: str) -> str:
    """Extract and normalize domain from a URL using tldextract to handle complex TLDs."""
    if not url or url in ["Unknown", "N/A"]:
        return ""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        extracted = tldextract.extract(url)
        # Combine domain and suffix (e.g., 'client.com.sg' instead of just 'com.sg')
        domain = f"{extracted.domain}.{extracted.suffix}".lower()
        if not domain or domain == ".":
            return ""
        return domain
    except Exception as e:
        logger.warning(f"Error extracting domain from {url}: {str(e)}")
        return ""

# Fuzzy domain matching
def fuzzy_match_domain(domain: str, domain_list: Set[str], debug_info: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """Check if domain matches any in domain_list using fuzzy matching."""
    if not domain:
        return False, ""
    
    best_match = None
    best_ratio = 0
    match_type = "none"
    for list_domain in domain_list:
        ratio = SequenceMatcher(None, domain, list_domain).ratio() * 100
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
                    page_calls = data.get("calls", [])
                    call_ids.extend(call["id"] for call in page_calls)
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor and page_calls:  # Continue if page has data but no cursor
                        break
                    elif not page_calls and not cursor:  # Break if page is empty
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
                    page_calls = data.get("calls", [])
                    call_details.extend(page_calls)
                    cursor = data.get("records", {}).get("cursor")
                    if not cursor and page_calls:  # Continue if page has data but no cursor
                        break
                    elif not page_calls and not cursor:  # Break if page is empty
                        break
                    time.sleep(1)
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
                    if not cursor and transcripts:  # Continue if page has data but no cursor
                        break
                    elif not transcripts and not cursor:  # Break if page is empty
                        break
                    time.sleep(1)
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
    return result

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]], domain_lists: Dict[str, Set[str]], debug_domain_matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process call data and apply product tags based on trackers and domain matching."""
    if not call_data:
        logger.warning("Call data is empty or None")
        return {}
    
    # Validate required fields
    if "metaData" not in call_data or not call_data["metaData"]:
        logger.error("Call missing metaData")
        return {}
    if "id" not in call_data["metaData"]:
        logger.error("Call metaData missing id")
        return {}
    if "parties" not in call_data:
        logger.error(f"Call {call_data['metaData']['id']} missing parties")
        return {}
    
    # Ensure consistent structure even if processing fails
    call_data["metaData"] = call_data.get("metaData", {})
    call_data["context"] = call_data.get("context", [])
    call_data["content"] = call_data.get("content", {
        "structure": [],
        "topics": [],
        "trackers": [],
        "trackerOccurrences": [],
        "brief": "",
        "keyPoints": [],
        "callOutcome": ""
    })
    call_data["parties"] = call_data.get("parties", [])
    call_data["utterances"] = transcript if transcript is not None else []
    call_data["products"] = []
    call_data["domain_matches"] = []
    call_data["tracker_matches"] = []
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
        
        # Extract domain
        domain = extract_domain(account_website)
        call_data["domain"] = domain
        
        # Process trackers
        trackers = call_data["content"].get("trackers", [])
        for tracker in trackers:
            tracker_name = tracker.get("name", "")
            count = tracker.get("count", 0)
            if count > 0:
                if tracker_name in PRODUCT_TAG_TRACKERS:
                    product_tag = PRODUCT_TAG_TRACKERS[tracker_name]
                    call_data["products"].append(product_tag)
                    call_data["tracker_matches"].append({
                        "tracker_name": tracker_name,
                        "count": count,
                        "product_tag": product_tag
                    })
                    logger.info(f"Applied product tag '{product_tag}' based on tracker '{tracker_name}' with count {count}")
                else:
                    logger.debug(f"Unmapped tracker '{tracker_name}' with count {count} in call {call_data.get('metaData', {}).get('id', 'Unknown')}")
        
        # Domain matching
        if domain:
            oa_match, oa_matched_domain = fuzzy_match_domain(domain, domain_lists["occupancy_analytics"], debug_domain_matches)
            if oa_match:
                call_data["products"].append("Occupancy Analytics (Tenant)")
                call_data["domain_matches"].append({
                    "domain": domain,
                    "matched_domain": oa_matched_domain,
                    "list": "occupancy_analytics",
                    "product_tag": "Occupancy Analytics (Tenant)"
                })
                logger.info(f"Applied 'Occupancy Analytics (Tenant)' tag - matched domain '{domain}' to '{oa_matched_domain}'")
            
            owner_match, owner_matched_domain = fuzzy_match_domain(domain, domain_lists["owner_offering"], debug_domain_matches)
            if owner_match:
                call_data["products"].append("Owner Offering")
                call_data["domain_matches"].append({
                    "domain": domain,
                    "matched_domain": owner_matched_domain,
                    "list": "owner_offering",
                    "product_tag": "Owner Offering"
                })
                logger.info(f"Applied 'Owner Offering' tag - matched domain '{domain}' to '{owner_matched_domain}'")
        
        # Deduplicate products list
        call_data["products"] = list(set(call_data["products"]))
        
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

def prepare_call_tables(calls: List[Dict[str, Any]], selected_products: List[str], high_quality_call_ids: Set[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare DataFrames for included and excluded calls based on product filtering and high-quality utterances."""
    included_data = []
    excluded_data = []
    
    for call in calls:
        if not call or "metaData" not in call:
            continue
        try:
            call_id = str(call.get("metaData", {}).get("id", ""))
            if not call_id:
                logger.error(f"Call ID missing or empty in metaData: {call.get('metaData', {})}")
                continue
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
            product_reason = ""
            quality_reason = ""
            if not selected_products or "Select All" in selected_products:
                product_reason = "No product filter applied"
            else:
                matched_products = [p for p in products if p in selected_products]
                if matched_products:
                    product_reason = f"Matched products: {('|'.join(matched_products))}"
                elif not products:  # Calls with no products are included by design
                    product_reason = "No product tags (included by design)"
                else:
                    product_reason = "No matching products"
            
            if call_id in high_quality_call_ids:
                quality_reason = ""
            else:
                quality_reason = "No high-quality utterances"
            
            if product_reason == "No matching products":
                excluded_data.append({
                    "call_id": call_id,
                    "call_title": call_title,
                    "call_date": call_date,
                    "account_name": account_name,
                    "products": products_str,
                    "brief": brief,
                    "keyPoints": key_points,
                    "reason": product_reason
                })
            elif quality_reason:
                excluded_data.append({
                    "call_id": call_id,
                    "call_title": call_title,
                    "call_date": call_date,
                    "account_name": account_name,
                    "products": products_str,
                    "brief": brief,
                    "keyPoints": key_points,
                    "reason": f"{product_reason} but excluded due to {quality_reason.lower()}"
                })
            else:
                included_data.append({
                    "call_id": call_id,
                    "call_title": call_title,
                    "call_date": call_date,
                    "account_name": account_name,
                    "products": products_str,
                    "brief": brief,
                    "keyPoints": key_points,
                    "reason": product_reason
                })
        except Exception as e:
            logger.error(f"Call table prep error for call {call_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    included_df = pd.DataFrame(included_data)
    excluded_df = pd.DataFrame(excluded_data)
    return included_df, excluded_df

def prepare_utterances_df(calls: List[Dict[str, Any]]) -> pd.DataFrame:
    """Prepare utterances DataFrame with quality labels, deduplicating utterances by (start, end, text)."""
    utterances_data = []
    debug_speaker_info = []  # For debug output
    seen_utterances = set()  # For deduplication by (start, end, text)
    for call in calls:
        if not call or "metaData" not in call:
            continue
        try:
            call_id = str(call.get("metaData", {}).get("id", ""))
            if not call_id:
                logger.error(f"Call ID missing or empty in metaData: {call.get('metaData', {})}")
                continue
            call_title = call.get("metaData", {}).get("title", "N/A")
            call_date = "N/A"
            try:
                started = call.get("metaData", {}).get("started", "1970-01-01T00:00:00Z")
                call_date = datetime.fromisoformat(started.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp in call {call_id}: {str(e)}")
            
            # Validate required fields
            if "parties" not in call:
                logger.error(f"Call {call_id} missing parties")
                continue
            
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
            
            for idx, utterance in enumerate(call.get("utterances", [])):
                sentences = utterance.get("sentences", [])
                if not sentences:
                    continue
                text = " ".join(s.get("text", "N/A") for s in sentences)
                start_time = sentences[0].get("start", 0)
                end_time = sentences[-1].get("end", 0)
                
                # Deduplicate utterances by (start, end, text)
                utterance_key = (start_time, end_time, text)
                if utterance_key in seen_utterances:
                    logger.debug(f"Deduplicated utterance in call {call_id}, index {idx}: {text}")
                    continue
                seen_utterances.add(utterance_key)
                
                word_count = len(text.split())
                topic = utterance.get("topic", "N/A")
                speaker_id = utterance.get("speakerId", "")
                
                # Attempt to match speaker using speakerId
                speaker = speaker_info.get(speaker_id, None)
                fallback_attempted = False
                if not speaker:
                    logger.warning(f"Missing or unmatched speakerId: {speaker_id} in call {call_id}, utterance {idx}, text: {text[:20]}")
                    # Fallback: match by name if available
                    speaker_name_in_utterance = None
                    for sentence in sentences:
                        if "speaker" in sentence and "name" in sentence["speaker"]:
                            speaker_name_in_utterance = sentence["speaker"]["name"]
                            break
                    if speaker_name_in_utterance:  # Validate before using
                        matched = next((p for p in parties if p.get("name") == speaker_name_in_utterance or p.get("emailAddress") == speaker_name_in_utterance), None)
                        if matched:
                            speaker = {
                                "name": matched.get("name", "Unknown"),
                                "title": matched.get("title", ""),
                                "affiliation": matched.get("affiliation", "Unknown")
                            }
                        fallback_attempted = True
                    # If still no match, default to External
                    if not speaker:
                        speaker = {
                            "name": speaker_name_in_utterance if speaker_name_in_utterance else "Unknown",
                            "title": "",
                            "affiliation": "External"
                        }
                
                # Debug info for speaker matching
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
                    "speaker_job_title": speaker["title"] if speaker["title"] is not None else "",
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
    
    # Store debug info in session state for display
    if "debug_speaker_info" not in st.session_state:
        st.session_state.debug_speaker_info = debug_speaker_info
    
    return pd.DataFrame(utterances_data)

def download_csv(df: pd.DataFrame, filename: str, label: str):
    """Generate download button for CSV."""
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(label, data=csv, file_name=filename, mime="text/csv")

def download_json(data: Any, filename: str, label: str):
    """Generate download button for JSON."""
    json_data = json.dumps(data, indent=4, ensure_ascii=False, default=str)
    st.download_button(label, data=json_data, file_name=filename, mime="application/json")

def run_test_harness(selected_products: List[str], debug_mode: bool = False):
    """Run the test harness with synthetic data and display the results."""
    st.subheader("Running Test Harness with Synthetic Data")
    
    # Use synthetic domain lists for testing
    domain_lists = TEST_DOMAIN_LISTS
    debug_domain_matches = []  # For debug output of domain matching
    
    # Combine synthetic calls and edge cases
    all_synthetic_calls = SYNTHETIC_CALLS + SYNTHETIC_EDGE_CALLS
    all_synthetic_transcripts = SYNTHETIC_TRANSCRIPTS.copy()
    all_synthetic_transcripts.update(SYNTHETIC_EDGE_TRANSCRIPTS)
    
    # Process synthetic calls and transcripts
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
    utterances_filtered_df = utterances_df[utterances_df["quality"] == "high"]
    # Filter utterances_filtered_df to only include calls from included_calls_df
    utterances_filtered_df = utterances_filtered_df[utterances_filtered_df["call_id"].isin(set(included_calls_df["call_id"]))]

    # Cache the computed data in session state
    st.session_state.utterances_df = utterances_df
    st.session_state.utterances_filtered_df = utterances_filtered_df
    st.session_state.included_calls_df = included_calls_df
    st.session_state.excluded_calls_df = excluded_calls_df
    st.session_state.full_data = full_data
    st.session_state.debug_domain_matches = debug_domain_matches
    st.session_state.dropped_calls_count = dropped_calls_count

    # Display test harness results
    st.subheader("Test Harness Results")
    st.write("**Full Data (Normalized Calls):**")
    st.json(full_data)
    st.write("**Utterances DataFrame:**")
    st.dataframe(utterances_df)
    st.write("**Utterances Filtered DataFrame:**")
    st.dataframe(utterances_filtered_df)
    st.write("**Included Calls DataFrame:**")
    st.dataframe(included_calls_df)
    st.write("**Excluded Calls DataFrame:**")
    st.dataframe(excluded_calls_df)

    if dropped_calls_count > 0:
        st.warning(f"⚠️ {dropped_calls_count} synthetic calls were dropped due to normalization failures. Check logs for details.")
        if debug_mode:
            st.write(f"Debug: Dropped {dropped_calls_count} synthetic calls due to normalization issues.")

    if debug_mode:
        st.subheader("Debug: Quality Distribution")
        quality_counts = utterances_df["quality"].value_counts()
        quality_dist = {q: f"{count} ({count/len(utterances_df)*100:.2f}%)" for q, count in quality_counts.items()}
        st.json(quality_dist)
        
        st.subheader("Debug: Internal Speaker Breakdown")
        internal_utterances = utterances_df[utterances_df["quality"] == "internal"]
        internal_speaker_counts = internal_utterances.groupby("speaker_name").size().sort_values(ascending=False).head(10)
        internal_speaker_dist = {name: f"{count} ({count/len(internal_utterances)*100:.2f}%)" for name, count in internal_speaker_counts.items()}
        st.json(internal_speaker_dist)
        
        st.subheader("Debug: Product Tagging")
        tag_counts = pd.Series([tag for call in full_data for tag in call.get("products", ["None"])]).value_counts()
        tag_dist = {tag: f"{count} ({count/len(full_data)*100:.2f}%)" for tag, count in tag_counts.items()}
        st.json(tag_dist)
        
        st.subheader("Debug: Tracker Matches")
        tracker_samples = [call["tracker_matches"][:3] for call in full_data if call.get("tracker_matches", [])][:3]
        st.json(tracker_samples)
        
        st.subheader("Debug: Domain Matches")
        st.json(debug_domain_matches[:3])  # Show first 3 for brevity
        
        st.subheader("Debug: Sample Calls")
        sample_calls = [{k: call.get(k, "N/A") for k in ["call_id", "account_name", "domain", "products", "partial_data"]} for call in full_data[:3]]
        st.json(sample_calls)
        
        st.subheader("Debug: Speaker Matching Info")
        st.json(st.session_state.debug_speaker_info[:10])  # Show first 10 for brevity

def main():
    st.title("📞 Gong Wizard")

    # Sidebar
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
        if select_all:
            selected_products = ALL_PRODUCT_TAGS
            st.multiselect("Product", ["Select All"] + ALL_PRODUCT_TAGS, default=["Select All"], disabled=True, help="Deselect 'Select All Products' to choose specific products.")
        else:
            selected_products = st.multiselect("Product", ALL_PRODUCT_TAGS, default=[])
        
        debug_mode = st.checkbox("Debug Mode", value=False)
        run_test = st.checkbox("Run Test Harness", value=False)
        
        submit = st.button("Submit")

    domain_lists = load_domain_lists()
    debug_domain_matches = []  # For debug output of domain matching

    if run_test:
        run_test_harness(selected_products, debug_mode)
        return

    if submit:
        if not access_key or not secret_key:
            st.error("Please provide both Gong Access Key and Secret Key.")
            return

        if st.session_state.start_date > st.session_state.end_date:
            st.error("Start date must be before or equal to end date.")
            return

        # Check if data is already computed and cached in session state
        required_keys = ["utterances_df", "utterances_filtered_df", "included_calls_df", "excluded_calls_df", "full_data"]
        if not all(k in st.session_state for k in required_keys):
            with st.spinner("Fetching calls..."):
                session = requests.Session()
                headers = create_auth_header(access_key, secret_key)
                session.headers.update(headers)
                call_ids = fetch_call_list(session, st.session_state.start_date.isoformat() + "T00:00:00Z", st.session_state.end_date.isoformat() + "T23:59:59Z")
                if not call_ids:
                    st.error("No calls found.")
                    return

                full_data = []
                dropped_calls_count = 0  # Track dropped calls
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
                        normalized_data = normalize_call_data(call, call_transcript, domain_lists, debug_domain_matches)
                        if normalized_data and normalized_data.get("metaData"):
                            full_data.append(normalized_data)
                        else:
                            dropped_calls_count += 1
                            logger.warning(f"Dropped call {call_id} due to normalization failure")

                if not full_data:
                    st.error("No call details fetched.")
                    return

                utterances_df = prepare_utterances_df(full_data)
                high_quality_call_ids = set(utterances_df[utterances_df["quality"] == "high"]["call_id"])
                included_calls_df, excluded_calls_df = prepare_call_tables(full_data, selected_products, high_quality_call_ids)
                utterances_filtered_df = utterances_df[utterances_df["quality"] == "high"]
                # Filter utterances_filtered_df to only include calls from included_calls_df
                utterances_filtered_df = utterances_filtered_df[utterances_filtered_df["call_id"].isin(set(included_calls_df["call_id"]))]

                # Cache the computed data in session state
                st.session_state.utterances_df = utterances_df
                st.session_state.utterances_filtered_df = utterances_filtered_df
                st.session_state.included_calls_df = included_calls_df
                st.session_state.excluded_calls_df = excluded_calls_df
                st.session_state.full_data = full_data
                st.session_state.debug_domain_matches = debug_domain_matches
                st.session_state.dropped_calls_count = dropped_calls_count

        # Use cached data for display
        utterances_df = st.session_state.utterances_df
        utterances_filtered_df = st.session_state.utterances_filtered_df
        included_calls_df = st.session_state.included_calls_df
        excluded_calls_df = st.session_state.excluded_calls_df
        full_data = st.session_state.full_data
        debug_domain_matches = st.session_state.debug_domain_matches
        dropped_calls_count = st.session_state.dropped_calls_count

        # Display dropped calls count
        if dropped_calls_count > 0:
            st.warning(f"⚠️ {dropped_calls_count} calls were dropped due to normalization failures. Check logs for details.")
            if debug_mode:
                st.write(f"Debug: Dropped {dropped_calls_count} calls due to normalization issues.")

        # === 🔍 Auto QA Checks ===
        st.subheader("🔍 Auto QA Checks")

        # 1. No overlap between included and excluded
        included_ids = set(included_calls_df["call_id"])
        excluded_ids = set(excluded_calls_df["call_id"])
        overlap_ids = included_ids.intersection(excluded_ids)
        if overlap_ids:
            st.error(f"❌ ERROR: {len(overlap_ids)} call(s) appear in both included and excluded summary!")
            st.write(overlap_ids)
        else:
            st.success("✅ No overlap between included and excluded summary CSVs.")

        # 2. All included calls must have ≥1 high-quality utterance
        included_high_utterance_ids = set(utterances_filtered_df["call_id"])
        included_but_missing_utterance_ids = included_ids - included_high_utterance_ids
        if included_but_missing_utterance_ids:
            st.error(f"❌ ERROR: {len(included_but_missing_utterance_ids)} included calls have no high-quality utterances!")
            st.write(included_but_missing_utterance_ids)
        else:
            st.success("✅ All included calls have at least one high-quality utterance.")

        # 3. All excluded calls with products but no utterances must say so
        excluded_check = excluded_calls_df[
            (excluded_calls_df["products"] != "None") &
            (~excluded_calls_df["reason"].str.contains("no high-quality utterances", case=False))
        ]
        if not excluded_check.empty:
            st.error(f"❌ ERROR: {len(excluded_check)} excluded calls with product tags are mislabeled.")
            st.dataframe(excluded_check)
        else:
            st.success("✅ Excluded calls with product tags correctly labeled if missing high-quality utterances.")

        if debug_mode:
            st.subheader("Debug: Quality Distribution")
            quality_counts = utterances_df["quality"].value_counts()
            quality_dist = {q: f"{count} ({count/len(utterances_df)*100:.2f}%)" for q, count in quality_counts.items()}
            st.json(quality_dist)
            
            st.subheader("Debug: Internal Speaker Breakdown")
            internal_utterances = utterances_df[utterances_df["quality"] == "internal"]
            internal_speaker_counts = internal_utterances.groupby("speaker_name").size().sort_values(ascending=False).head(10)
            internal_speaker_dist = {name: f"{count} ({count/len(internal_utterances)*100:.2f}%)" for name, count in internal_speaker_counts.items()}
            st.json(internal_speaker_dist)
            
            st.subheader("Debug: Product Tagging")
            tag_counts = pd.Series([tag for call in full_data for tag in call.get("products", ["None"])]).value_counts()
            tag_dist = {tag: f"{count} ({count/len(full_data)*100:.2f}%)" for tag, count in tag_counts.items()}
            st.json(tag_dist)
            
            st.subheader("Debug: Tracker Matches")
            tracker_samples = [call["tracker_matches"][:3] for call in full_data if call.get("tracker_matches", [])][:3]
            st.json(tracker_samples)
            
            st.subheader("Debug: Domain Matches")
            st.json(debug_domain_matches[:3])  # Show first 3 for brevity
            
            st.subheader("Debug: Sample Calls")
            sample_calls = [{k: call.get(k, "N/A") for k in ["call_id", "account_name", "domain", "products", "partial_data"]} for call in full_data[:3]]
            st.json(sample_calls)
            
            st.subheader("Debug: Speaker Matching Info")
            st.json(st.session_state.debug_speaker_info[:10])  # Show first 10 for brevity

        st.subheader("INCLUDED CALLS (Product Filter)")
        st.write("Note: Calls in 'Included Calls' are based on product filtering and have at least one high-quality utterance. Calls with no product tags are included by design.")
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

        # Isolate download buttons in an expander to prevent rerunning main logic
        with st.expander("Download Options"):
            start_date_str = st.session_state.start_date.strftime("%d%b%y").lower()
            end_date_str = st.session_state.end_date.strftime("%d%b%y").lower()
            col1, col2 = st.columns(2)
            with col1:
                download_csv(st.session_state.utterances_df, f"utterances_full_gong_{start_date_str}_to_{end_date_str}.csv", "Utterances - Full CSV")
                download_csv(st.session_state.utterances_filtered_df, f"utterances_filtered_gong_{start_date_str}_to_{end_date_str}.csv", "Utterances - Filtered CSV")
                download_csv(st.session_state.included_calls_df, f"summary_included_gong_{start_date_str}_to_{end_date_str}.csv", "Summary - Included CSV")
            with col2:
                download_csv(st.session_state.excluded_calls_df, f"summary_excluded_gong_{start_date_str}_to_{end_date_str}.csv", "Summary - Excluded CSV")
                download_json(st.session_state.full_data, f"calls_full_gong_{start_date_str}_to_{end_date_str}.json", "Calls - Full JSON")

if __name__ == "__main__":
    main()