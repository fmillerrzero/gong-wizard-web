import pandas as pd
import requests
import base64
import json
import time
from datetime import datetime, timedelta, date
import logging
from typing import Dict, List, Optional, Tuple, Any
import pytz
import re
import os
import tempfile
from flask import Flask, render_template, request, send_file, session

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Required for session management

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SF_TZ = pytz.timezone('America/Los_Angeles')

# Product mappings with consistent plural handling
PRODUCT_MAPPINGS = {
    "IAQ Monitoring": ["Air Quality"],
    "ODCV": ["ODCV"],
    "Secure Air": ["Filter", "Filtration"],
    "Occupancy Analytics": [
        r'capacit(y|ies)',
        r'connect[\s-]?(dashboard|platform)(s)?',
        r'coworker(s)?',
        r'densit(y|ies)',
        r'dwell[\s-]?time(s)?',
        r'group[\s-]?size(s)?',
        r'hot[\s-]?desk(s)?',
        r'occupancy[\s-]?analytics',
        r'real[\s-]?time[\s-]?api(s)?',
        r'real[\s-]?time[\s-]?occupancy',
        r'room[\s-]?reservation(s)?',
        r'space[\s-]?type(s)?',
        r'stream[\s-]?api(s)?',
        r'utilization(s)?',
        r'vergesense',
        r'workplace[\s-]?(strategy|strategist)(s)?',
        r'heat[\s-]?map(s)?'
    ]
}
ALL_PRODUCT_TAGS = list(PRODUCT_MAPPINGS.keys())

class GongAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Gong API Error {status_code}: {message}")

class GongAPIClient:
    def __init__(self, access_key: str, secret_key: str, api_version: str = "v2"):
        self.access_key = access_key
        self.secret_key = secret_key
        self.api_version = api_version
        self.base_url = f"https://us-11211.api.gong.io/{api_version}"
        self.session = requests.Session()
        self.session.headers.update(self.create_auth_header())

    def create_auth_header(self) -> Dict[str, str]:
        credentials = base64.b64encode(f"{self.access_key}:{self.secret_key}".encode()).decode()
        return {"Authorization": f"Basic {credentials}"}

    def verify_api_compatibility(self) -> bool:
        try:
            response = self.session.get(f"{self.base_url}/calls?limit=1", timeout=5)
            if response.status_code == 200:
                return True
            elif response.status_code == 400:
                error_data = response.json()
                if "version" in (error_data.get("message", "").lower() if isinstance(error_data.get("message"), str) else ""):
                    logger.warning(f"API version compatibility issue detected: {error_data}")
                    return False
            return True
        except Exception as e:
            logger.error(f"API compatibility check failed: {str(e)}")
            return False

def is_retryable_error(status_code: int) -> bool:
    return status_code == 429 or (500 <= status_code < 600)

def convert_to_sf_time(utc_time: str) -> str:
    try:
        utc_dt = datetime.fromisoformat(utc_time.replace("Z", "+00:00"))
        sf_dt = utc_dt.astimezone(SF_TZ)
        return sf_dt.strftime("%m/%d/%y")
    except Exception as e:
        logger.warning(f"Invalid timestamp {utc_time}: {str(e)}")
        return "N/A"

def get_case_insensitive(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Access dictionary keys case-insensitively."""
    for k in d:
        if k.lower() == key.lower():
            return d[k]
    return default

def validate_call_data(call: Dict[str, Any]) -> bool:
    """Validate the structure of call data from the API."""
    if not call or not isinstance(call, dict):
        return False
    if "metaData" not in call or not isinstance(call["metaData"], dict):
        return False
    if "id" not in call["metaData"]:
        return False
    if "content" in call and not isinstance(call["content"], dict):
        return False
    return True

def safely_get_utterances(call: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Safely extract utterances with validation."""
    if not call or not isinstance(call, dict):
        return []
    utterances = call.get("utterances", [])
    if not isinstance(utterances, list):
        logger.warning(f"Unexpected utterances format for call {call.get('metaData', {}).get('id', 'unknown')}")
        return []
    return utterances

def safely_extract_field_values(context_data: List[Dict[str, Any]], field_name: str, object_type: str = None) -> List[str]:
    """Extract field values robustly, case-insensitively."""
    values = []
    for context in context_data:
        if not isinstance(context, dict):
            continue
        for obj in context.get("objects", []):
            if not isinstance(obj, dict):
                continue
            obj_type = get_case_insensitive(obj, "objectType", "")
            if object_type and obj_type.lower() != object_type.lower():
                continue
            for field in obj.get("fields", []):
                if not isinstance(field, dict):
                    continue
                field_name_value = get_case_insensitive(field, "name", "")
                if field_name_value.lower() == field_name.lower():
                    value = field.get("value")
                    if value is not None:
                        values.append(str(value))
    return values

def detect_gong_schema_version(call_data: Dict[str, Any]) -> str:
    """Detect Gong API schema version from call data structure."""
    if not call_data or not isinstance(call_data, dict):
        return "unknown"
    content = call_data.get("content", {})
    if "trackers" in content:
        if any("phrases" in tracker for tracker in content.get("trackers", [])):
            return "v2_with_phrases"
        return "v2_base"
    elif "topics" in content:
        return "v1"
    return "unknown"

def fetch_call_list(client: GongAPIClient, from_date: str, to_date: str) -> List[str]:
    url = f"{client.base_url}/calls"
    params = {"fromDateTime": from_date, "toDateTime": to_date}
    call_ids = []
    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            page_params = dict(params)
            while True:
                response = client.session.get(url, params=page_params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    calls = data.get("calls", [])
                    call_ids.extend(str(get_case_insensitive(call, "id", "")) for call in calls if validate_call_data(call))
                    cursor = data.get("pagination", {}).get("next")
                    if not cursor:
                        break
                    page_params["cursor"] = cursor
                    time.sleep(1)
                elif response.status_code in (401, 403):
                    raise GongAPIError(response.status_code, f"Authentication failed: {response.text or 'No response text'}")
                elif is_retryable_error(response.status_code):
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                    if response.status_code == 429:
                        error_message = response.text.lower() if response.text and isinstance(response.text, str) else "No response text"
                        if "daily limit" in error_message:
                            raise GongAPIError(429, "Daily API call limit (10,000) exceeded. Try again tomorrow.")
                    logger.warning(f"Retryable error {response.status_code}, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    raise GongAPIError(response.status_code, f"API error: {response.text or 'No response text'}")
            break
        except requests.RequestException as e:
            if attempt < max_attempts - 1:
                time.sleep((2 ** attempt) * 1)
            else:
                raise GongAPIError(0, f"Network error: {str(e)}")
    return call_ids

def fetch_call_details(client: GongAPIClient, call_ids: List[str]) -> List[Dict[str, Any]]:
    url = f"{client.base_url}/calls/extensive"
    call_details = []
    max_attempts = 3
    batch_size = 100

    for i in range(0, len(call_ids), batch_size):
        batch_ids = call_ids[i:i + batch_size]
        cursor = None
        while True:
            request_body = {
                "filter": {"callIds": batch_ids},
                "contentSelector": {
                    "context": "Extended",
                    "exposedFields": {
                        "parties": True,
                        "content": {"trackers": True, "brief": True, "keyPoints": True},
                        "media": True,
                        "crmAssociations": True
                    }
                }
            }
            if cursor:
                request_body["cursor"] = cursor

            for attempt in range(max_attempts):
                try:
                    response = client.session.post(url, json=request_body, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        calls = data.get("calls", [])
                        for call in calls:
                            if validate_call_data(call):
                                # Debug: Log the raw parties data to investigate missing jobTitle
                                call_id = get_case_insensitive(call.get("metaData", {}), "id", "unknown")
                                parties = call.get("parties", [])
                                logger.info(f"Parties data for call {call_id}: {parties}")
                                call_details.append(call)
                            else:
                                logger.warning(f"Invalid call data structure: {call.get('metaData', {}).get('id', 'unknown')}")
                        cursor = data.get("pagination", {}).get("next")
                        if not cursor:
                            break
                        time.sleep(1)
                        break
                    elif response.status_code in (401, 403):
                        raise GongAPIError(response.status_code, "Permission denied: Check API key permissions or required scopes.")
                    elif is_retryable_error(response.status_code):
                        wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                        if response.status_code == 429:
                            response_text = response.text if response.text is not None else ""
                            error_message = response_text.lower() if isinstance(response_text, str) else "No response text"
                            if "daily limit" in error_message:
                                raise GongAPIError(429, "Daily API call limit (10,000) exceeded. Try again tomorrow.")
                        logger.warning(f"Retryable error {response.status_code}, waiting {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise GongAPIError(response.status_code, f"API error: {response.text or 'No response text'}")
                except requests.RequestException as e:
                    if attempt < max_attempts - 1:
                        time.sleep((2 ** attempt) * 1)
                    else:
                        raise GongAPIError(0, f"Network error: {str(e)}")
            if not cursor:
                break
    return call_details

def fetch_transcript(client: GongAPIClient, call_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    url = f"{client.base_url}/calls/transcript"
    result = {}
    max_attempts = 3
    batch_size = 100

    for i in range(0, len(call_ids), batch_size):
        batch_ids = call_ids[i:i + batch_size]
        cursor = None
        while True:
            request_body = {"filter": {"callIds": batch_ids}}
            if cursor:
                request_body["cursor"] = cursor

            for attempt in range(max_attempts):
                try:
                    response = client.session.post(url, json=request_body, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        transcripts = data.get("callTranscripts", [])
                        for t in transcripts:
                            call_id = get_case_insensitive(t, "callId", "")
                            transcript_data = t.get("transcript", [])
                            if call_id and isinstance(transcript_data, list):
                                result[str(call_id)] = transcript_data
                            else:
                                logger.warning(f"Invalid transcript data for call {call_id or 'unknown'}")
                        cursor = data.get("pagination", {}).get("next")
                        if not cursor:
                            break
                        time.sleep(1)
                        break
                    elif response.status_code in (401, 403):
                        raise GongAPIError(response.status_code, "Permission denied: Check API key permissions or required scopes.")
                    elif is_retryable_error(response.status_code):
                        wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                        if response.status_code == 429 and response.text is not None:
                            error_message = response.text.lower() if isinstance(response.text, str) else "No response text"
                            if "daily limit" in error_message:
                                raise GongAPIError(429, "Daily API call limit (10,000) exceeded. Try again tomorrow.")
                        logger.warning(f"Retryable error {response.status_code}, waiting {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise GongAPIError(response.status_code, f"API error: {response.text}")
                except requests.RequestException as e:
                    if attempt < max_attempts - 1:
                        time.sleep((2 ** attempt) * 1)
                    else:
                        raise GongAPIError(0, f"Network error: {str(e)}")
            if not cursor:
                break
    return result

def apply_occupancy_analytics_tags(call: Dict[str, Any]) -> bool:
    fields = [
        get_case_insensitive(call.get("metaData", {}), "title", ""),
        get_case_insensitive(call.get("content", {}), "brief", ""),
        " ".join(str(km.get("description", "")) for km in call.get("content", {}).get("keyPoints", []))
    ]
    for field in fields:
        if field is None:
            continue
        for pattern in PRODUCT_MAPPINGS["Occupancy Analytics"]:
            if re.search(pattern, str(field).lower()):
                return True
    return False

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    max_retries = 3
    schema_version = detect_gong_schema_version(call_data)

    for attempt in range(max_retries):
        try:
            if not call_data or "metaData" not in call_data or not get_case_insensitive(call_data["metaData"], "id"):
                logger.warning("Skipping call with missing critical fields")
                return None

            processed_data = {
                "metaData": call_data.get("metaData", {}),
                "context": call_data.get("context", []),
                "content": call_data.get("content", {"trackers": [], "brief": "", "keyPoints": []}),
                "parties": call_data.get("parties", []),
                "utterances": transcript or [],
                "products": [],
                "other_topics": [],
                "account_industry": "",
                "opportunity_name": "",
                "partial_data": False
            }

            # Safely extract account data
            try:
                account_names = safely_extract_field_values(processed_data["context"], "Name", "Account")
                account_name = account_names[0] if account_names else "Unknown"
                account_ids = safely_extract_field_values(processed_data["context"], "objectId", "Account")
                account_id = account_ids[0] if account_ids else "Unknown"
                account_websites = safely_extract_field_values(processed_data["context"], "Website", "Account")
                account_website = account_websites[0] if account_websites else "Unknown"
                industries = safely_extract_field_values(processed_data["context"], "Industry", "Account")
                processed_data["account_industry"] = industries[0] if industries else ""
            except Exception as e:
                logger.warning(f"Error extracting account context: {str(e)}")
                account_name = "Unknown"
                account_id = "Unknown"
                account_website = "Unknown"
                processed_data["account_industry"] = ""

            # Safely extract opportunity data
            try:
                opportunity_names = safely_extract_field_values(processed_data["context"], "Name", "Opportunity")
                processed_data["opportunity_name"] = opportunity_names[0] if opportunity_names else ""
            except Exception as e:
                logger.warning(f"Error extracting opportunity context: {str(e)}")
                processed_data["opportunity_name"] = ""

            processed_data["account_name"] = account_name
            processed_data["account_id"] = account_id
            processed_data["account_website"] = account_website

            # Process trackers
            try:
                trackers = processed_data.get("content", {}).get("trackers", [])
                tracker_counts = {t.get("name", "").lower(): t.get("count", 0) for t in trackers if isinstance(t, dict) and t.get("name")}
                all_product_trackers = []
                for p in PRODUCT_MAPPINGS:
                    if p != "Occupancy Analytics":
                        all_product_trackers.extend(t.lower() for t in PRODUCT_MAPPINGS[p])

                for product, trackers_or_patterns in PRODUCT_MAPPINGS.items():
                    if product == "Occupancy Analytics":
                        if apply_occupancy_analytics_tags(call_data):
                            processed_data["products"].append(product)
                    else:
                        for tracker in trackers_or_patterns:
                            if tracker.lower() in tracker_counts and tracker_counts[tracker.lower()] > 0:
                                processed_data["products"].append(product)
                                break

                for tracker in trackers:
                    if not isinstance(tracker, dict):
                        continue
                    count = tracker.get("count", 0)
                    name = tracker.get("name", "")
                    if count > 0 and isinstance(name, str) and name.lower() not in all_product_trackers:
                        processed_data["other_topics"].append({"name": name, "count": count})
            except Exception as e:
                logger.warning(f"Error processing trackers: {str(e)}")
                processed_data["products"] = []
                processed_data["other_topics"] = []

            return processed_data

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed to normalize call {call_data.get('metaData', {}).get('id', 'unknown')}: {str(e)}")
            if attempt == max_retries - 1:
                try:
                    # Fallback: Extract minimal data
                    minimal_data = {
                        "metaData": {"id": get_case_insensitive(call_data.get("metaData", {}), "id", "")},
                        "context": [],
                        "content": {"trackers": [], "brief": "", "keyPoints": []},
                        "parties": [],
                        "utterances": [],
                        "products": [],
                        "other_topics": [],
                        "account_name": "Unknown",
                        "account_id": "Unknown",
                        "account_website": "Unknown",
                        "account_industry": "",
                        "opportunity_name": "",
                        "partial_data": True
                    }
                    return minimal_data
                except Exception as final_e:
                    logger.error(f"Failed to extract minimal data for call {call_data.get('metaData', {}).get('id', 'unknown')}: {str(final_e)}")
                    return None
            time.sleep(1)

def format_speaker(speaker: Dict[str, Any]) -> str:
    name = get_case_insensitive(speaker, "name", "").strip()
    title = get_case_insensitive(speaker, "jobTitle", "").strip()
    if name and title:
        return f"{name}, {title}"
    return name or title or ""

def get_primary_speakers(call: Dict[str, Any]) -> Tuple[str, str, str]:
    speaker_counts = {}
    for utterance in safely_get_utterances(call):
        speaker_id = get_case_insensitive(utterance, "speakerId", "")
        if speaker_id:
            speaker_counts[speaker_id] = speaker_counts.get(speaker_id, 0) + 1

    internal_speaker = external_speaker = unknown_speaker = ""
    max_internal = max_external = max_unknown = 0

    for party in call.get("parties", []):
        speaker_id = get_case_insensitive(party, "speakerId", "")
        if speaker_id not in speaker_counts:
            continue
        count = speaker_counts[speaker_id]
        affiliation = get_case_insensitive(party, "affiliation", "unknown")
        affiliation_lower = affiliation.lower() if isinstance(affiliation, str) else "unknown"

        if affiliation_lower == "internal" and count > max_internal:
            internal_speaker = format_speaker(party)
            max_internal = count
        elif affiliation_lower == "external" and count > max_external:
            external_speaker = format_speaker(party)
            max_external = count
        elif affiliation_lower == "unknown" and count > max_unknown:
            unknown_speaker = format_speaker(party)
            max_unknown = count

    return internal_speaker, external_speaker, unknown_speaker

def prepare_call_summary_df(calls: List[Dict[str, Any]], selected_products: List[str]) -> pd.DataFrame:
    data = []
    for call in calls:
        if not call or "metaData" not in call:
            continue

        call_id = f'"{str(get_case_insensitive(call["metaData"], "id", ""))}"'
        call_date = convert_to_sf_time(get_case_insensitive(call["metaData"], "started", ""))
        call_title = get_case_insensitive(call["metaData"], "title", "N/A")
        call_summary = get_case_insensitive(call.get("content", {}), "brief", "N/A")
        key_points = "; ".join(str(kp.get("description", "")) for kp in call.get("content", {}).get("keyPoints", []) if isinstance(kp, dict)) or "N/A"
        products = sorted(set(p for p in call.get("products", []) if p in ALL_PRODUCT_TAGS))
        products_str = "|".join(products) if products else "none"
        other_topics = sorted(call.get("other_topics", []), key=lambda x: x["count"], reverse=True)
        other_topics_str = "|".join(t["name"] for t in other_topics) if other_topics else "none"
        internal_speaker, external_speaker, unknown_speaker = get_primary_speakers(call)

        filtered_out = "yes" if products and not any(p in selected_products for p in products) else "no"

        data.append({
            "call_id": call_id,
            "call_date": call_date,
            "call_title": call_title,
            "call_summary": call_summary,
            "key_points": key_points,
            "filtered_out": filtered_out,
            "product_tags": products_str,
            "other_topics": other_topics_str,
            "primary_internal_speaker": internal_speaker,
            "primary_external_speaker": external_speaker,
            "primary_unknown_speaker": unknown_speaker,
            "account_name": call.get("account_name", "N/A"),
            "account_website": call.get("account_website", "N/A"),
            "account_industry": call.get("account_industry", "")
        })

    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values("call_date", ascending=False)
    return df

def prepare_utterances_df(calls: List[Dict[str, Any]], selected_products: List[str]) -> pd.DataFrame:
    data = []
    for call in calls:
        if not call or "metaData" not in call:
            continue

        call_id = f'"{str(get_case_insensitive(call["metaData"], "id", ""))}"'
        call_title = get_case_insensitive(call["metaData"], "title", "N/A")
        call_date = convert_to_sf_time(get_case_insensitive(call["metaData"], "started", ""))
        account_id = call.get("account_id", "N/A")
        account_name = call.get("account_name", "N/A")
        account_website = call.get("account_website", "N/A")
        account_industry = call.get("account_industry", "")
        products = sorted(set(p for p in call.get("products", []) if p in ALL_PRODUCT_TAGS))
        products_str = "|".join(products) if products else "none"

        if products and not any(p in selected_products for p in products):
            continue

        speaker_info = {get_case_insensitive(p, "speakerId", ""): p for p in call.get("parties", [])}
        for utterance in sorted(safely_get_utterances(call), key=lambda x: x.get("sentences", [{}])[0].get("start", 0)):
            sentences = utterance.get("sentences", [])
            if not sentences:
                continue

            text = " ".join(s.get("text", "") for s in sentences)
            if len(text.split()) <= 5:
                continue

            speaker_id = get_case_insensitive(utterance, "speakerId", "")
            speaker = speaker_info.get(speaker_id, {})
            affiliation = get_case_insensitive(speaker, "affiliation", "unknown")
            affiliation_lower = affiliation.lower() if isinstance(affiliation, str) else "unknown"
            if affiliation_lower == "internal":
                continue

            topic = get_case_insensitive(utterance, "topic", "N/A")
            if isinstance(topic, str) and topic.lower() in ["call setup", "small talk"]:
                continue

            data.append({
                "call_id": call_id,
                "call_title": call_title,
                "call_date": call_date,
                "account_id": account_id,
                "account_name": account_name,
                "account_website": account_website,
                "account_industry": account_industry,
                "products": products_str,
                "speaker_name": get_case_insensitive(speaker, "name", "Unknown"),
                "speaker_job_title": get_case_insensitive(speaker, "jobTitle", ""),
                "speaker_affiliation": affiliation_lower,
                "speaker_email_address": get_case_insensitive(speaker, "emailAddress", ""),
                "utterance_text": text,
                "topic": topic
            })

    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values(["call_date", "call_id"], ascending=[False, True])
    return df

def prepare_json_output(calls: List[Dict[str, Any]], selected_products: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    filtered_calls = []
    non_filtered_calls = []

    for call in sorted(calls, key=lambda x: get_case_insensitive(x.get("metaData", {}), "started", ""), reverse=True):
        if not call or "metaData" not in call:
            continue

        call_id = f'"{str(get_case_insensitive(call["metaData"], "id", ""))}"'
        call_date = convert_to_sf_time(get_case_insensitive(call["metaData"], "started", ""))
        products = sorted(set(p for p in call.get("products", []) if p in ALL_PRODUCT_TAGS))
        
        if products and not any(p in selected_products for p in products):
            is_filtered = False
        else:
            is_filtered = True

        call_data = {
            "call_id": call_id,
            "call_title": get_case_insensitive(call["metaData"], "title", "N/A"),
            "call_date": call_date,
            "call_duration": (get_case_insensitive(call["metaData"], "duration", 0) // 60000),
            "call_summary": get_case_insensitive(call.get("content", {}), "brief", "N/A"),
            "key_points": "; ".join(str(kp.get("description", "")) for kp in call.get("content", {}).get("keyPoints", []) if isinstance(kp, dict)) or "N/A",
            "product_tags": "|".join(products) if products else "none",
            "other_topics": "|".join(t["name"] for t in sorted(call.get("other_topics", []), key=lambda x: x["count"], reverse=True)) or "none",
            "account_name": call.get("account_name", "N/A"),
            "account_website": call.get("account_website", "N/A"),
            "account_industry": call.get("account_industry", ""),
            "opportunity_name": call.get("opportunity_name", ""),
            "utterances": []
        }

        int_spk, ext_spk, unk_spk = get_primary_speakers(call)
        call_data["primary_internal_speaker"] = int_spk
        call_data["primary_external_speaker"] = ext_spk
        call_data["primary_unknown_speaker"] = unk_spk

        speaker_info = {get_case_insensitive(p, "speakerId", ""): p for p in call.get("parties", [])}
        for utterance in sorted(safely_get_utterances(call), key=lambda x: x.get("sentences", [{}])[0].get("start", 0)):
            sentences = utterance.get("sentences", [])
            if not sentences:
                continue

            text = " ".join(s.get("text", "") for s in sentences)
            speaker_id = get_case_insensitive(utterance, "speakerId", "")
            speaker = speaker_info.get(speaker_id, {})
            start_time = sentences[0].get("start", 0)
            end_time = sentences[-1].get("end", 0)
            affiliation = get_case_insensitive(speaker, "affiliation", "unknown")
            affiliation_lower = affiliation.lower() if isinstance(affiliation, str) else "unknown"

            call_data["utterances"].append({
                "timestamp": (start_time // 60000),
                "speaker_name": get_case_insensitive(speaker, "name", "Unknown"),
                "speaker_title": get_case_insensitive(speaker, "jobTitle", ""),
                "speaker_affiliation": affiliation_lower,
                "speaker_email": get_case_insensitive(speaker, "emailAddress", ""),
                "utterance_text": text,
                "topic": get_case_insensitive(utterance, "topic", "N/A")
            })

        if is_filtered:
            filtered_calls.append(call_data)
        else:
            non_filtered_calls.append(call_data)

    return {"filtered_calls": filtered_calls, "non_filtered_calls": non_filtered_calls}

@app.route('/')
def index():
    start_date = (date.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = date.today().strftime('%Y-%m-%d')
    return render_template('index.html', start_date=start_date, end_date=end_date, products=ALL_PRODUCT_TAGS, access_key="", secret_key="")

@app.route('/process', methods=['POST'])
def process():
    access_key = request.form.get('access_key', '')
    secret_key = request.form.get('secret_key', '')
    products = request.form.getlist('products')

    form_state = {
        "access_key": access_key,
        "secret_key": secret_key,
        "products": products if products else ALL_PRODUCT_TAGS
    }

    try:
        start_date = datetime.strptime(request.form['start_date'], '%Y-%m-%d').date()
        end_date = datetime.strptime(request.form['end_date'], '%Y-%m-%d').date()
    except ValueError:
        form_state["start_date"] = request.form.get('start_date', (date.today() - timedelta(days=7)).strftime('%Y-%m-%d'))
        form_state["end_date"] = request.form.get('end_date', date.today().strftime('%Y-%m-%d'))
        return render_template('index.html', message="Invalid date format. Use YYYY-MM-DD.", **form_state)

    form_state["start_date"] = start_date.strftime('%Y-%m-%d')
    form_state["end_date"] = end_date.strftime('%Y-%m-%d')

    if not access_key or not secret_key:
        return render_template('index.html', message="Please provide both Gong Access Key and Secret Key.", **form_state)

    if start_date > end_date:
        return render_template('index.html', message="Start date cannot be after end date.", **form_state)

    try:
        client = GongAPIClient(access_key, secret_key)
        if not client.verify_api_compatibility():
            return render_template('index.html', message="Gong API version incompatibility detected.", **form_state)

        call_ids = fetch_call_list(
            client,
            start_date.isoformat() + "T00:00:00Z",
            end_date.isoformat() + "T23:59:59Z"
        )

        if not call_ids:
            return render_template('index.html', message="No calls found for the selected date range.", **form_state)

        details = fetch_call_details(client, call_ids)
        transcripts = fetch_transcript(client, call_ids)

        full_data = []
        dropped_calls = 0
        for call in details:
            call_id = str(get_case_insensitive(call.get("metaData", {}), "id", ""))
            if not call_id:
                dropped_calls += 1
                continue

            normalized_data = normalize_call_data(call, transcripts.get(call_id, []))
            if normalized_data:
                full_data.append(normalized_data)
            else:
                dropped_calls += 1

        if dropped_calls > 0:
            message = f"Dropped {dropped_calls} calls ({(dropped_calls/len(call_ids)*100):.1f}%) due to data issues."
        else:
            message = ""

        if not full_data:
            return render_template('index.html', message="No valid call data retrieved.", **form_state)

        utterances_df = prepare_utterances_df(full_data, products)
        call_summary_df = prepare_call_summary_df(full_data, products)
        json_data = prepare_json_output(full_data, products)

        start_date_str = start_date.strftime("%d%b%y").lower()
        end_date_str = end_date.strftime("%d%b%y").lower()

        # Use temporary files for storage
        temp_dir = tempfile.mkdtemp()
        session['temp_dir'] = temp_dir
        session['utterances_path'] = os.path.join(temp_dir, f"filtered_utterances_gong_{start_date_str}_to_{end_date_str}.csv")
        session['call_summary_path'] = os.path.join(temp_dir, f"call_summary_gong_{start_date_str}_to_{end_date_str}.csv")
        session['json_path'] = os.path.join(temp_dir, f"call_data_gong_{start_date_str}_to_{end_date_str}.json")

        utterances_df.to_csv(session['utterances_path'], index=False)
        call_summary_df.to_csv(session['call_summary_path'], index=False)
        with open(session['json_path'], 'w') as f:
            json.dump(json_data, f, indent=2)

        message += f"\nTotal calls processed: {len(full_data)}\nFiltered utterances: {len(utterances_df)}"
        return render_template('index.html', message=message, show_download=True, **form_state)

    except GongAPIError as e:
        return render_template('index.html', message=f"API Error: {e.message}", **form_state)
    except Exception as e:
        logger.exception("Unexpected error in process")
        return render_template('index.html', message=f"Unexpected error: {str(e)}", **form_state)

@app.route('/download/utterances')
def download_utterances():
    if 'utterances_path' not in session:
        return "Session expired", 400
    return send_file(
        session['utterances_path'],
        mimetype='text/csv',
        as_attachment=True,
        download_name=os.path.basename(session['utterances_path'])
    )

@app.route('/download/call_summary')
def download_call_summary():
    if 'call_summary_path' not in session:
        return "Session expired", 400
    return send_file(
        session['call_summary_path'],
        mimetype='text/csv',
        as_attachment=True,
        download_name=os.path.basename(session['call_summary_path'])
    )

@app.route('/download/json')
def download_json():
    if 'json_path' not in session:
        return "Session expired", 400
    return send_file(
        session['json_path'],
        mimetype='application/json',
        as_attachment=True,
        download_name=os.path.basename(session['json_path'])
    )

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000, debug=True)