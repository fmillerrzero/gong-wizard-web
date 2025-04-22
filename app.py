import base64
import json
import logging
import os
import re
import time
import glob
from datetime import datetime, timedelta
from io import StringIO
import csv
import pandas as pd
import pytz
import requests
from flask import Flask, render_template, request, send_file

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', os.urandom(24))
logger = logging.getLogger(__name__)

# Configure logging
log_dir = "/tmp"
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "app.log")
logging.basicConfig(
    level=logging.DEBUG if os.environ.get('FLASK_DEBUG', 'False').lower() == 'true' else logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

logger.info("Starting Gong Wizard Web Flask - Version 2025-04-21")
logger.info("Application startup initiated")

# Constants
GONG_BASE_URL = "https://us-11211.api.gong.io"
SF_TZ = pytz.timezone('America/Los_Angeles')
TARGET_DOMAINS = set()
TENANT_DOMAINS = set()
OUTPUT_DIR = "/tmp/gong_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
logger.info(f"Output directory created: {OUTPUT_DIR}")
PATHS_FILE = os.path.join(OUTPUT_DIR, "file_paths.json")
BATCH_SIZE = 50
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
INTERNAL_DOMAINS = {"secureaire.com", "rzero.com", "rzerosystems.com"}

# Precompile regex patterns for Occupancy Analytics with case-insensitive flag
for product in PRODUCT_MAPPINGS:
    if product == "Occupancy Analytics":
        PRODUCT_MAPPINGS[product] = [re.compile(pattern, re.IGNORECASE) for pattern in PRODUCT_MAPPINGS[product]]

def safe_operation(operation, default_value=None, log_message=None, *args, **kwargs):
    try:
        return operation(*args, **kwargs)
    except Exception as e:
        if log_message:
            logger.error(f"{log_message}: {str(e)}")
        return default_value

def normalize_domain(url):
    if not url or url in ["N/A", "Unknown"]:
        return ""
    domain = re.sub(r'^https?://', '', str(url).lower(), flags=re.IGNORECASE)
    domain = re.sub(r'^www\.', '', domain, flags=re.IGNORECASE)
    domain = domain.split('/')[0]
    return domain.strip()

def get_email_domain(email):
    if not email or "@" not in email:
        return ""
    domain = email.split("@")[-1].lower().strip()
    return domain

def load_domains_from_sheet(sheet_id, target_set, label):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    response = safe_operation(
        requests.get, None, f"Failed to fetch {label} Google Sheet", url, timeout=10
    )
    if response and response.status_code == 200:
        df = safe_operation(
            pd.read_csv, None, "Failed to read CSV", StringIO(response.text)
        )
        if df is not None:
            domains_list = df.iloc[:, 0].dropna().astype(str).tolist()
            for domain in domains_list:
                normalized = normalize_domain(domain)
                if normalized:
                    target_set.add(normalized)
            logger.info(f"Loaded {len(target_set)} {label} domains")
    else:
        logger.warning(f"Continuing without {label} domains")

def cleanup_old_files():
    now = time.time()
    for file_path in glob.glob(os.path.join(OUTPUT_DIR, "*")):
        if file_path == PATHS_FILE:
            continue
        if os.path.isfile(file_path) and (now - os.path.getmtime(file_path)) > 3600:
            try:
                os.remove(file_path)
                logger.info(f"Removed old file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing old file {file_path}: {str(e)}")

def save_file_paths(paths):
    try:
        with open(PATHS_FILE, 'w') as f:
            json.dump(paths, f)
        logger.info(f"Saved file paths to {PATHS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save file paths to {PATHS_FILE}: {str(e)}")

def load_file_paths():
    if not os.path.exists(PATHS_FILE):
        logger.error(f"Paths file not found: {PATHS_FILE}")
        return {}
    try:
        with open(PATHS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading paths file {PATHS_FILE}: {str(e)}")
        return {}

_initialization_done = False

@app.before_request
def initialize():
    global TARGET_DOMAINS, TENANT_DOMAINS, _initialization_done
    if not _initialization_done:
        logger.info("Starting initialization of domains")
        try:
            load_domains_from_sheet("1HMAQ3eNhXhCAfcxPqQwds1qn1ZW8j6Sc1oCM9_TLjtQ", TARGET_DOMAINS, "owner")
            load_domains_from_sheet("19WrPxtEZV59_irXRm36TJGRNJFRoYsi0KnrOUDIDBVM", TENANT_DOMAINS, "tenant")
            logger.info(f"Initialized with {len(TARGET_DOMAINS)} owner domains and {len(TENANT_DOMAINS)} tenant domains")
            cleanup_old_files()
            _initialization_done = True
            logger.info("Initialization completed successfully")
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

class GongAPIError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Gong API Error {status_code}: {message}")

class GongAPIClient:
    def __init__(self, access_key, secret_key):
        self.base_url = GONG_BASE_URL
        self.session = requests.Session()
        credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
        self.session.headers.update({"Authorization": f"Basic {credentials}"})

    def api_call(self, method, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint}"
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = self.session.request(method, url, **kwargs, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (401, 403):
                    raise GongAPIError(response.status_code, "Authentication failed")
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                    logger.warning(f"Rate limit hit, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    raise GongAPIError(response.status_code, f"API error: {response.text}")
            except requests.RequestException as e:
                if attempt == max_attempts - 1:
                    raise GongAPIError(0, f"Network error: {str(e)}")
                time.sleep(2 ** attempt)
        raise GongAPIError(429, "Max retries exceeded")

    def fetch_call_list(self, from_date, to_date):
        endpoint = "/v2/calls"
        call_ids = []
        cursor = None
        while True:
            params = {
                "fromDateTime": from_date,
                "toDateTime": to_date,
                "cursor": cursor
            }
            response = self.api_call("GET", endpoint, params=params)
            calls = response.get("calls", [])
            call_ids.extend([str(call.get("id")) for call in calls])
            records = response.get("records", {})
            logger.info(f"Page info: totalRecords={records.get('totalRecords')}, currentPageSize={records.get('currentPageSize')}, cursor={cursor}")
            cursor = records.get("cursor")
            if not cursor:
                break
        logger.info(f"Fetched {len(call_ids)} call IDs")
        return call_ids

    def fetch_call_details(self, call_ids):
        endpoint = "/v2/calls/extensive"
        data = {
            "filter": {
                "callIds": call_ids
            },
            "contentSelector": {
                "exposedFields": {
                    "parties": True,
                    "content": {
                        "trackers": True,
                        "trackerOccurrences": True,
                        "brief": True,
                        "keyPoints": True
                    },
                    "collaboration": {
                        "publicComments": True
                    }
                },
                "context": "Extended"
            }
        }
        response = self.api_call("POST", endpoint, json=data)
        records = response.get("records", {})
        logger.info(f"Call details page info: totalRecords={records.get('totalRecords')}, currentPageSize={records.get('currentPageSize')}")
        for call in response.get("calls", []):
            yield call

    def fetch_transcript(self, call_ids, max_attempts=3):
        endpoint = "/v2/calls/transcript"
        result = {}
        cursor = None
        while True:
            request_body = {"filter": {"callIds": call_ids}, "cursor": cursor}
            data = self.api_call("POST", endpoint, json=request_body)
            transcripts = data.get("callTranscripts", [])
            for t in transcripts:
                if t.get("callId"):
                    call_id = str(t["callId"])
                    result[call_id] = t.get("transcript", [])
            cursor = data.get("records", {}).get("cursor")
            if not cursor:
                logger.info(f"Fetched transcripts for {len(result)} calls")
                break
        return result

def convert_to_sf_time(utc_time):
    if not utc_time:
        return "N/A"
    try:
        if utc_time.endswith('Z'):
            utc_time = utc_time.replace("Z", "+00:00")
        utc_dt = datetime.fromisoformat(utc_time)
        sf_dt = utc_dt.astimezone(SF_TZ)
        return sf_dt.strftime("%m/%d/%y")
    except ValueError as e:
        logger.error(f"Date conversion error for {utc_time}: {str(e)}")
        return "N/A"

def get_field(data, key, default="Unknown"):
    if not isinstance(data, dict):
        return default
    for k, v in data.items():
        if k.lower() == key.lower():
            return v if v is not None else default
    return default

def extract_field_values(context, field_name, object_type=None):
    values = []
    for ctx in context or []:
        for obj in ctx.get("objects", []):
            obj_type = get_field(obj, "objectType", "Unknown")
            if object_type and obj_type.lower() != object_type.lower():
                continue
            if field_name.lower() == "objectid":
                value = get_field(obj, "objectId", "Unknown")
                if value != "Unknown":
                    values.append(str(value))
                continue
            for field in obj.get("fields", []):
                if not isinstance(field, dict):
                    continue
                field_name_val = get_field(field, "name", "Unknown")
                if field_name_val.lower() == field_name.lower():
                    value = get_field(field, "value", "Unknown")
                    if value != "Unknown":
                        values.append(str(value))
    return values

def apply_occupancy_analytics_tags(call):
    fields = [
        get_field(call.get("metaData", {}), "title"),
        get_field(call.get("content", {}), "brief"),
        " ".join(str(get_field(kp, "description")) for kp in call.get("content", {}).get("keyPoints", []) if isinstance(kp, dict))
    ]
    text = " ".join(f for f in fields if f).lower()
    matches = [pattern.pattern for pattern in PRODUCT_MAPPINGS["Occupancy Analytics"] if pattern.search(text)]
    return bool(matches)

def normalize_call_data(call, transcript):
    try:
        meta_data = call.get("metaData", {})
        content = call.get("content", {})
        parties = call.get("parties", [])
        context = call.get("context", [])

        call_id = get_field(meta_data, "id", "Unknown")
        call_title = get_field(meta_data, "title", "Unknown")
        call_date = convert_to_sf_time(get_field(meta_data, "started"))
        account_ids = extract_field_values(context, "objectId", "Account")
        account_name = extract_field_values(context, "Name", "Account")[0] if extract_field_values(context, "Name", "Account") else "Unknown"
        account_id = account_ids[0] if account_ids else "Unknown"
        account_website = extract_field_values(context, "Website", "Account")[0] if extract_field_values(context, "Website", "Account") else "Unknown"
        account_industry = extract_field_values(context, "Industry", "Account")[0] if extract_field_values(context, "Industry", "Account") else "Unknown"

        trackers = content.get("trackers", [])
        tracker_counts = {get_field(t, "name").lower(): get_field(t, "count", 0) for t in trackers if get_field(t, "name")}

        products = []
        for product in PRODUCT_MAPPINGS:
            if product == "Occupancy Analytics":
                if apply_occupancy_analytics_tags(call):
                    products.append(product)
            else:
                for tracker in PRODUCT_MAPPINGS[product]:
                    if tracker_counts.get(tracker.lower(), 0) > 0:
                        products.append(product)
                        break

        tracker_occurrences = []
        for tracker in trackers:
            tracker_name = get_field(tracker, "name", "Unknown")
            for occurrence in tracker.get("occurrences", []):
                tracker_occurrences.append({
                    "tracker_name": tracker_name,
                    "phrase": get_field(occurrence, "phrase", "Unknown"),
                    "start": get_field(occurrence, "startTime", 0),
                    "speakerId": get_field(occurrence, "speakerId", "Unknown")
                })

        call_summary = get_field(content, "brief", "Unknown")
        key_points = []
        key_points_raw = content.get("keyPoints", [])
        for kp in key_points_raw:
            if not isinstance(kp, dict):
                continue
            description = get_field(kp, "description", "Unknown")
            if description != "Unknown":
                key_points.append(description.strip())
        key_points_str = "|".join(key_points) if key_points else "Unknown"

        normalized_website = normalize_domain(account_website)
        org_type = "other"
        if normalized_website in TARGET_DOMAINS:
            org_type = "owner"
        elif normalized_website in TENANT_DOMAINS:
            org_type = "tenant"

        utterances = transcript if transcript is not None else []

        return {
            "call_id": f"'{call_id}",
            "call_title": call_title,
            "call_date": call_date,
            "account_name": account_name,
            "account_id": account_id,
            "account_website": account_website,
            "account_industry": account_industry,
            "products": products,
            "parties": parties,
            "utterances": utterances,
            "partial_data": False,
            "org_type": org_type,
            "tracker_occurrences": tracker_occurrences,
            "call_summary": call_summary,
            "key_points": key_points_str
        }
    except Exception as e:
        call_id = get_field(call.get("metaData", {}), "id", "Unknown")
        logger.error(f"Normalization error for call '{call_id}': {str(e)}")
        return {
            "call_id": f"'{call_id}",
            "call_title": "Unknown",
            "call_date": "N/A",
            "account_name": "Unknown",
            "account_id": "Unknown",
            "account_website": "Unknown",
            "account_industry": "Unknown",
            "products": [],
            "parties": call.get("parties", []),
            "utterances": [],
            "partial_data": True,
            "org_type": "Unknown",
            "tracker_occurrences": [],
            "call_summary": "Unknown",
            "key_points": "Unknown"
        }

def prepare_utterances_df(calls, selected_products):
    if not calls:
        logger.info("No calls to process for utterances DataFrame")
        return pd.DataFrame()
    
    data = []
    call_tracker_map = {}
    selected_products_lower = [p.lower() for p in selected_products]
    excluded_topics = {"call setup", "small talk", "wrap-up"}
    
    for call in calls:
        call_id = call["call_id"]
        products = call.get("products", [])
        
        if not products:
            logger.debug(f"Call {call_id}: No products assigned, skipping")
            continue
            
        products_lower = [p.lower() for p in products if isinstance(p, str)]
        
        if not any(p in selected_products_lower for p in products_lower):
            logger.debug(f"Call {call_id}: Products {products} don't match selection {selected_products}, skipping")
            continue
        
        logger.debug(f"Call {call_id}: Products assigned: {products}, Selected products: {selected_products}")
        
        utterances = sorted(call["utterances"] or [], key=lambda x: get_field(x, "start", 0))
        if not utterances:
            logger.debug(f"Call {call_id}: No utterances found, skipping")
            continue
        
        call_tracker_map[call_id] = {}
        for utterance in utterances:
            start_time = safe_operation(
                lambda: float(get_field(utterance, "start", 0)), 0, f"Invalid utterance start time for call {call_id}"
            )
            utterance_key = f"{call_id}_{start_time}"
            call_tracker_map[call_id][utterance_key] = {"trackers": []}
        
        for tracker in call.get("tracker_occurrences", []):
            tracker_start = safe_operation(
                lambda: float(get_field(tracker, "start", 0)), 0, f"Invalid tracker start time for call {call_id}"
            )
            tracker_time = tracker_start * 1000 if tracker_start < 1000 else tracker_start
            
            matched = False
            for utterance_key, info in call_tracker_map[call_id].items():
                utterance_start = float(utterance_key.split('_')[-1])
                utterance_end = utterance_start + 60000
                if utterance_start <= tracker_time <= utterance_end:
                    info["trackers"].append({
                        "tracker_name": get_field(tracker, "tracker_name", "Unknown"),
                        "phrase": get_field(tracker, "phrase", "Unknown")
                    })
                    matched = True
                    break
            
            if not matched:
                closest_utterance = None
                min_distance = float('inf')
                for utterance_key, info in call_tracker_map[call_id].items():
                    utterance_start = float(utterance_key.split('_')[-1])
                    distance = abs(utterance_start - tracker_time)
                    if distance <= 10000 and distance < min_distance:
                        min_distance = distance
                        closest_utterance = utterance_key
                if closest_utterance:
                    call_tracker_map[call_id][closest_utterance]["trackers"].append({
                        "tracker_name": get_field(tracker, "tracker_name", "Unknown"),
                        "phrase": get_field(tracker, "phrase", "Unknown")
                    })
        
        speaker_info = {get_field(p, "speakerId", "Unknown"): p for p in call["parties"]}
        
        for utterance in utterances:
            if "sentences" in utterance:
                text = " ".join(s.get("text", "") if isinstance(s, dict) else "" for s in utterance.get("sentences", []))
            elif "text" in utterance:
                text = utterance.get("text", "")
            else:
                logger.warning(f"Call {call_id}: Unexpected utterance structure: {list(utterance.keys())}")
                text = str(utterance)
            
            speaker_id = get_field(utterance, "speakerId", "Unknown")
            speaker = speaker_info.get(speaker_id, {})
            speaker_name = get_field(speaker, "name", "Unknown")
            original_affiliation = get_field(speaker, "affiliation", "unknown").lower()
            speaker_email_address = get_field(speaker, "emailAddress", "Unknown")
            email_domain = get_email_domain(speaker_email_address)
            if any(internal_domain == email_domain or email_domain.endswith("." + internal_domain) for internal_domain in INTERNAL_DOMAINS):
                speaker_affiliation = "internal"
                if original_affiliation != "internal":
                    logger.info(f"Overrode affiliation from {original_affiliation} to internal for speaker {speaker_name} ({speaker_email_address}) in call {call_id}")
            else:
                speaker_affiliation = original_affiliation
            
            if speaker_affiliation == "internal":
                continue
            
            speaker_job_title = get_field(speaker, "title", "Unknown")
            if speaker_job_title == "Unknown":
                logger.debug(f"Missing job title for speaker {speaker_name} in call {call_id}")
            
            topic = get_field(utterance, "topic", "Unknown").lower()
            
            if topic in excluded_topics:
                continue
            
            if len(text.split()) < 8:
                continue
            
            utterance_start = safe_operation(
                lambda: float(get_field(utterance, "start", 0)), 0, f"Invalid utterance start time for call {call_id}"
            )
            utterance_key = f"{call_id}_{utterance_start}"
            triggered_trackers = call_tracker_map.get(call_id, {}).get(utterance_key, {"trackers": []})["trackers"]
            
            tracker_names = [t["tracker_name"] for t in triggered_trackers]
            tracker_phrases = [t["phrase"] for t in triggered_trackers]
            
            data.append({
                "call_id": call_id,
                "call_date": call["call_date"],
                "account_name": call["account_name"],
                "account_industry": call["account_industry"],
                "org_type": call["org_type"],
                "speaker_name": speaker_name,
                "speaker_job_title": speaker_job_title,
                "speaker_affiliation": speaker_affiliation,
                "speaker_email_address": speaker_email_address,
                "sales_topic": topic,
                "product": "|".join(products) if products else "N/A",
                "tracker": "|".join(tracker_names) if tracker_names else "",
                "keyword": "|".join(tracker_phrases) if tracker_phrases else "",
                "utterance_text": text
            })
    
    if data:
        columns = [
            "call_id", "call_date", "account_name", "account_industry", "org_type",
            "speaker_name", "speaker_job_title", "speaker_affiliation", "speaker_email_address",
            "sales_topic", "product", "tracker", "keyword", "utterance_text"
        ]
        df = pd.DataFrame(data)[columns]
        df['call_id'] = df['call_id'].astype(str)
        df = df.sort_values(["call_date", "call_id"], ascending=[False, True])
        logger.info(f"Utterances DataFrame: {len(df)} rows, columns: {df.columns.tolist()}")
    else:
        logger.info("Utterances DataFrame is empty after processing")
        df = pd.DataFrame()
    
    return df

def save_utterances_to_csv(df, path):
    if df.empty:
        logger.warning("Cannot save empty DataFrame to CSV")
        return
        
    df['call_id'] = df['call_id'].astype(str)
    df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logger.info(f"Saved utterances DataFrame to {path}")

def prepare_call_summary_df(calls, selected_products):
    if not calls:
        logger.info("No calls to process for call summary DataFrame")
        return pd.DataFrame()
    
    data = []
    selected_products_lower = [p.lower() for p in selected_products]
    for call in calls:
        products = call.get("products", [])
        products_lower = [p.lower() for p in products if isinstance(p, str)]
        if not products:
            filtered_out = "no product tags"
        elif not any(p in selected_products_lower for p in products_lower):
            filtered_out = "no matching product"
        else:
            filtered_out = "included"
        
        data.append({
            "call_id": call["call_id"],
            "call_date": call["call_date"],
            "filtered_out": filtered_out,
            "product_tags": "|".join(products) if products else "",
            "org_type": call["org_type"],
            "account_name": call["account_name"],
            "account_website": call["account_website"],
            "account_industry": call["account_industry"],
            "call_summary": call.get("call_summary", "Unknown"),
            "key_points": call.get("key_points", "Unknown")
        })
    
    df = pd.DataFrame(data)
    if not df.empty:
        df['call_id'] = df['call_id'].astype(str)
        df = df.sort_values("call_date", ascending=False)
        logger.info(f"Call summary DataFrame: {len(df)} rows, columns: {df.columns.tolist()}")
    else:
        logger.info("Call summary DataFrame is empty after processing")
    
    return df

def prepare_json_output(calls, selected_products):
    if not calls:
        logger.info("No calls to process for JSON output")
        return {"filtered_calls": [], "non_filtered_calls": []}
    
    filtered_calls = []
    non_filtered_calls = []
    selected_products_lower = [p.lower() for p in selected_products]
    for call in calls:
        products = call.get("products", [])
        products_lower = [p.lower() for p in products if isinstance(p, str)]
        speaker_info = {get_field(p, "speakerId", "Unknown"): p for p in call["parties"]}
        call_data = {
            "call_id": call["call_id"],
            "call_date": call["call_date"],
            "product_tags": "|".join(products) if products else "",
            "org_type": call["org_type"],
            "account_name": call["call_name"],
            "account_website": call["account_website"],
            "account_industry": call["account_industry"],
            "utterances": [
                {
                    "timestamp": get_field(u, "start", "N/A"),
                    "speaker_name": get_field(speaker_info.get(get_field(u, "speakerId", "Unknown"), {}), "name", "Unknown"),
                    "speaker_affiliation": get_field(speaker_info.get(get_field(u, "speakerId", "Unknown"), {}), "affiliation", "Unknown"),
                    "utterance_text": " ".join(s.get("text", "") if isinstance(s, dict) else "" for s in u.get("sentences", [])),
                    "sales_topic": get_field(u, "topic", "Unknown")
                } for u in sorted(call["utterances"] or [], key=lambda x: get_field(x, "start", 0))
            ]
        }
        if products and any(p in selected_products_lower for p in products_lower):
            filtered_calls.append(call_data)
        else:
            non_filtered_calls.append(call_data)
    
    filtered_calls = sorted(filtered_calls, key=lambda x: datetime.strptime(x["call_date"], "%m/%d/%y") if x["call_date"] != "N/A" else datetime.min, reverse=True)
    non_filtered_calls = sorted(non_filtered_calls, key=lambda x: datetime.strptime(x["call_date"], "%m/%d/%y") if x["call_date"] != "N/A" else datetime.min, reverse=True)
    logger.info(f"JSON output: {len(filtered_calls)} filtered, {len(non_filtered_calls)} non-filtered calls")
    return {"filtered_calls": filtered_calls, "non_filtered_calls": non_filtered_calls}

@app.route('/')
def index():
    end_date = datetime.today()
    start_date = end_date - timedelta(days=7)
    return render_template('index.html', start_date=start_date.strftime('%Y-%m-%d'), end_date=end_date.strftime('%Y-%m-%d'), products=ALL_PRODUCT_TAGS, access_key="", secret_key="", message="", show_download=False)

@app.route('/health')
def health():
    return "OK", 200

@app.route('/process', methods=['POST'])
def process():
    access_key = request.form.get('access_key', '')
    secret_key = request.form.get('secret_key', '')
    products = request.form.getlist('products') or ALL_PRODUCT_TAGS
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')

    form_state = {
        "start_date": start_date,
        "end_date": end_date,
        "products": products,
        "access_key": access_key,
        "secret_key": secret_key,
        "message": "",
        "show_download": False
    }

    if not start_date or not end_date:
        form_state["message"] = "Missing start or end date."
        return render_template('index.html', **form_state)

    date_format = '%Y-%m-%d'
    try:
        start_dt = datetime.strptime(start_date, date_format)
        end_dt = datetime.strptime(end_date, date_format)
        if start_dt > end_dt:
            form_state["message"] = "Start date cannot be after end date."
            return render_template('index.html', **form_state)
        today = datetime.today()
        if start_dt > today or end_dt > today:
            form_state["message"] = "Date range cannot include future dates."
            return render_template('index.html', **form_state)
    except ValueError:
        form_state["message"] = "Invalid date format. Use YYYY-MM-DD."
        return render_template('index.html', **form_state)

    if not access_key or not secret_key:
        form_state["message"] = "Missing API keys."
        return render_template('index.html', **form_state)

    try:
        client = GongAPIClient(access_key, secret_key)
        utc = pytz.UTC
        start_dt = utc.localize(datetime.strptime(start_date, date_format))
        end_dt = utc.localize(datetime.strptime(end_date, date_format).replace(hour=23, minute=59, second=59))
        start_date_utc = start_dt.isoformat().replace('+00:00', 'Z')
        end_date_utc = end_dt.isoformat().replace('+00:00', 'Z')
        
        logger.info(f"Fetching calls from {start_date_utc} to {end_date_utc}")
        call_ids = client.fetch_call_list(start_date_utc, end_date_utc)
        
        if not call_ids:
            form_state["message"] = "No calls found for the selected date range."
            return render_template('index.html', **form_state)

        full_data = []
        dropped_calls = 0

        logger.info("Fetching transcripts")
        transcripts = client.fetch_transcript(call_ids)

        logger.info("Fetching and normalizing call details")
        for i in range(0, len(call_ids), BATCH_SIZE):
            batch_call_ids = call_ids[i:i + BATCH_SIZE]
            logger.info(f"Processing batch {i // BATCH_SIZE + 1}: calls {i + 1} to {min(i + BATCH_SIZE, len(call_ids))}")
            for call in client.fetch_call_details(batch_call_ids):
                call_id = get_field(call.get("metaData", {}), "id", "Unknown")
                if call_id == "Unknown":
                    dropped_calls += 1
                    continue
                call_transcript = transcripts.get(call_id, [])
                normalized = normalize_call_data(call, call_transcript)
                full_data.append(normalized)
                if len(full_data) % 10 == 0:
                    logger.info(f"Processed {len(full_data)} calls")
        logger.info(f"Total calls normalized: {len(full_data)}, dropped: {dropped_calls}")

        if not full_data:
            form_state["message"] = f"No valid call data retrieved. Dropped {dropped_calls} calls."
            return render_template('index.html', **form_state)

        utterances_df = prepare_utterances_df(full_data, products)
        call_summary_df = prepare_call_summary_df(full_data, products)
        json_data = prepare_json_output(full_data, products)

        if utterances_df.empty and call_summary_df.empty:
            form_state["message"] = "No calls matched the selected products."
            return render_template('index.html', **form_state)

        unique_id = datetime.now().strftime("%Y%m%d%H%M%S")
        logger.info(f"Using output directory: {OUTPUT_DIR}")
        cleanup_old_files()
        start_date_str = start_dt.strftime("%d%b%y").lower()
        end_date_str = end_dt.strftime("%d%b%y").lower()
        utterances_path = os.path.join(OUTPUT_DIR, f"utterances_gong_{start_date_str}_to_{end_date_str}_{unique_id}.csv")
        call_summary_path = os.path.join(OUTPUT_DIR, f"call_summary_gong_{start_date_str}_to_{end_date_str}_{unique_id}.csv")
        json_path = os.path.join(OUTPUT_DIR, f"call_data_gong_{start_date_str}_to_{end_date_str}_{unique_id}.json")

        paths = {
            "utterances_path": utterances_path,
            "call_summary_path": call_summary_path,
            "json_path": json_path,
            "log_path": log_file_path
        }
        save_file_paths(paths)

        save_utterances_to_csv(utterances_df, utterances_path)
        call_summary_df.to_csv(call_summary_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=2)

        form_state["message"] = f"Processed {len(full_data)} calls. Dropped {dropped_calls} calls. Filtered utterances: {len(utterances_df)}."
        form_state["show_download"] = True
        return render_template('index.html', **form_state)

    except GongAPIError as e:
        logger.error(f"API error: {e.message}")
        form_state["message"] = f"API Error: {e.message}"
        return render_template('index.html', **form_state)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        form_state["message"] = f"Unexpected error: {str(e)}"
        return render_template('index.html', **form_state)

@app.route('/download/utterances')
def download_utterances():
    paths = load_file_paths()
    utterances_path = paths.get("utterances_path")
    if not utterances_path or not os.path.exists(utterances_path):
        return "File not found", 404
    return send_file(
        utterances_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=os.path.basename(utterances_path)
    )

@app.route('/download/call_summary')
def download_call_summary():
    paths = load_file_paths()
    call_summary_path = paths.get("call_summary_path")
    if not call_summary_path or not os.path.exists(call_summary_path):
        return "File not found", 404
    return send_file(
        call_summary_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=os.path.basename(call_summary_path)
    )

@app.route('/download/json')
def download_json():
    paths = load_file_paths()
    json_path = paths.get("json_path")
    if not json_path or not os.path.exists(json_path):
        return "File not found", 404
    return send_file(
        json_path,
        mimetype='application/json',
        as_attachment=True,
        download_name=os.path.basename(json_path)
    )

@app.route('/download/logs')
def download_logs():
    paths = load_file_paths()
    log_path = paths.get("log_path")
    if not log_path or not os.path.exists(log_path):
        return "File not found", 404
    return send_file(
        log_path,
        mimetype='text/plain',
        as_attachment=True,
        download_name="logs.txt"
    )

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_mode)