import base64
import json
import logging
import os
import re
import time
import glob
import threading
import traceback
import unicodedata
from datetime import datetime, timedelta
from io import StringIO
import csv
import pandas as pd
import pytz
import requests
from flask import Flask, render_template, request, send_file, jsonify

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
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()]
)
logging.getLogger('').setLevel(logging.DEBUG if os.environ.get('FLASK_DEBUG', 'False').lower() == 'true' else logging.INFO)

logger.info("Starting Gong Wizard Web Flask - Version 2025-04-21")
GONG_BASE_URL = "https://us-11211.api.gong.io"
SF_TZ = pytz.timezone('America/Los_Angeles')
OUTPUT_DIR = "/tmp/gong_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
PATHS_FILE = os.path.join(OUTPUT_DIR, "file_paths.json")
BATCH_SIZE = 25
TRACKER_BUFFER_S = 1
MAX_DATE_RANGE_MONTHS = 12

# Google Sheet ID
SHEET_ID = "1tvItwAqONZYhetTbg7KAHw0OMPaDfCoFC4g6rSg0QvE"

# Initialize global variables
PRODUCT_MAPPINGS = {}
ENERGY_SAVINGS_KEYWORDS = []
HVAC_TOPICS_KEYWORDS = []
INTERNAL_DOMAINS = set()
EXCLUDED_DOMAINS = set()
EXCLUDED_ACCOUNT_NAMES = set()
EXCLUDED_TRACKERS = set()
INTERNAL_SPEAKERS = set()
EXCLUDED_TOPICS = set()
CALL_ID_TO_ACCOUNT_NAME = {}
OWNER_ACCOUNT_NAMES = set()
TARGET_DOMAINS = set()
TENANT_DOMAINS = set()
ALL_PRODUCT_TAGS = []

def safe_operation(operation, default_value=None, log_message=None, *args, **kwargs):
    try:
        return operation(*args, **kwargs)
    except Exception as e:
        if log_message:
            logger.error(f"{log_message}: {str(e)}\n{traceback.format_exc()}")
        return default_value

def normalize_domain(url):
    if not url or url.lower() in ["n/a", "unknown"]:
        return ""
    domain = re.sub(r'^https?://', '', str(url).lower(), flags=re.IGNORECASE)
    domain = re.sub(r'^www\.', '', domain, flags=re.IGNORECASE)
    return domain.split('/')[0].strip()

def get_email_domain(email):
    return "" if not email or "@" not in email else email.split("@")[-1].strip().lower()

def get_email_local_part(email):
    return "" if not email or "@" not in email else email.split("@")[0].strip().lower()

def load_csv_from_sheet(gid: int, label: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            logger.info(f"Loaded {label} sheet with {len(df)} records")
            return df
        else:
            logger.error(f"Failed to fetch {label} Google Sheet: HTTP {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to load {label} Google Sheet: {str(e)}\n{traceback.format_exc()}")
        return pd.DataFrame()

def load_product_mappings() -> dict:
    gid = 1216942066
    df = load_csv_from_sheet(gid, "PRODUCT_MAPPINGS")
    if df.empty or "Product" not in df.columns or "Keyword" not in df.columns:
        logger.warning("PRODUCT_MAPPINGS sheet is empty or missing required columns")
        return {}
    mappings = {}
    for _, row in df.iterrows():
        product = row.get("Product", "").lower()
        keyword = row.get("Keyword", "")
        if product and keyword:
            mappings.setdefault(product, []).append(keyword)
    for product in ["occupancy analytics", "odcv_keywords"]:
        if product in mappings:
            mappings[product] = [re.compile(pattern, re.IGNORECASE) for pattern in mappings[product]]
    logger.info(f"Loaded {len(mappings)} product mappings")
    return mappings

def load_energy_savings_keywords() -> list:
    gid = 482507272
    df = load_csv_from_sheet(gid, "ENERGY_SAVINGS_KEYWORDS")
    if df.empty or "Keyword" not in df.columns:
        logger.warning("ENERGY_SAVINGS_KEYWORDS sheet is empty or missing 'Keyword' column")
        return []
    keywords = df["Keyword"].dropna().astype(str).tolist()
    logger.info(f"Loaded {len(keywords)} energy savings keywords")
    return keywords

def load_hvac_topics_keywords() -> list:
    gid = 746230823
    df = load_csv_from_sheet(gid, "HVAC_TOPICS_KEYWORDS")
    if df.empty or "Keyword" not in df.columns:
        logger.warning("HVAC_TOPICS_KEYWORDS sheet is empty or missing 'Keyword' column")
        return []
    keywords = df["Keyword"].dropna().astype(str).tolist()
    logger.info(f"Loaded {len(keywords)} HVAC topics keywords")
    return keywords

def load_internal_domains() -> set:
    gid = 784372544
    df = load_csv_from_sheet(gid, "INTERNAL_DOMAINS")
    if df.empty or "Domain" not in df.columns:
        logger.warning("INTERNAL_DOMAINS sheet is empty or missing 'Domain' column")
        return set()
    domains = set(df["Domain"].dropna().astype(str).str.lower())
    logger.info(f"Loaded {len(domains)} internal domains")
    return domains

def load_excluded_domains() -> set:
    gid = 463927561
    df = load_csv_from_sheet(gid, "EXCLUDED_DOMAINS")
    if df.empty or "Domain" not in df.columns:
        logger.warning("EXCLUDED_DOMAINS sheet is empty or missing 'Domain' column")
        return set()
    domains = set(df["Domain"].dropna().astype(str).str.lower())
    logger.info(f"Loaded {len(domains)} excluded domains")
    return domains

def load_excluded_account_names() -> set:
    gid = 1453423105
    df = load_csv_from_sheet(gid, "EXCLUDED_ACCOUNT_NAMES")
    if df.empty or "Account Name" not in df.columns:
        logger.warning("EXCLUDED_ACCOUNT_NAMES sheet is empty or missing 'Account Name' column")
        return set()
    names = set(df["Account Name"].dropna().astype(str).str.lower())
    logger.info(f"Loaded {len(names)} excluded account names")
    return names

def load_excluded_trackers() -> set:
    gid = 1627752322
    df = load_csv_from_sheet(gid, "EXCLUDED_TRACKERS")
    if df.empty or "Tracker" not in df.columns:
        logger.warning("EXCLUDED_TRACKERS sheet is empty or missing 'Tracker' column")
        return set()
    trackers = set(df["Tracker"].dropna().astype(str).str.lower())
    logger.info(f"Loaded {len(trackers)} excluded trackers")
    return trackers

def load_internal_speakers() -> set:
    gid = 1402964429
    df = load_csv_from_sheet(gid, "INTERNAL_SPEAKERS")
    if df.empty or "Speaker" not in df.columns:
        logger.warning("INTERNAL_SPEAKERS sheet is empty or missing 'Speaker' column")
        return set()
    speakers = set(df["Speaker"].dropna().astype(str).str.lower())
    logger.info(f"Loaded {len(speakers)} internal speakers")
    return speakers

def load_excluded_topics() -> set:
    gid = 1653785571
    df = load_csv_from_sheet(gid, "EXCLUDED_TOPICS")
    if df.empty or "Topic" not in df.columns:
        logger.warning("EXCLUDED_TOPICS sheet is empty or missing 'Topic' column")
        return set()
    topics = set(df["Topic"].dropna().astype(str).str.lower())
    logger.info(f"Loaded {len(topics)} excluded topics")
    return topics

def load_call_id_to_account_name() -> dict:
    gid = 300481101
    df = load_csv_from_sheet(gid, "CALL_ID_TO_ACCOUNT_NAME")
    if df.empty or "Call ID" not in df.columns or "Account Name" not in df.columns:
        logger.warning("CALL_ID_TO_ACCOUNT_NAME sheet is empty or missing required columns")
        return {}
    mappings = {}
    for _, row in df.iterrows():
        call_id = str(row.get("Call ID", ""))
        account_name = row.get("Account Name", "").lower()
        if call_id and account_name:
            mappings[call_id] = account_name
    logger.info(f"Loaded {len(mappings)} call ID to account name mappings")
    return mappings

def load_owner_account_names() -> set:
    gid = 583478969
    df = load_csv_from_sheet(gid, "OWNER_ACCOUNT_NAMES")
    if df.empty or "Account Name" not in df.columns:
        logger.warning("OWNER_ACCOUNT_NAMES sheet is empty or missing 'Account Name' column")
        return set()
    names = set(df["Account Name"].dropna().astype(str).str.lower())
    logger.info(f"Loaded {len(names)} owner account names")
    return names

def load_target_domains() -> set:
    gid = 1010248949
    df = load_csv_from_sheet(gid, "OWNER_DOMAINS")
    if df.empty or "Domain" not in df.columns:
        logger.warning("OWNER_DOMAINS sheet is empty or missing 'Domain' column")
        return set()
    domains = set(normalize_domain(domain) for domain in df["Domain"].dropna().astype(str))
    logger.info(f"Loaded {len(domains)} target domains")
    return domains

def load_tenant_domains() -> set:
    gid = 139303828
    df = load_csv_from_sheet(gid, "TENANT_DOMAINS")
    if df.empty or "Domain" not in df.columns:
        logger.warning("TENANT_DOMAINS sheet is empty or missing 'Domain' column")
        return set()
    domains = set(normalize_domain(domain) for domain in df["Domain"].dropna().astype(str))
    logger.info(f"Loaded {len(domains)} tenant domains")
    return domains

def cleanup_old_files():
    now = time.time()
    for file_path in glob.glob(os.path.join(OUTPUT_DIR, "*")):
        if file_path != PATHS_FILE and os.path.isfile(file_path) and (now - os.path.getmtime(file_path)) > 3600:
            try:
                os.remove(file_path)
                logger.info(f"Removed old file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing old file {file_path}: {str(e)}\n{traceback.format_exc()}")

def save_file_paths(paths):
    try:
        with open(PATHS_FILE, 'w') as f:
            json.dump(paths, f)
        logger.info(f"Saved file paths to {PATHS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save file paths: {str(e)}\n{traceback.format_exc()}")

def load_file_paths():
    if not os.path.exists(PATHS_FILE):
        return {}
    try:
        with open(PATHS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading paths file: {str(e)}\n{traceback.format_exc()}")
        return {}

_initialization_done = False
_init_lock = threading.Lock()

@app.before_request
def initialize():
    global PRODUCT_MAPPINGS, ENERGY_SAVINGS_KEYWORDS, HVAC_TOPICS_KEYWORDS, INTERNAL_DOMAINS, EXCLUDED_DOMAINS, EXCLUDED_ACCOUNT_NAMES, EXCLUDED_TRACKERS, INTERNAL_SPEAKERS, EXCLUDED_TOPICS, CALL_ID_TO_ACCOUNT_NAME, OWNER_ACCOUNT_NAMES, TARGET_DOMAINS, TENANT_DOMAINS, ALL_PRODUCT_TAGS, _initialization_done
    with _init_lock:
        if not _initialization_done:
            logger.info("Starting initialization")
            PRODUCT_MAPPINGS.update(load_product_mappings())
            ENERGY_SAVINGS_KEYWORDS.extend(load_energy_savings_keywords())
            HVAC_TOPICS_KEYWORDS.extend(load_hvac_topics_keywords())
            INTERNAL_DOMAINS.update(load_internal_domains())
            EXCLUDED_DOMAINS.update(load_excluded_domains())
            EXCLUDED_ACCOUNT_NAMES.update(load_excluded_account_names())
            EXCLUDED_TRACKERS.update(load_excluded_trackers())
            INTERNAL_SPEAKERS.update(load_internal_speakers())
            EXCLUDED_TOPICS.update(load_excluded_topics())
            CALL_ID_TO_ACCOUNT_NAME.update(load_call_id_to_account_name())
            OWNER_ACCOUNT_NAMES.update(load_owner_account_names())
            TARGET_DOMAINS.update(load_target_domains())
            TENANT_DOMAINS.update(load_tenant_domains())
            ALL_PRODUCT_TAGS.extend([p for p in PRODUCT_MAPPINGS.keys() if p != "odcv_keywords"])
            cleanup_old_files()
            _initialization_done = True
            logger.info("Initialization completed")

class GongAPIError(Exception):
    def __init__(self, status_code, message):
        self.status_code, self.message = status_code, message
        super().__init__(f"Gong API Error {status_code}: {message}")

class GongAPIClient:
    def __init__(self, access_key, secret_key):
        self.base_url = GONG_BASE_URL
        self.session = requests.Session()
        credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
        self.session.headers.update({"Authorization": f"Basic {credentials}"})

    def api_call(self, method, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(5):
            try:
                response = self.session.request(method, url, **kwargs, timeout=10)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (401, 403):
                    raise GongAPIError(response.status_code, "Authentication failed")
                elif response.status_code == 429:
                    time.sleep(int(response.headers.get("Retry-After", (2 ** attempt) * 2)))
                    continue
                else:
                    raise GongAPIError(response.status_code, f"API error: {response.text}")
            except requests.RequestException as e:
                if attempt == 4:
                    raise GongAPIError(0, f"Network error: {str(e)}")
                time.sleep(2 ** attempt)
        raise GongAPIError(429, "Max retries exceeded")

    def fetch_call_list(self, from_date, to_date):
        endpoint = "/v2/calls"
        call_ids = []
        page = 1
        while True:
            params = {"fromDateTime": from_date, "toDateTime": to_date, "page": page, "perPage": 100}
            response = self.api_call("GET", endpoint, params=params)
            call_ids.extend([str(call.get("id")) for call in response.get("calls", [])])
            if len(call_ids) >= response.get("records", {}).get('totalRecords', 0):
                break
            page += 1
        logger.info(f"Fetched {len(call_ids)} call IDs")
        return call_ids

    def fetch_call_details(self, call_ids):
        endpoint = "/v2/calls/extensive"
        cursor = None
        while True:
            data = {
                "filter": {"callIds": call_ids},
                "contentSelector": {
                    "exposedFields": {"parties": True, "content": {"trackers": True, "trackerOccurrences": True, "brief": True, "keyPoints": True, "highlights": True}, "collaboration": {"publicComments": True}},
                    "context": "Extended"
                },
                "cursor": cursor
            }
            response = self.api_call("POST", endpoint, json=data)
            for call in response.get("calls", []):
                yield call
            cursor = response.get("records", {}).get("cursor")
            if not cursor:
                break

    def fetch_transcript(self, call_ids):
        endpoint = "/v2/calls/transcript"
        result = {}
        cursor = None
        while True:
            request_body = {"filter": {"callIds": call_ids}, "cursor": cursor}
            data = self.api_call("POST", endpoint, json=request_body)
            for t in data.get("callTranscripts", []):
                if t.get("callId"):
                    result[str(t["callId"])] = t.get("transcript", [])
            cursor = data.get("records", {}).get("cursor")
            if not cursor:
                break
        logger.info(f"Fetched transcripts for {len(result)} calls")
        return result

def convert_to_sf_time(utc_time):
    if not utc_time:
        return "N/A"
    try:
        utc_time = re.sub(r'\.\d+(?=[+-]\d{2}:\d{2})', '', utc_time.replace("Z", "+00:00"))
        return datetime.fromisoformat(utc_time).astimezone(SF_TZ).strftime("%b %d, %Y")
    except ValueError as e:
        logger.error(f"Date conversion error: {str(e)}\n{traceback.format_exc()}")
        return "N/A"

def get_field(data, key, default=""):
    return next((v if v is not None else default for k, v in data.items() if k.lower() == key.lower()), default) if isinstance(data, dict) else default

def extract_field_values(context, field_name, object_type=None):
    values = []
    for ctx in context or []:
        for obj in ctx.get("objects", []):
            if object_type and get_field(obj, "objectType", "").lower() != object_type.lower():
                continue
            if field_name.lower() == "objectid":
                if value := get_field(obj, "objectId", ""):
                    values.append(str(value))
                continue
            for field in obj.get("fields", []):
                if isinstance(field, dict) and get_field(field, "name", "").lower() == field_name.lower():
                    if value := get_field(field, "value", ""):
                        values.append(str(value))
    return values

def apply_occupancy_analytics_tags(call):
    text = " ".join([get_field(call.get("metaData", {}), "title"), get_field(call.get("content", {}), "brief"), " ".join(kp.get("text", "") for kp in call.get("content", {}).get("keyPoints", [])), " ".join(h.get("text", "") for h in call.get("content", {}).get("highlights", []))]).lower()
    return any(pattern.search(text) for pattern in PRODUCT_MAPPINGS["occupancy analytics"]) if "occupancy analytics" in PRODUCT_MAPPINGS else False

def normalize_call_data(call, transcript):
    try:
        meta_data, content, parties, context = call.get("metaData", {}), call.get("content", {}), call.get("parties", []), call.get("context", [])
        call_id = get_field(meta_data, "id", "")
        call_id_clean = call_id.lstrip("'")
        account_name = (extract_field_values(context, "name", "account") or [""])[0].lower()
        account_website = (extract_field_values(context, "website", "account") or [""])[0].lower()
        normalized_domain = normalize_domain(account_website)

        if call_id_clean in CALL_ID_TO_ACCOUNT_NAME:
            account_name, org_type = CALL_ID_TO_ACCOUNT_NAME[call_id_clean], "owner" if call_id_clean == "5800318421597720457" else "other"
            logger.info(f"Overrode account_name to {account_name} and org_type to {org_type} for call {call_id}")
        else:
            account_name_mappings = {
                "brandywine": "brandywine reit", "crescent heights": "crescent real estate",
                "mayo foundation for medical education and research": "mayo clinic",
                "netflix - new york": "netflix", "qualcomm demo": "qualcomm",
                "stanford health care - all sites": "stanford health care"
            }
            account_name = account_name_mappings.get(account_name.lower(), account_name)
            if not account_name and account_website:
                account_name = normalized_domain
            elif not account_name:
                for party in parties:
                    email_domain = get_email_domain(get_field(party, "emailAddress", ""))
                    if email_domain and email_domain not in INTERNAL_DOMAINS and email_domain not in EXCLUDED_DOMAINS:
                        account_name = email_domain
                        break
                account_name = "" if not account_name or account_name in INTERNAL_DOMAINS or account_name in EXCLUDED_DOMAINS else account_name
            org_type = "owner" if account_name in OWNER_ACCOUNT_NAMES or normalized_domain in TARGET_DOMAINS else "tenant" if normalized_domain in TENANT_DOMAINS else "other"

        trackers = content.get("trackers", [])
        tracker_counts = {get_field(t, "name").lower(): get_field(t, "count", 0) for t in trackers}
        products = ["occupancy analytics"] if org_type == "tenant" and apply_occupancy_analytics_tags(call) else []
        for product in PRODUCT_MAPPINGS:
            if product == "occupancy analytics" and product not in products and apply_occupancy_analytics_tags(call):
                products.append(product)
            elif product != "odcv_keywords":
                if any(tracker_counts.get(tracker.lower(), 0) > 0 for tracker in PRODUCT_MAPPINGS[product] if isinstance(tracker, str)):
                    products.append(product)

        tracker_occurrences = [
            {"tracker_name": str(get_field(t, "name", "")).lower(), "phrase": str(get_field(o, "phrase", "")),
             "start": float(get_field(o, "startTime", 0.0)), "speakerId": str(get_field(o, "speakerId", ""))}
            for t in trackers for o in t.get("occurrences", [])
        ]
        return {
            "call_id": f"'{call_id}", "call_title": get_field(meta_data, "title"),
            "call_date": convert_to_sf_time(get_field(meta_data, "started")),
            "account_name": account_name, "account_id": (extract_field_values(context, "objectId", "account") or [""])[0],
            "account_website": account_website, "account_industry": (extract_field_values(context, "industry", "account") or [""])[0].lower(),
            "products": products, "parties": parties, "utterances": transcript or [], "partial_data": False,
            "org_type": org_type, "tracker_occurrences": tracker_occurrences, "call_summary": get_field(content, "brief", ""),
            "key_points": " | ".join(kp.get("text", "") for kp in content.get("keyPoints", [])),
            "highlights": " | ".join(h.get("text", "") for h in content.get("highlights", []))
        }
    except Exception as e:
        logger.error(f"Normalization error for call '{call_id}': {str(e)}\n{traceback.format_exc()}")
        return {
            "call_id": f"'{call_id}", "call_title": "", "call_date": "N/A", "account_name": "", "account_id": "",
            "account_website": "", "account_industry": "", "products": [], "parties": call.get("parties", []),
            "utterances": [], "partial_data": True, "org_type": "", "tracker_occurrences": [], "call_summary": "",
            "key_points": "", "highlights": ""
        }

def normalize_keyword(keyword):
    return " ".join(re.sub(r'm\s*&\s*v|m&v', 'm and v', re.sub(r'[-]', ' ', keyword.lower().strip())).split())

def find_keyword(text, keywords):
    text_normalized, text_words = normalize_keyword(text), normalize_keyword(text).split()
    for keyword in keywords:
        keyword_normalized, keyword_parts = normalize_keyword(keyword), normalize_keyword(keyword).split()
        if keyword_normalized in text_normalized or any(" ".join(text_words[i:i + len(keyword_parts)]) == keyword_normalized for i in range(len(text_words) - len(keyword_parts) + 1)):
            return keyword
    return ""

def filter_call(call):
    return call["account_name"].lower() not in EXCLUDED_ACCOUNT_NAMES and call["account_name"].lower() not in INTERNAL_DOMAINS

def prepare_utterances_df(calls, selected_products):
    if not calls:
        logger.debug("No calls provided to prepare_utterances_df")
        return pd.DataFrame(), {
            "total_utterances": 0, "internal_utterances": 0, "short_utterances": 0,
            "excluded_topic_utterances": 0, "excluded_topics": {t: 0 for t in EXCLUDED_TOPICS},
            "no_metadata_utterances": 0, "non_matching_product_utterances": 0, "included_utterances": 0,
            "percentInternalUtterances": 0, "percentShortUtterances": 0, "percentExcludedTopicUtterances": 0,
            "percentNoMetadataUtterances": 0, "percentNonMatchingProductUtterances": 0, "percentIncludedUtterances": 0
        }, False, 0, 0

    total_utterances, internal_utterances, short_utterances, excluded_topic_utterances = 0, 0, 0, 0
    no_metadata_utterances, non_matching_product_utterances, excluded_account_calls, no_utterances_calls = 0, 0, 0, 0
    excluded_topics, selected_products_lower = {t: 0 for t in EXCLUDED_TOPICS}, [p.lower() for p in selected_products]
    include_energy_savings, call_utterances = any(p in ["secure air", "odcv"] for p in selected_products_lower), []

    for call in calls:
        call_id, products = call["call_id"], call.get("products", [])
        if not filter_call(call):
            excluded_account_calls += 1
            logger.debug(f"Call {call_id} excluded: account_name={call['account_name']} in EXCLUDED_ACCOUNT_NAMES or INTERNAL_DOMAINS")
            continue
        utterances = call["utterances"] or []
        if not utterances:
            no_utterances_calls += 1
            logger.debug(f"Call {call_id} excluded: no utterances")
            continue

        for u in utterances:
            sentences = u.get("sentences", [])
            u.update({
                "start_time": min([s.get("start", 0) for s in sentences]) / 1000 if sentences else 0,
                "end_time": max([s.get("end", 0) for s in sentences]) / 1000 if sentences else 0,
                "is_incomplete": not sentences, "trackers": []
            })

        valid_trackers = [t for t in call.get("tracker_occurrences", []) if float(t.get("start", 0.0)) > 0]
        unmatched_trackers = []
        for tracker in valid_trackers:
            tracker_name, tracker_time = get_field(tracker, "tracker_name", "").lower(), tracker["start"]
            if tracker_name == "negative impact (by gong)":
                tracker_name = "objection"
            if tracker_name in EXCLUDED_TRACKERS:
                logger.debug(f"Tracker {tracker_name} in call {call_id} excluded: in EXCLUDED_TRACKERS")
                continue
            eligible = [(u, abs(tracker_time - u["start_time"]), u["end_time"]) for u in utterances if (u["start_time"] - TRACKER_BUFFER_S) <= tracker_time <= (u["end_time"] + TRACKER_BUFFER_S)]
            if eligible:
                eligible.sort(key=lambda x: (x[1], x[2]))
                eligible[0][0]["trackers"].append({"tracker_name": tracker_name})
            else:
                unmatched_trackers.append(tracker_name)
        if unmatched_trackers:
            logger.debug(f"Unmatched trackers in call {call_id}: {', '.join(set(unmatched_trackers))}")

        speaker_info = {get_field(p, "speakerId", ""): p for p in call["parties"]}
        call_data = {
            "call_id": call_id, "call_date": call["call_date"], "account_name": call["account_name"],
            "account_industry": call["account_industry"], "org_type": call["org_type"], "utterances": []
        }

        for u in utterances:
            total_utterances += 1
            text = " ".join(s.get("text", "") for s in u.get("sentences", [])) or "No transcript available"
            text = re.sub(r'\bR0\b', 'R-Zero', text, flags=re.IGNORECASE)
            speaker = speaker_info.get(get_field(u, "speakerId", ""), {})
            speaker_name = get_field(speaker, "name", "").lower() or get_email_local_part(get_field(speaker, "emailAddress", ""))
            email_domain = get_email_domain(get_field(speaker, "emailAddress", ""))

            if not speaker_name and text == "No transcript available":
                logger.debug(f"Utterance in call {call_id} excluded: no speaker name and no transcript")
                continue
            if speaker_name in INTERNAL_SPEAKERS or (email_domain and (email_domain in INTERNAL_DOMAINS or any(email_domain.endswith("." + d) for d in INTERNAL_DOMAINS))):
                internal_utterances += 1
                logger.debug(f"Utterance in call {call_id} excluded: internal speaker {speaker_name} or domain {email_domain}")
                continue
            speaker_affiliation, speaker_job_title = get_field(speaker, "affiliation", "unknown").lower(), get_field(speaker, "title", "")

            tracker_set = {t["tracker_name"].lower() for t in u.get("trackers", [])}
            mapped_products = set()
            if "filter" in tracker_set or "filtration" in tracker_set:
                mapped_products.add("secure air")
            if "energy savings" in tracker_set:
                mapped_products.add("odcv" if "odcv" in selected_products_lower and "secure air" not in selected_products_lower else "secure air")
            if "odcv" in tracker_set:
                mapped_products.add("odcv")
            if "r-zero competitors" in tracker_set or "remote work (by gong)" in tracker_set:
                mapped_products.add("occupancy analytics")
            if "air quality" in tracker_set:
                mapped_products.add("iaq monitoring")
            text_lower = text.lower()
            if "occupancy analytics" in PRODUCT_MAPPINGS and any(p.search(text_lower) for p in PRODUCT_MAPPINGS["occupancy analytics"]):
                mapped_products.add("occupancy analytics")
            if "odcv_keywords" in PRODUCT_MAPPINGS and any(p.search(text_lower) for p in PRODUCT_MAPPINGS["odcv_keywords"]):
                mapped_products.add("odcv")
            product = "|".join(mapped_products) if mapped_products else ""

            topic = get_field(u, "topic", "").lower()
            if product and any(tag in selected_products_lower for tag in product.split("|")):
                logger.debug(f"Utterance in call {call_id} included: matching product tag {product}")
            elif topic in EXCLUDED_TOPICS:
                excluded_topic_utterances += 1
                excluded_topics[topic] += 1
                logger.debug(f"Utterance in call {call_id} excluded: topic {topic} in EXCLUDED_TOPICS")
                continue
            if len(text.split()) < 8 and text != "No transcript available":
                short_utterances += 1
                logger.debug(f"Utterance in call {call_id} excluded: short utterance ({len(text.split())} words)")
                continue

            tracker_str = "|".join(sorted({t["tracker_name"].lower() for t in u["trackers"]})) or (topic if topic and topic not in EXCLUDED_TOPICS else "")
            energy_savings = find_keyword(text, ENERGY_SAVINGS_KEYWORDS) if include_energy_savings and "energy savings" not in tracker_set else "energy savings" if "energy savings" in tracker_set else ""
            hvac_topics = find_keyword(text, HVAC_TOPICS_KEYWORDS)

            if not (product or tracker_str or (include_energy_savings and energy_savings) or hvac_topics):
                no_metadata_utterances += 1
                logger.debug(f"Utterance in call {call_id} excluded: no product, tracker, energy savings, or HVAC topics")
                continue
            if product and not any(p in selected_products_lower for p in products) and not any(tag in selected_products_lower for tag in product.split("|")):
                non_matching_product_utterances += 1
                logger.debug(f"Utterance in call {call_id} excluded: product {product} does not match selected products or call products")
                continue

            logger.debug(f"Utterance in call {call_id} included: product={product}, tracker={tracker_str}, energy_savings={energy_savings}, hvac_topics={hvac_topics}")
            call_data["utterances"].append({
                "utterance_start_time": u["start_time"], "utterance_end_time": u["end_time"],
                "speaker_name": speaker_name, "speaker_job_title": speaker_job_title,
                "speaker_affiliation": speaker_affiliation, "product": product,
                "energy_savings_measurement": energy_savings if include_energy_savings else "",
                "hvac_topics": hvac_topics, "tracker": tracker_str, "utterance_text": text,
                "is_incomplete": u["is_incomplete"]
            })

        if call_data["utterances"]:
            call_data["utterances"].sort(key=lambda x: x["utterance_start_time"])
            call_utterances.append(call_data)

    call_utterances.sort(key=lambda x: datetime.strptime(x["call_date"], "%b %d, %Y"), reverse=True)
    data = [
        dict({"call_id": c["call_id"], "call_date": c["call_date"], "account_name": c["account_name"],
              "account_industry": c["account_industry"], "org_type": c["org_type"]}, **u)
        for c in call_utterances for u in c["utterances"]
    ]
    columns = [
        "call_id", "call_date", "account_name", "account_industry", "org_type", "speaker_name",
        "speaker_job_title", "speaker_affiliation", "product",
        "energy_savings_measurement" if include_energy_savings else None, "hvac_topics", "tracker",
        "utterance_text", "utterance_start_time", "utterance_end_time", "is_incomplete"
    ]
    df = pd.DataFrame(data, columns=[c for c in columns if c]).astype({"call_id": str}) if data else pd.DataFrame()

    utterance_stats = {
        "total_utterances": total_utterances, "internal_utterances": internal_utterances,
        "short_utterances": short_utterances, "excluded_topic_utterances": excluded_topic_utterances,
        "excluded_topics": excluded_topics, "no_metadata_utterances": no_metadata_utterances,
        "non_matching_product_utterances": non_matching_product_utterances, "included_utterances": len(df),
        "percentInternalUtterances": round(internal_utterances / total_utterances * 100) if total_utterances else 0,
        "percentShortUtterances": round(short_utterances / total_utterances * 100) if total_utterances else 0,
        "percentExcludedTopicUtterances": round(excluded_topic_utterances / total_utterances * 100) if total_utterances else 0,
        "percentNoMetadataUtterances": round(no_metadata_utterances / total_utterances * 100) if total_utterances else 0,
        "percentNonMatchingProductUtterances": round(non_matching_product_utterances / total_utterances * 100) if total_utterances else 0,
        "percentIncludedUtterances": round(len(df) / total_utterances * 100) if total_utterances else 0
    }
    logger.debug(f"Utterance stats for calls: {utterance_stats}")
    return df, utterance_stats, include_energy_savings, excluded_account_calls, no_utterances_calls

def prepare_call_summary_df(calls, selected_products):
    if not calls:
        logger.debug("No calls provided to prepare_call_summary_df")
        return pd.DataFrame()
    selected_products_lower = [p.lower() for p in selected_products]
    data = [
        {
            "call_id": c["call_id"], "call_title": c["call_title"], "call_date": c["call_date"],
            "filtered_out": "included" if any(p.lower() in selected_products_lower for p in c.get("products", [])) else "no product tags" if not c.get("products", []) else "no matching product",
            "product_tags": "|".join(c.get("products", [])), "org_type": c["org_type"],
            "account_name": c["account_name"], "account_website": c["account_website"],
            "account_industry": c["account_industry"], "call_summary": c.get("call_summary", ""),
            "key_points": c.get("key_points", ""), "highlights": c.get("highlights", "")
        } for c in calls
    ]
    df = pd.DataFrame(data).astype({"call_id": str}).sort_values("call_date", ascending=False) if data else pd.DataFrame()
    logger.debug(f"Prepared call summary DataFrame with {len(df)} rows")
    return df

def prepare_json_output(calls, utterance_call_ids, selected_products):
    if not calls or not utterance_call_ids:
        logger.debug("No calls or utterance call IDs provided to prepare_json_output")
        return []
    selected_products_lower = [p.lower() for p in selected_products]
    filtered_calls = []
    for call in calls:
        call_id, products = call["call_id"], call.get("products", [])
        if call_id not in utterance_call_ids or not products or not any(p.lower() in selected_products_lower for p in products) or not filter_call(call):
            logger.debug(f"Call {call_id} skipped in JSON output: not in utterance_call_ids, no products, or filtered out")
            continue
        utterances = sorted(call["utterances"] or [], key=lambda x: float(get_field(x.get("sentences", [{}])[0] if x.get("sentences") else {}, "start", float('inf')) / 1000))
        if not utterances:
            logger.debug(f"Call {call_id} skipped in JSON output: no utterances")
            continue

        speaker_info = {get_field(p, "speakerId", ""): p for p in call["parties"]}
        rzero_participants, other_participants, transcript_entries = [], [], []
        for speaker_id, speaker in speaker_info.items():
            speaker_name = get_field(speaker, "name", "").lower() or get_email_local_part(get_field(speaker, "emailAddress", ""))
            email_domain = get_email_domain(get_field(speaker, "emailAddress", ""))
            if speaker_name:
                (rzero_participants if speaker_name in INTERNAL_SPEAKERS or (email_domain and (email_domain in INTERNAL_DOMAINS or any(email_domain.endswith("." + d) for d in INTERNAL_DOMAINS))) else other_participants).append(speaker_name.title())

        for u in utterances:
            speaker_name = get_field(speaker_info.get(get_field(u, "speakerId", ""), {}), "name", "").lower() or get_email_local_part(get_field(speaker_info.get(get_field(u, "speakerId", ""), {}), "emailAddress", ""))
            if not speaker_name:
                logger.debug(f"Utterance in call {call_id} skipped in JSON output: no speaker name")
                continue
            sentences = u.get("sentences", [])
            if sentences:
                text = re.sub(r'\bR0\b', 'R-Zero', " ".join(s.get("text", "") for s in sentences), flags=re.IGNORECASE)
                transcript_entries.append({
                    "start_time": min([s.get("start", 0) for s in sentences]) / 1000,
                    "end_time": max([s.get("end", 0) for s in sentences]) / 1000,
                    "speaker": speaker_name.title(), "text": text
                })

        if transcript_entries:
            filtered_calls.append({
                "call_id": call_id, "title": call["call_title"], "date": call["call_date"],
                "participants": {"R-Zero": sorted(rzero_participants), "Other": sorted(other_participants)},
                "transcript": transcript_entries
            })
            logger.debug(f"Call {call_id} included in JSON output with {len(transcript_entries)} transcript entries")

    filtered_calls.sort(key=lambda x: datetime.strptime(x["date"], "%b %d, %Y"), reverse=True)
    return filtered_calls

@app.route('/', methods=['GET', 'POST'])
def index():
    current_date, yesterday = datetime.now(pytz.UTC), datetime.now(pytz.UTC) - timedelta(days=1)
    form_state = {
        "products": ALL_PRODUCT_TAGS,
        "access_key": "",
        "secret_key": "",
        "start_date": (yesterday - timedelta(days=30)).strftime('%Y-%m-%d'),
        "end_date": yesterday.strftime('%Y-%m-%d'),
        "message": "",
        "show_download": False
    }
    logger.debug("Rendering index page")
    return render_template(
        'index.html',
        start_date=form_state["start_date"],
        end_date=form_state["end_date"],
        available_products=ALL_PRODUCT_TAGS,
        access_key="",
        secret_key="",
        message="",
        show_download=False,
        form_state=form_state,
        current_date=current_date,
        max_date=yesterday.strftime('%Y-%m-%d')
    )

@app.route('/process', methods=['POST'])
def process():
    try:
        access_key, secret_key = request.form.get('access_key', ''), request.form.get('secret_key', '')
        selected_products = request.form.getlist('products') or ALL_PRODUCT_TAGS
        start_date, end_date = request.form.get('start_date'), request.form.get('end_date')
        form_state = {
            "start_date": start_date, "end_date": end_date, "products": selected_products,
            "access_key": access_key, "secret_key": secret_key, "message": "", "show_download": False,
            "stats": {}, "utterance_breakdown": {}
        }

        if not all([start_date, end_date, access_key, secret_key]):
            form_state["message"] = "Missing required fields."
            logger.warning("Missing required fields in /process request")
            return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
            today = datetime.now(pytz.UTC).date()
            if (start_dt.date() > end_dt.date() or start_dt.date() > today or
                end_dt.date() > today or (end_dt - start_dt).days / 30.42 > MAX_DATE_RANGE_MONTHS):
                form_state["message"] = "Invalid date range."
                logger.warning(f"Invalid date range: start={start_date}, end={end_date}")
                return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)
        except ValueError as e:
            form_state["message"] = "Invalid date format."
            logger.error(f"Date format error: {str(e)}")
            return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

        client = GongAPIClient(access_key, secret_key)
        start_date_utc = start_dt.isoformat().replace('+00:00', 'Z')
        end_date_utc = end_dt.replace(hour=23, minute=59, second=59).isoformat().replace('+00:00', 'Z')
        logger.debug(f"Fetching call list from {start_date_utc} to {end_date_utc}")
        call_ids = client.fetch_call_list(start_date_utc, end_date_utc)
        if not call_ids:
            form_state["message"] = "No calls found."
            logger.info("No calls found for the specified date range")
            return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

        full_data, dropped_calls, transcripts = [], 0, client.fetch_transcript(call_ids)
        for i in range(0, len(call_ids), BATCH_SIZE):
            for call in client.fetch_call_details(call_ids[i:i + BATCH_SIZE]):
                call_id = get_field(call.get("metaData", {}), "id", "")
                if not call_id:
                    dropped_calls += 1
                    logger.debug("Dropped call: no call ID")
                    continue
                full_data.append(normalize_call_data(call, transcripts.get(call_id, [])))

        if not full_data:
            form_state["message"] = "No valid call data retrieved."
            logger.info("No valid call data retrieved from Gong API")
            return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

        logger.debug(f"Preparing utterances DataFrame for {len(full_data)} calls")
        utterances_df, utterance_stats, include_energy_savings, excluded_account_calls, no_utterances_calls = prepare_utterances_df(full_data, selected_products)
        logger.debug("Preparing call summary DataFrame")
        call_summary_df = prepare_call_summary_df(full_data, selected_products)
        logger.debug("Preparing JSON output")
        json_data = prepare_json_output(full_data, set(utterances_df['call_id'].unique()) if not utterances_df.empty else set(), selected_products)

        if utterances_df.empty and call_summary_df.empty:
            form_state["message"] = "No calls matched the selected products."
            logger.info("No calls matched the selected products")
            return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

        total_calls = len(full_data) + dropped_calls + excluded_account_calls + no_utterances_calls
        stats = {
            "totalCallsRetrieved": total_calls, "droppedCalls": dropped_calls, "validCalls": len(full_data),
            "callsWithNoProducts": len(call_summary_df[call_summary_df["filtered_out"] == "no product tags"]) if not call_summary_df.empty else 0,
            "callsNotMatchingSelection": len(call_summary_df[call_summary_df["filtered_out"] == "no matching product"]) if not call_summary_df.empty else 0,
            "callsIncluded": len(call_summary_df[call_summary_df["filtered_out"] == "included"]) if not call_summary_df.empty else 0,
            "callsIncludedFormatted": "{:,}".format(len(call_summary_df[call_summary_df["filtered_out"] == "included"]) if not call_summary_df.empty else 0),
            "partialDataCalls": sum(1 for c in full_data if c["partial_data"]),
            "invalidDateCalls": sum(1 for c in full_data if c["call_date"] == "N/A"),
            "percentDropped": round(dropped_calls / total_calls * 100) if total_calls else 0,
            "percentValid": round(len(full_data) / total_calls * 100) if total_calls else 0,
            "percentNoProducts": round((len(call_summary_df[call_summary_df["filtered_out"] == "no product tags"]) if not call_summary_df.empty else 0) / total_calls * 100) if total_calls else 0,
            "percentNotMatching": round((len(call_summary_df[call_summary_df["filtered_out"] == "no matching product"]) if not call_summary_df.empty else 0) / total_calls * 100) if total_calls else 0,
            "percentIncluded": round((len(call_summary_df[call_summary_df["filtered_out"] == "included"]) if not call_summary_df.empty else 0) / total_calls * 100) if total_calls else 0,
            "calls_table": sorted([
                {"exclusion": e, "count": c, "count_formatted": "{:,}".format(c), "percent": round(c / total_calls * 100)}
                for e, c in [
                    ("Excluded (Invalid Date)", sum(1 for c in full_data if c["call_date"] == "N/A")),
                    ("Excluded (No Product Tag)", len(call_summary_df[call_summary_df["filtered_out"] == "no product tags"]) if not call_summary_df.empty else 0),
                    ("Excluded (Unselected Product Tag)", len(call_summary_df[call_summary_df["filtered_out"] == "no matching product"]) if not call_summary_df.empty else 0),
                    ("Excluded (Dropped)", dropped_calls),
                    ("Excluded (Account Name)", excluded_account_calls),
                    ("Excluded (No Utterances)", no_utterances_calls)
                ]
            ], key=lambda x: (-x["count"], x["exclusion"])),
            "included_utterances": utterance_stats["included_utterances"],
            "included_utterances_formatted": "{:,}".format(utterance_stats["included_utterances"]),
            "percentIncludedUtterances": utterance_stats["percentIncludedUtterances"],
            "excluded_topic_percentages": {
                t: round(c / utterance_stats["total_utterances"] * 100) if utterance_stats["total_utterances"] else 0
                for t, c in utterance_stats["excluded_topics"].items()
            }
        }
        stats["calls_table"] = [entry for entry in stats["calls_table"] if entry["count"] > 0]

        start_date_str = start_dt.strftime("%d%b%y").lower()
        end_date_str = end_dt.strftime("%d%b%y").lower()
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        utterances_path = os.path.join(OUTPUT_DIR, f"utterances_{start_date_str}to{end_date_str}_{timestamp}.csv")
        call_summary_path = os.path.join(OUTPUT_DIR, f"summary_{start_date_str}to{end_date_str}_{timestamp}.csv")
        json_path = os.path.join(OUTPUT_DIR, f"transcripts_{start_date_str}to{end_date_str}_{timestamp}.json")

        try:
            if not utterances_df.empty:
                utterances_df['utterance_text'] = utterances_df['utterance_text'].apply(
                    lambda x: unicodedata.normalize('NFKD', str(x)).encode('ascii', 'ignore').decode('ascii') if x else ''
                )
                utterances_df.to_csv(utterances_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8', errors='replace')
                logger.info(f"Saved utterances to {utterances_path}")
            call_summary_df.to_csv(call_summary_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8', errors='replace')
            logger.info(f"Saved call summary to {call_summary_path}")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved transcripts to {json_path}")
            save_file_paths({"utterances_path": utterances_path, "call_summary_path": call_summary_path, "json_path": json_path, "log_path": log_file_path})
        except Exception as e:
            logger.error(f"Error saving output files: {str(e)}\n{traceback.format_exc()}")
            form_state["message"] = "Error saving output files."
            return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

        total_tags = utterances_df['product'].str.split("|").explode().count() if not utterances_df.empty else 0
        energy_savings_count = utterances_df['energy_savings_measurement'].ne('').sum() if include_energy_savings and not utterances_df.empty else 0
        hvac_topics_count = utterances_df['hvac_topics'].ne('').sum() if not utterances_df.empty else 0
        other_topics = sorted(
            [(t, utterances_df['tracker'].str.count(t).sum())
             for t in set(utterances_df['tracker'].str.split("|").explode())
             if t and t.lower() not in ["energy savings", "hvac topics"]],
            key=lambda x: -x[1]
        )[:8] if not utterances_df.empty else []

        form_state.update({
            "message": "Processing complete. Download files below.",
            "show_download": True,
            "stats": stats,
            "utterance_breakdown": {
                "product": sorted([
                    {"value": p, "count": c, "count_formatted": "{:,}".format(c)}
                    for p, c in utterances_df['product'].str.split("|").explode().value_counts().items() if p
                ], key=lambda x: (-x["count"], x["value"])) if not utterances_df.empty else [],
                "exclusions": [entry for entry in sorted([
                    {"exclusion": "Internal Speaker", "count": utterance_stats["internal_utterances"],
                     "count_formatted": "{:,}".format(utterance_stats["internal_utterances"])},
                    {"exclusion": "Short Utterance", "count": utterance_stats["short_utterances"],
                     "count_formatted": "{:,}".format(utterance_stats["short_utterances"])},
                    {"exclusion": "No Tag", "count": utterance_stats["no_metadata_utterances"],
                     "count_formatted": "{:,}".format(utterance_stats["no_metadata_utterances"])},
                    {"exclusion": "Non Matching Product Tag", "count": utterance_stats["non_matching_product_utterances"],
                     "count_formatted": "{:,}".format(utterance_stats["non_matching_product_utterances"])}
                ] + [
                    {"exclusion": t.title(), "count": c, "count_formatted": "{:,}".format(c)}
                    for t, c in utterance_stats["excluded_topics"].items() if c > 0
                ], key=lambda x: (-x["count"], x["exclusion"])) if entry["count"] > 0],
                "topics": sorted([
                    {"topic": t, "count": c, "count_formatted": "{:,}".format(c)}
                    for t, c in other_topics + [("Energy Savings", energy_savings_count), ("HVAC Topics", hvac_topics_count)]
                ], key=lambda x: -x["count"]) if not utterances_df.empty else []
            }
        })
        logger.info("Processing completed successfully")
        return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

    except Exception as e:
        logger.error(f"Unexpected error in /process: {str(e)}\n{traceback.format_exc()}")
        form_state["message"] = "An unexpected error occurred. Please try again."
        return render_template('index.html', form_state=form_state, available_products=ALL_PRODUCT_TAGS, **form_state)

@app.route('/download/<file_type>')
def download(file_type):
    try:
        paths = load_file_paths()
        file_mapping = {
            'utterances': ('utterances_path', 'utterances.csv', 'text/csv'),
            'call_summary': ('call_summary_path', 'call_summary.csv', 'text/csv'),
            'json': ('json_path', 'transcripts.json', 'application/json'),
            'logs': ('log_path', 'app.log', 'text/plain')
        }
        if file_type not in file_mapping:
            logger.warning(f"Invalid file type requested: {file_type}")
            return "Invalid file type", 400
        file_path = paths.get(file_mapping[file_type][0])
        if not file_path or not os.path.exists(file_path):
            logger.warning(f"File not found for type {file_type}: {file_path}")
            return "File not found", 404
        logger.debug(f"Serving file {file_path} for type {file_type}")
        return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path), mimetype=file_mapping[file_type][2])
    except Exception as e:
        logger.error(f"Error in /download for {file_type}: {str(e)}\n{traceback.format_exc()}")
        return "Error downloading file", 500

if __name__ == '__main__':
    app.run(debug=os.environ.get('FLASK_DEBUG', 'False').lower() == 'true', host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))