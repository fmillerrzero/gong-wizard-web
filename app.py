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
BATCH_SIZE = 25
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
INTERNAL_DOMAINS = {
    "secureaire.com", "rzero.com", "rzerosystems.com", "globant.com",
    "4mod.fr", "teamblume.com", "greenkoncepts.com", "dbl.vc"
}
EXCLUDED_DOMAINS = {"gmail.com", "outlook.com"}
EXCLUDED_ACCOUNT_NAMES = {"4MOD Technology", "Green Koncepts"}
EXCLUDED_TRACKERS = {"product trackers", "covid-19 (by gong)"}
INTERNAL_SPEAKERS = {
    "Andy Pires", "Anthony Salvatore", "Ben Boyer", "Ben Siegfried", "Benjamin Boyer",
    "Benjamin Green", "Bob Ladue", "Bob Li", "Brenda Quan", "Chad Miller",
    "Chandrika Arya", "Chelsea Sutherland", "Christopher Tulabut", "Dana DuFrane",
    "Dana Karnon", "Dana Mor Karnon", "Danielle Duhon", "Dave Cox", "David Nuno",
    "David Schlaifer", "David Seniawski", "Don Hess", "Drew Oliner", "Elizabeth Redmond",
    "Eric Foster", "Fabian Echevarria", "Forrest Miller", "Francis Stamatatos",
    "Frank Stamatatos", "Hannah Sverdlik", "Ian Leshinsky", "Ilya Gendelman",
    "James Rollins", "Jennifer Nuckles", "Jim Hine", "Jorge Quiros", "Julio Munoz",
    "Kayla Wilson", "Kevin Baxter", "Kim Neff", "Kristen Alexander", "Lee Oshnock",
    "Lou Preston", "Luis Aguilar", "Manali Kulkarni", "Martyn R. Buffler", "Matt Arneson",
    "Mehak Dharmani", "Michael Chu", "Michael Dever", "Michael Hopps", "Mohamed El-afifi",
    "Molly Chen", "Monique Barash", "Nelson Alvarado", "Nestor Turizo", "Nick Viscuso",
    "Nicolaas Van Nuil", "Nicole Dianne Banta", "Olivia Cvitanic", "Patrick Gerding",
    "Priscilla Pan", "Rick Martin", "Roger Baker", "Ryan Aman", "Sanjil Karki",
    "Stas Kurgansky", "Stephanie Snow", "Steven Lee", "Suman Bharadwaj", "Thomas Reznik",
    "Tim Lombardi", "Trish Pearce", "Uri Kogan", "Varun Shroff", "Veronica Herico",
    "Wiley Wang", "Will Musat"
}
EXCLUDED_TOPICS = {"call setup", "small talk", "wrap-up"}
MAX_DATE_RANGE_MONTHS = 12

# Mapping of call IDs to account names (without leading quote in input)
CALL_ID_TO_ACCOUNT_NAME = {
    "1846318168516521453": "Skanska",
    "3516974213942229787": "Polinger",
    "3748506113741127946": "Low Tide",
    "3778553613579836966": "BGO",
    "3975541205726528077": "SHI",
    "4043412895308886662": "Skanska",
    "453107256614930203": "Hudson Pacific Properties",
    "4978183599069254431": "Cushman & Wakefield",
    "6020208759295664749": "BGO",
    "7077682709419191760": "Brandywine REIT",
    "5800318421597720457": "Tri Properties",
    "1012640371113456338": "teamblume.com",
    "8016049473232396330": "SANAS",
    "3685926123376587680": "Liberty Universal Management",
    "3693959199474407205": "Robinson, Mills, & Williams",
    "7029155942116413511": "Echelon Energy",
    "242882209984690388": "Hudson Pacific Properties",
    "6165233458620391702": "featsolutions.co",
    "6311626885008998437": "Windemere Park",
    "3161981180942379924": "Trebeller",
    "5539318371463264430": "Heintges",
    "2583785926492910728": "Wasatch Pediatrics",
    "8706978918402417625": "R-Zero",
    "4773313504786316524": "featsolutions.co",
    "7381136096718703005": "GSA",
    "5323386923177414850": "featsolutions.co",
    "1043468188026972306": "Martyn",
    "3024779093682219637": "Acuity",
    "3086859059452771835": "Syserco",
    "731350228371917423": "Syserco",
    "6730174387046870110": "Syserco"
}

# Account names that should always have org_type = "owner"
OWNER_ACCOUNT_NAMES = {"Brandywine REIT", "Crescent Real Estate", "Hudson Pacific Properties"}

# Precompile regex patterns for Occupancy Analytics with case-insensitive flag
for product in PRODUCT_MAPPINGS:
    if product == "Occupancy Analytics":
        PRODUCT_MAPPINGS[product] = [re.compile(pattern, re.IGNORECASE) for pattern in PRODUCT_MAPPINGS[product]]

def safe_operation(operation, default_value=None, log_message=None, *args, **kwargs):
    try:
        return operation(*args, **kwargs)
    except Exception as e:
        if log_message:
            logger.error(f"{log_message}: {str(e)}\n{traceback.format_exc()}")
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
    email = str(email).strip().lower()
    domain = email.split("@")[-1].strip()
    return domain

def get_email_local_part(email):
    if not email or "@" not in email:
        return ""
    return email.split("@")[0].strip()

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
                logger.error(f"Error removing old file {file_path}: {str(e)}\n{traceback.format_exc()}")

def save_file_paths(paths):
    try:
        with open(PATHS_FILE, 'w') as f:
            json.dump(paths, f)
        logger.info(f"Saved file paths to {PATHS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save file paths to {PATHS_FILE}: {str(e)}\n{traceback.format_exc()}")

def load_file_paths():
    if not os.path.exists(PATHS_FILE):
        logger.error(f"Paths file not found: {PATHS_FILE}")
        return {}
    try:
        with open(PATHS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading paths file {PATHS_FILE}: {str(e)}\n{traceback.format_exc()}")
        return {}

_initialization_done = False
_init_lock = threading.Lock()

@app.before_request
def initialize():
    global TARGET_DOMAINS, TENANT_DOMAINS, _initialization_done
    with _init_lock:
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
                logger.error(f"Initialization failed: {str(e)}\n{traceback.format_exc()}")
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
        max_attempts = 5
        for attempt in range(max_attempts):
            logger.info(f"Starting API call to {endpoint} - Attempt {attempt + 1}")
            try:
                response = self.session.request(method, url, **kwargs, timeout=10)
                logger.info(f"Completed API call to {endpoint} - Attempt {attempt + 1}, Status: {response.status_code}")
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (401, 403):
                    logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                    raise GongAPIError(response.status_code, "Authentication failed")
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                    logger.warning(f"Rate limit hit, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"API error: {response.status_code} - {response.text}")
                    raise GongAPIError(response.status_code, f"API error: {response.text}")
            except requests.RequestException as e:
                logger.error(f"Network error on attempt {attempt + 1}: {str(e)}\n{traceback.format_exc()}")
                if attempt == max_attempts - 1:
                    raise GongAPIError(0, f"Network error: {str(e)}")
                time.sleep(2 ** attempt)
        raise GongAPIError(429, "Max retries exceeded")

    def fetch_call_list(self, from_date, to_date):
        endpoint = "/v2/calls"
        call_ids = []
        page = 1
        while True:
            params = {
                "fromDateTime": from_date,
                "toDateTime": to_date,
                "page": page,
                "perPage": 100
            }
            response = self.api_call("GET", endpoint, params=params)
            calls = response.get("calls", [])
            call_ids.extend([str(call.get("id")) for call in calls])
            records = response.get("records", {})
            logger.info(f"Page info: totalRecords={records.get('totalRecords')}, currentPageSize={records.get('currentPageSize')}, page={page}")
            total_records = records.get('totalRecords', 0)
            if len(call_ids) >= total_records:
                break
            page += 1
        logger.info(f"Fetched {len(call_ids)} call IDs")
        return call_ids

    def fetch_call_details(self, call_ids):
        endpoint = "/v2/calls/extensive"
        cursor = None
        while True:
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
                            "keyPoints": True,
                            "highlights": True
                        },
                        "collaboration": {
                            "publicComments": True
                        }
                    },
                    "context": "Extended"
                },
                "cursor": cursor
            }
            response = self.api_call("POST", endpoint, json=data)
            records = response.get("records", {})
            logger.info(f"Call details page info: totalRecords={records.get('totalRecords')}, currentPageSize={records.get('currentPageSize')}, cursor={cursor}")
            for call in response.get("calls", []):
                yield call
            cursor = records.get("cursor")
            if not cursor:
                break

    def fetch_transcript(self, call_ids, max_attempts=5):
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
        utc_time = re.sub(r'\.\d+(?=[+-]\d{2}:\d{2})', '', utc_time)
        utc_dt = datetime.fromisoformat(utc_time)
        sf_dt = utc_dt.astimezone(SF_TZ)
        return sf_dt.strftime("%b %d, %Y")
    except ValueError as e:
        logger.error(f"Date conversion error for {utc_time}: {str(e)}\n{traceback.format_exc()}")
        return "N/A"

def get_field(data, key, default=""):
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
            obj_type = get_field(obj, "objectType", "")
            if object_type and obj_type.lower() != object_type.lower():
                continue
            if field_name.lower() == "objectid":
                value = get_field(obj, "objectId", "")
                if value:
                    values.append(str(value))
                continue
            for field in obj.get("fields", []):
                if not isinstance(field, dict):
                    continue
                field_name_val = get_field(field, "name", "")
                if field_name_val.lower() == field_name.lower():
                    value = get_field(field, "value", "")
                    if value:
                        values.append(str(value))
    return values

def apply_occupancy_analytics_tags(call):
    fields = [
        get_field(call.get("metaData", {}), "title"),
        get_field(call.get("content", {}), "brief")
    ]
    fields.append(" ".join(kp.get("text", "") for kp in call.get("content", {}).get("keyPoints", [])))
    fields.append(" ".join(h.get("text", "") for h in call.get("content", {}).get("highlights", [])))
    text = " ".join(f for f in fields if f).lower()
    matches = [pattern.pattern for pattern in PRODUCT_MAPPINGS["Occupancy Analytics"] if pattern.search(text)]
    return bool(matches)

def normalize_call_data(call, transcript):
    try:
        meta_data = call.get("metaData", {})
        content = call.get("content", {})
        parties = call.get("parties", [])
        context = call.get("context", [])

        call_id = get_field(meta_data, "id", "")
        call_title = get_field(meta_data, "title", "")
        call_date = convert_to_sf_time(get_field(meta_data, "started"))
        account_ids = extract_field_values(context, "objectId", "Account")
        account_name = extract_field_values(context, "Name", "Account")[0] if extract_field_values(context, "Name", "Account") else ""
        account_id = account_ids[0] if account_ids else ""
        account_website = extract_field_values(context, "Website", "Account")[0] if extract_field_values(context, "Website", "Account") else ""
        account_industry = extract_field_values(context, "Industry", "Account")[0] if extract_field_values(context, "Industry", "Account") else ""

        call_id_clean = call_id.lstrip("'")

        if call_id_clean in CALL_ID_TO_ACCOUNT_NAME:
            account_name = CALL_ID_TO_ACCOUNT_NAME[call_id_clean]
            org_type = "owner" if call_id_clean in {"5800318421597720457"} else "other"
            logger.info(f"Overrode account_name to {account_name} and org_type to {org_type} for call {call_id}")
        else:
            account_name_mappings = {
                "Brandywine": "Brandywine REIT",
                "Crescent Heights": "Crescent Real Estate",
                "Mayo Foundation for Medical Education and Research": "Mayo Clinic",
                "Netflix - New York": "Netflix",
                "Qualcomm Demo": "Qualcomm",
                "Stanford Health Care - All Sites": "Stanford Health Care"
            }
            for old_name, new_name in account_name_mappings.items():
                if account_name == old_name:
                    account_name = new_name
                    logger.info(f"Normalized account_name from {old_name} to {new_name} for call {call_id}")
                    break

            normalized_domain = normalize_domain(account_website)
            if not account_name and account_website:
                account_name = normalized_domain
            if not account_name and not account_website:
                for party in parties:
                    email = get_field(party, "emailAddress", "")
                    email_domain = get_email_domain(email)
                    if (email_domain and 
                        not any(email_domain.endswith("." + internal_domain) for internal_domain in INTERNAL_DOMAINS) and
                        email_domain not in EXCLUDED_DOMAINS):
                        account_name = email_domain
                        break
                if not account_name or account_name in INTERNAL_DOMAINS or account_name in EXCLUDED_DOMAINS:
                    account_name = ""

            org_type = "other"
            if normalized_domain in TARGET_DOMAINS:
                org_type = "owner"
            elif normalized_domain in TENANT_DOMAINS:
                org_type = "tenant"
            if account_name in OWNER_ACCOUNT_NAMES:
                org_type = "owner"
                logger.info(f"Set org_type to owner for account_name {account_name} in call {call_id}")

        trackers = content.get("trackers", [])
        tracker_counts = {get_field(t, "name").lower(): get_field(t, "count", 0) for t in trackers if get_field(t, "name")}

        products = []
        if org_type == "tenant" and "Occupancy Analytics" not in products:
            products.append("Occupancy Analytics")

        for product in PRODUCT_MAPPINGS:
            if product == "Occupancy Analytics" and "Occupancy Analytics" not in products:
                if apply_occupancy_analytics_tags(call):
                    products.append(product)
            else:
                for tracker in PRODUCT_MAPPINGS[product]:
                    if isinstance(tracker, str) and tracker_counts.get(tracker.lower(), 0) > 0:
                        products.append(product)
                        break

        tracker_occurrences = []
        for tracker in trackers:
            tracker_name = get_field(tracker, "name", "")
            for occurrence in tracker.get("occurrences", []):
                tracker_occurrences.append({
                    "tracker_name": tracker_name,
                    "phrase": get_field(occurrence, "phrase", ""),
                    "start": int(get_field(occurrence, "startTime", 0)),
                    "speakerId": get_field(occurrence, "speakerId", "")
                })

        call_summary = get_field(content, "brief", "")
        key_points = " | ".join(kp.get("text", "") for kp in content.get("keyPoints", []))
        highlights = " | ".join(h.get("text", "") for h in content.get("highlights", []))

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
            "key_points": key_points,
            "highlights": highlights,
            "metaData": meta_data
        }
    except Exception as e:
        call_id = get_field(call.get("metaData", {}), "id", "")
        logger.error(f"Normalization error for call '{call_id}': {str(e)}\n{traceback.format_exc()}")
        return {
            "call_id": f"'{call_id}",
            "call_title": "",
            "call_date": "N/A",
            "account_name": "",
            "account_id": "",
            "account_website": "",
            "account_industry": "",
            "products": [],
            "parties": call.get("parties", []),
            "utterances": [],
            "partial_data": True,
            "org_type": "",
            "tracker_occurrences": [],
            "call_summary": "",
            "key_points": "",
            "highlights": ""
        }

def prepare_utterances_df(calls, selected_products):
    if not calls:
        logger.info("No calls to process for utterances DataFrame")
        return pd.DataFrame(), {
            "total_utterances": 0,
            "internal_utterances": 0,
            "short_utterances": 0,
            "excluded_topic_utterances": 0,
            "excluded_topics": {topic: 0 for topic in EXCLUDED_TOPICS},
            "included_utterances": 0
        }
    
    total_utterances = 0
    internal_utterances = 0
    short_utterances = 0
    excluded_topic_utterances = 0
    excluded_topics = {topic: 0 for topic in EXCLUDED_TOPICS}
    data = []
    call_tracker_map = {}
    selected_products_lower = [p.lower() for p in selected_products]
    excluded_topics_set = EXCLUDED_TOPICS
    
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

        account_name = call["account_name"]
        if account_name in EXCLUDED_ACCOUNT_NAMES or account_name in INTERNAL_DOMAINS:
            logger.info(f"Excluded call {call_id} due to account_name {account_name}")
            continue
        
        utterances = call["utterances"] or []
        if not utterances:
            logger.debug(f"Call {call_id}: No utterances found, skipping")
            continue
        
        call_tracker_map[call_id] = {}
        for utterance in utterances:
            sentences = utterance.get("sentences", [])
            if not sentences or not all(isinstance(s, dict) and "start" in s and "end" in s and s["start"] <= s["end"] for s in sentences):
                logger.debug(f"Call {call_id}: Utterance skipped due to missing or invalid sentence data")
                continue
            start_time = min(int(s.get("start", 0)) for s in sentences if s.get("start"))
            end_time = max(int(s.get("end", 0)) for s in sentences if s.get("end"))
            utterance_key = f"{call_id}_{start_time}_{end_time}"
            call_tracker_map[call_id][utterance_key] = {"trackers": [], "start_time": start_time, "end_time": end_time}
        
        for tracker in call.get("tracker_occurrences", []):
            tracker_start = tracker.get("start")
            if not tracker_start or tracker_start < 0:
                logger.debug(f"Call {call_id}: Tracker skipped due to missing or invalid timestamp")
                continue
            tracker_time = int(tracker_start)
            tracker_name = get_field(tracker, "tracker_name", "").lower()
            if tracker_name in EXCLUDED_TRACKERS:
                continue
            for utterance_key, info in call_tracker_map[call_id].items():
                utterance_start = info["start_time"]
                utterance_end = info["end_time"]
                if utterance_start <= tracker_time <= utterance_end:
                    info["trackers"].append({"tracker_name": tracker_name})
        
        speaker_info = {get_field(p, "speakerId", ""): p for p in call["parties"]}
        
        for utterance in utterances:
            total_utterances += 1
            sentences = utterance.get("sentences", [])
            if not sentences:
                logger.warning(f"Call {call_id}: Unexpected utterance structure: {list(utterance.keys())}")
                continue
            text = " ".join(s.get("text", "") if isinstance(s, dict) else "" for s in sentences)
            
            speaker_id = get_field(utterance, "speakerId", "")
            speaker = speaker_info.get(speaker_id, {})
            speaker_name = get_field(speaker, "name", "")
            speaker_email_address = get_field(speaker, "emailAddress", "")
            if not speaker_name and speaker_email_address:
                speaker_name = get_email_local_part(speaker_email_address)
                logger.debug(f"Populated missing speaker name with email local part: {speaker_name} in call {call_id}")
            
            email_domain = get_email_domain(speaker_email_address)
            original_affiliation = get_field(speaker, "affiliation", "unknown").lower()
            if speaker_name in INTERNAL_SPEAKERS or (
                email_domain and (
                    any(email_domain.endswith("." + internal_domain) for internal_domain in INTERNAL_DOMAINS) or 
                    email_domain in INTERNAL_DOMAINS
                )
            ):
                speaker_affiliation = "internal"
                internal_utterances += 1
                if original_affiliation != "internal":
                    logger.info(f"Overrode affiliation from {original_affiliation} to internal for speaker {speaker_name} ({speaker_email_address}) in call {call_id}")
                continue
            else:
                speaker_affiliation = original_affiliation
            
            speaker_job_title = get_field(speaker, "title", "")
            if not speaker_job_title:
                logger.debug(f"Missing job title for speaker {speaker_name} in call {call_id}")
            
            topic = get_field(utterance, "topic", "").lower()
            if topic in excluded_topics_set:
                excluded_topic_utterances += 1
                excluded_topics[topic] += 1
                continue
            if len(text.split()) < 8:
                short_utterances += 1
                continue
            
            start_time = min(int(s.get("start", 0)) for s in sentences if s.get("start"))
            end_time = max(int(s.get("end", 0)) for s in sentences if s.get("end"))
            utterance_key = f"{call_id}_{start_time}_{end_time}"
            triggered_trackers = call_tracker_map.get(call_id, {}).get(utterance_key, {"trackers": []})["trackers"]
            
            tracker_names = []
            for t in triggered_trackers:
                tracker_name = t["tracker_name"].lower()
                if tracker_name == "negative impact (by gong)":
                    tracker_name = "objection"
                if tracker_name:
                    tracker_names.append(tracker_name)
            
            if topic and topic not in excluded_topics_set:
                tracker_names.append(topic)
            
            tracker_counts = {}
            for name in tracker_names:
                if name:
                    tracker_counts[name] = tracker_counts.get(name, 0) + 1
            tracker_str = "|".join(f"{name}: {count}" for name, count in tracker_counts.items()) if tracker_counts else ""
            
            tracker_set = set(t["tracker_name"].lower() for t in triggered_trackers if t["tracker_name"])
            product = ""
            mapped_products = set()
            tracker_names_to_remove = set()
            
            if "filter" in tracker_set:
                mapped_products.add("Secure Air")
                tracker_names_to_remove.add("filter")
            if "energy savings" in tracker_set and "odcv" not in tracker_set:
                mapped_products.add("Secure Air")
                tracker_names_to_remove.add("energy savings")
            elif "energy savings" in tracker_set and "filter" not in tracker_set:
                mapped_products.add("ODCV")
                tracker_names_to_remove.add("energy savings")
            if "odcv" in tracker_set:
                mapped_products.add("ODCV")
                tracker_names_to_remove.add("odcv")
            if "r-zero competitors" in tracker_set:
                mapped_products.add("Occupancy Analytics")
                tracker_names_to_remove.add("r-zero competitors")
            if "remote work (by gong)" in tracker_set:
                mapped_products.add("Occupancy Analytics")
                tracker_names_to_remove.add("remote work (by gong)")
            if "air quality" in tracker_set:
                mapped_products.add("IAQ Monitoring")
                tracker_names_to_remove.add("air quality")
            
            if mapped_products:
                product = "|".join(mapped_products)
                tracker_counts = {name: count for name, count in tracker_counts.items() if name.lower() not in tracker_names_to_remove}
                tracker_str = "|".join(f"{name}: {count}" for name, count in tracker_counts.items()) if tracker_counts else ""
            
            data.append({
                "call_id": call_id,
                "call_date": call["call_date"],
                "account_name": call["account_name"],
                "account_industry": call["account_industry"],
                "org_type": call["org_type"],
                "speaker_name": speaker_name,
                "speaker_job_title": speaker_job_title,
                "speaker_affiliation": speaker_affiliation,
                "product": product,
                "tracker": tracker_str,
                "utterance_text": text,
                "start_time": start_time,
                "end_time": end_time
            })
    
    if data:
        columns = [
            "call_id", "call_date", "account_name", "account_industry", "org_type",
            "speaker_name", "speaker_job_title", "speaker_affiliation",
            "product", "tracker", "utterance_text", "start_time", "end_time"
        ]
        df = pd.DataFrame(data)[columns]
        df['call_id'] = df['call_id'].astype(str)
        df['call_date'] = pd.to_datetime(df['call_date'], format='%b %d, %Y', errors='coerce')
        df = df.sort_values("call_date", ascending=False)
        df['call_date'] = df['call_date'].dt.strftime('%b %d, %Y')
        logger.info(f"Utterances DataFrame: {len(df)} rows, columns: {df.columns.tolist()}")
    else:
        logger.info("Utterances DataFrame is empty after processing")
        df = pd.DataFrame()
    
    return df, {
        "total_utterances": total_utterances,
        "internal_utterances": internal_utterances,
        "short_utterances": short_utterances,
        "excluded_topic_utterances": excluded_topic_utterances,
        "excluded_topics": excluded_topics,
        "included_utterances": len(df)
    }

def save_utterances_to_csv(df, path):
    if df.empty:
        logger.warning("Cannot save empty DataFrame to CSV")
        return
        
    if 'utterance_text' in df.columns:
        df['utterance_text'] = df['utterance_text'].apply(lambda x: 
            unicodedata.normalize('NFKD', str(x))
            .encode('ascii', 'ignore')
            .decode('ascii') if x else '')
    
    df['call_id'] = df['call_id'].astype(str)
    df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8', errors='replace')
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
            "call_title": call["call_title"],
            "call_date": call["call_date"],
            "filtered_out": filtered_out,
            "product_tags": "|".join(products) if products else "",
            "org_type": call["org_type"],
            "account_name": call["account_name"],
            "account_website": call["account_website"],
            "account_industry": call["account_industry"],
            "call_summary": call.get("call_summary", ""),
            "key_points": call.get("key_points", ""),
            "highlights": call.get("highlights", "")
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
        return []
    
    filtered_calls = []
    selected_products_lower = [p.lower() for p in selected_products]
    
    for call in calls:
        call_id = call["call_id"]
        products = call.get("products", [])
        if not products:
            logger.debug(f"Call {call_id}: No products assigned, skipping for JSON")
            continue
        products_lower = [p.lower() for p in products if isinstance(p, str)]
        if not any(p in selected_products_lower for p in products_lower):
            logger.debug(f"Call {call_id}: Products {products} don't match selection {selected_products}, skipping for JSON")
            continue
        
        account_name = call["account_name"]
        if account_name in EXCLUDED_ACCOUNT_NAMES or account_name in INTERNAL_DOMAINS:
            logger.info(f"Excluded call {call_id} from JSON due to account_name {account_name}")
            continue
        
        utterances = call["utterances"] or []
        if not utterances:
            logger.debug(f"Call {call_id}: No utterances found, skipping for JSON")
            continue
        
        # Sort utterances by start time
        utterances = sorted(
            utterances,
            key=lambda x: float(get_field(x.get("sentences", [{}])[0] if x.get("sentences") else {}, "start", 0))
        )
        
        speaker_info = {get_field(p, "speakerId", ""): p for p in call["parties"]}
        transcript_entries = []
        rzero_participants = []
        other_participants = []
        
        # Build speaker lists for metadata
        for speaker_id, speaker in speaker_info.items():
            speaker_name = get_field(speaker, "name", "")
            speaker_email_address = get_field(speaker, "emailAddress", "")
            if not speaker_name and speaker_email_address:
                speaker_name = get_email_local_part(speaker_email_address)
            speaker_title = get_field(speaker, "title", "")
            email_domain = get_email_domain(speaker_email_address)
            is_internal = speaker_name in INTERNAL_SPEAKERS or (
                email_domain and (
                    any(email_domain.endswith("." + internal_domain) for internal_domain in INTERNAL_DOMAINS) or 
                    email_domain in INTERNAL_DOMAINS
                )
            )
            participant_entry = speaker_name if not speaker_title else f"{speaker_name}, {speaker_title}"
            if is_internal:
                rzero_participants.append(participant_entry)
            else:
                other_participants.append(participant_entry)
        
        # Include all utterances in the transcript, unfiltered
        for u in utterances:
            speaker_id = get_field(u, "speakerId", "")
            speaker = speaker_info.get(speaker_id, {})
            speaker_name = get_field(speaker, "name", "")
            speaker_email_address = get_field(speaker, "emailAddress", "")
            if not speaker_name and speaker_email_address:
                speaker_name = get_email_local_part(speaker_email_address)
            
            sentences = u.get("sentences", [])
            if not sentences:
                logger.debug(f"Call {call_id}: Monologue has no sentences, skipping")
                continue
            
            start_time_ms = float(get_field(sentences[0], "start", 0))
            if start_time_ms == 0:
                logger.warning(f"Call {call_id}: Start time is 0 for monologue, using default value")
                start_time_ms = 1
            end_time_ms = float(get_field(sentences[-1], "end", 0))
            
            text = " ".join(s.get("text", "") if isinstance(s, dict) else "" for s in sentences)
            
            minutes = int(start_time_ms // 60000)
            seconds = int((start_time_ms % 60000) // 1000)
            timestamp = f"{minutes:02d}:{seconds:02d}"
            
            transcript_entries.append({
                "timestamp": timestamp,
                "start_time_ms": int(start_time_ms),
                "end_time_ms": int(end_time_ms),
                "speaker_name": speaker_name,
                "text": text
            })
        
        if not transcript_entries:
            logger.debug(f"Call {call_id}: No utterances after processing, skipping for JSON")
            continue
        
        duration_seconds = call.get("metaData", {}).get("duration", 0)
        if not duration_seconds and utterances:
            duration_ms = float(utterances[-1].get("sentences", [{}])[-1].get("end", 0))
        else:
            duration_ms = duration_seconds * 1000
        duration_minutes = int(duration_ms // 60000)
        duration_str = f"{duration_minutes}m"
        
        raw_call_date = call.get("metaData", {}).get("started", "N/A")
        
        call_entry = {
            "metadata": {
                "call_id": call_id,
                "title": call["call_title"],
                "recording_details": f"Recorded on {raw_call_date} via Zoom, {duration_str}",
                "participants": {
                    "R-Zero": rzero_participants,
                    "Other": other_participants
                }
            },
            "transcript": transcript_entries,
            "started": raw_call_date
        }
        filtered_calls.append(call_entry)
    
    for call_entry in filtered_calls:
        started = call_entry["started"]
        try:
            started_cleaned = re.sub(r'\.\d+', '', started)
            started_cleaned = re.sub(r'([+-]\d{2}):(\d{2})', r'\1\2', started_cleaned)
            call_entry["started_dt"] = datetime.fromisoformat(started_cleaned)
        except ValueError as e:
            logger.error(f"Failed to parse started date {started} for call {call_entry['metadata']['call_id']}: {str(e)}\n{traceback.format_exc()}")
            call_entry["started_dt"] = datetime.now(pytz.UTC)
    
    filtered_calls.sort(key=lambda x: x["started_dt"], reverse=True)
    
    for call_entry in filtered_calls:
        call_entry.pop("started", None)
        call_entry.pop("started_dt", None)
    
    logger.info(f"JSON output: {len(filtered_calls)} calls included")
    return filtered_calls

@app.route('/', methods=['GET', 'HEAD'])
def index():
    try:
        logger.debug(f"Handling request to / with method {request.method}")
        end_date = datetime.now(pytz.UTC)
        start_date = end_date - timedelta(days=30)
        max_date = (end_date - timedelta(days=1)).strftime('%Y-%m-%d')
        form_state = {
            "products": ALL_PRODUCT_TAGS,
            "access_key": "",
            "secret_key": "",
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            "message": "",
            "show_download": False
        }
        response = render_template('index.html', 
                                 start_date=start_date.strftime('%Y-%m-%d'), 
                                 end_date=end_date.strftime('%Y-%m-%d'), 
                                 products=ALL_PRODUCT_TAGS, 
                                 access_key="", 
                                 secret_key="", 
                                 message="", 
                                 show_download=False,
                                 form_state=form_state,
                                 current_date=end_date,
                                 max_date=max_date)
        logger.debug("Successfully rendered index.html for / route")
        return response
    except Exception as e:
        logger.error(f"Error in / route: {str(e)}\n{traceback.format_exc()}")
        return "Internal Server Error", 500

@app.route('/health')
def health():
    return "OK", 200

@app.route('/process', methods=['POST'])
def process():
    logger.info("Received POST request to /process")
    access_key = request.form.get('access_key', '')
    secret_key = request.form.get('secret_key', '')
    products = request.form.getlist('products') or ALL_PRODUCT_TAGS
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')

    logger.info(f"Form data - access_key: {access_key}, secret_key: {'[REDACTED]' if secret_key else ''}, products: {products}, start_date: {start_date}, end_date: {end_date}")

    form_state = {
        "start_date": start_date,
        "end_date": end_date,
        "products": products,
        "access_key": access_key,
        "secret_key": secret_key,
        "message": "",
        "show_download": False,
        "stats": {},
        "utterance_breakdown": {}
    }

    if not start_date or not end_date:
        logger.warning("Validation failed: Missing start or end date")
        form_state["message"] = "Missing start or end date."
        return render_template('index.html', form_state=form_state, **form_state)

    date_format = '%Y-%m-%d'
    try:
        start_dt = datetime.strptime(start_date, date_format).replace(tzinfo=pytz.UTC)
        end_dt = datetime.strptime(end_date, date_format).replace(tzinfo=pytz.UTC)
        
        today = datetime.now(pytz.UTC).date()
        start_date_only = start_dt.date()
        end_date_only = end_dt.date()
        
        logger.info(f"Comparing dates - start_date: {start_date_only}, end_date: {end_date_only}, today: {today}")
        
        delta = (end_dt - start_dt).days / 30.42
        if delta > MAX_DATE_RANGE_MONTHS:
            logger.warning(f"Validation failed: Date range exceeds {MAX_DATE_RANGE_MONTHS} months")
            form_state["message"] = f"Date range cannot exceed {MAX_DATE_RANGE_MONTHS} months. Please select a shorter range."
            return render_template('index.html', form_state=form_state, **form_state)
        
        if start_date_only > today or end_date_only > today:
            logger.warning("Validation failed: Date range includes future dates")
            form_state["message"] = "Date range cannot include future dates."
            return render_template('index.html', form_state=form_state, **form_state)
        
        if start_date_only > end_date_only:
            logger.warning("Validation failed: Start date after end date")
            form_state["message"] = "Start date cannot be after end date."
            return render_template('index.html', form_state=form_state, **form_state)
    except ValueError as e:
        logger.warning(f"Validation failed: Invalid date format - {str(e)}\n{traceback.format_exc()}")
        form_state["message"] = "Invalid date format. Use YYYY-MM-DD."
        return render_template('index.html', form_state=form_state, **form_state)

    if not access_key or not secret_key:
        logger.warning("Validation failed: Missing API keys")
        form_state["message"] = "Missing API keys."
        return render_template('index.html', form_state=form_state, **form_state)

    logger.info("All validations passed, proceeding with API call")
    try:
        client = GongAPIClient(access_key, secret_key)
        start_dt = start_dt.astimezone(pytz.UTC)
        end_dt = end_dt.replace(hour=23, minute=59, second=59).astimezone(pytz.UTC)
        start_date_utc = start_dt.isoformat().replace('+00:00', 'Z')
        end_date_utc = end_dt.isoformat().replace('+00:00', 'Z')
        
        logger.info(f"Fetching calls from {start_date_utc} to {end_date_utc}")
        call_ids = client.fetch_call_list(start_date_utc, end_date_utc)
        
        logger.info(f"Retrieved {len(call_ids)} call IDs")
        if not call_ids:
            logger.info("No calls found for the selected date range")
            form_state["message"] = "No calls found for the selected date range."
            return render_template('index.html', form_state=form_state, **form_state)

        full_data = []
        dropped_calls = 0

        logger.info("Fetching transcripts")
        transcripts = client.fetch_transcript(call_ids)

        logger.info("Fetching and normalizing call details")
        for i in range(0, len(call_ids), BATCH_SIZE):
            batch_call_ids = call_ids[i:i + BATCH_SIZE]
            logger.info(f"Processing batch {i // BATCH_SIZE + 1}: calls {i + 1} to {min(i + BATCH_SIZE, len(call_ids))}")
            for call in client.fetch_call_details(batch_call_ids):
                call_id = get_field(call.get("metaData", {}), "id", "")
                if not call_id:
                    dropped_calls += 1
                    continue
                call_transcript = transcripts.get(call_id, [])
                normalized = normalize_call_data(call, call_transcript)
                full_data.append(normalized)
                if len(full_data) % 5 == 0:
                    logger.info(f"Processed {len(full_data)} calls")
        logger.info(f"Total calls normalized: {len(full_data)}, dropped: {dropped_calls}")

        if not full_data:
            logger.info(f"No valid call data retrieved. Dropped {dropped_calls} calls")
            form_state["message"] = f"No valid call data retrieved. Dropped {dropped_calls} calls."
            return render_template('index.html', form_state=form_state, **form_state)

        utterances_df, utterance_stats = prepare_utterances_df(full_data, products)
        call_summary_df = prepare_call_summary_df(full_data, products)
        json_data = prepare_json_output(full_data, products)

        if utterances_df.empty and call_summary_df.empty:
            logger.info("No calls matched the selected products")
            form_state["message"] = "No calls matched the selected products."
            return render_template('index.html', form_state=form_state, **form_state)

        total_calls = len(full_data) + dropped_calls
        partial_data_calls = sum(1 for call in full_data if call["partial_data"])
        invalid_date_calls = sum(1 for call in full_data if call["call_date"] == "N/A")
        calls_with_no_products = len(call_summary_df[call_summary_df["filtered_out"] == "no product tags"])
        calls_not_matching = len(call_summary_df[call_summary_df["filtered_out"] == "no matching product"])
        calls_included = len(call_summary_df[call_summary_df["filtered_out"] == "included"])

        logger.info(f"Call summary - total_calls: {total_calls}, calls_with_no_products: {calls_with_no_products}, calls_not_matching: {calls_not_matching}, calls_included: {calls_included}")

        total_utterances = utterance_stats["total_utterances"]
        utterance_breakdown = {
            "product": []
        }
        if total_utterances > 0:
            product_counts = {}
            for _, row in utterances_df.iterrows():
                if row['product']:
                    for product in row['product'].split("|"):
                        product_counts[product] = product_counts.get(product, 0) + 1
            
            product_total = sum(product_counts.values())
            product_percentages = {}
            for product, count in product_counts.items():
                percentage = round(count / product_total * 100) if product_total > 0 else 0
                product_percentages[product] = percentage
            total_percentage = sum(product_percentages.values())
            if total_percentage != 100 and product_total > 0:
                max_product = max(product_counts, key=product_counts.get)
                product_percentages[max_product] += 100 - total_percentage
            for product, count in product_counts.items():
                utterance_breakdown["product"].append({
                    "value": product,
                    "count": count,
                    "percentage": product_percentages[product]
                })
            
            utterance_breakdown["product"].sort(key=lambda x: x["count"], reverse=True)

        excluded_topic_percentages = {}
        for topic, count in utterance_stats["excluded_topics"].items():
            percentage = round(count / total_utterances * 100) if total_utterances > 0 else 0
            excluded_topic_percentages[topic] = percentage

        stats = {
            "totalCallsRetrieved": total_calls,
            "droppedCalls": dropped_calls,
            "validCalls": len(full_data),
            "callsWithNoProducts": calls_with_no_products,
            "callsNotMatchingSelection": calls_not_matching,
            "callsIncluded": calls_included,
            "partialDataCalls": partial_data_calls,
            "invalidDateCalls": invalid_date_calls,
            "percentDropped": round(dropped_calls / total_calls * 100) if total_calls > 0 else 0,
            "percentValid": round(len(full_data) / total_calls * 100) if total_calls > 0 else 0,
            "percentNoProducts": round(calls_with_no_products / total_calls * 100) if total_calls > 0 else 0,
            "percentNotMatching": round(calls_not_matching / total_calls * 100) if total_calls > 0 else 0,
            "percentIncluded": round(calls_included / total_calls * 100) if total_calls > 0 else 0,
            **utterance_stats,
            "excluded_topic_percentages": excluded_topic_percentages,
            "percentInternalUtterances": round(utterance_stats["internal_utterances"] / utterance_stats["total_utterances"] * 100) if utterance_stats["total_utterances"] > 0 else 0,
            "percentShortUtterances": round(utterance_stats["short_utterances"] / utterance_stats["total_utterances"] * 100) if utterance_stats["total_utterances"] > 0 else 0,
            "percentExcludedTopics": round(utterance_stats["excluded_topic_utterances"] / utterance_stats["total_utterances"] * 100) if utterance_stats["total_utterances"] > 0 else 0,
            "percentIncludedUtterances": round(utterance_stats["included_utterances"] / utterance_stats["total_utterances"] * 100) if utterance_stats["total_utterances"] > 0 else 0
        }
        logger.info(f"Computed stats: {stats}")
        logger.info(f"Utterance breakdown: {utterance_breakdown}")

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
        call_summary_df.to_csv(call_summary_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8', errors='replace')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON to {json_path}")

        form_state["message"] = "Processing completed successfully."
        form_state["show_download"] = True
        form_state["stats"] = stats
        form_state["utterance_breakdown"] = utterance_breakdown
        logger.info("Rendering index.html with download links")

        current_date = datetime.now(pytz.UTC)
        max_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
        return render_template('index.html',
                             form_state=form_state,
                             start_date=start_date,
                             end_date=end_date,
                             products=products,
                             access_key=access_key,
                             secret_key=secret_key,
                             message="Processing completed successfully.",
                             show_download=True,
                             stats=stats,
                             utterance_breakdown=utterance_breakdown,
                             current_date=current_date,
                             max_date=max_date)

    except GongAPIError as e:
        logger.error(f"Gong API error: {str(e)}\n{traceback.format_exc()}")
        form_state["message"] = f"Gong API error: {e.message}"
        return render_template('index.html', form_state=form_state, **form_state)
    except Exception as e:
        logger.error(f"Unexpected error during processing: {str(e)}\n{traceback.format_exc()}")
        form_state["message"] = "An unexpected error occurred. Please try again."
        return render_template('index.html', form_state=form_state, **form_state)

@app.route('/download/<file_type>')
def download(file_type):
    paths = load_file_paths()
    path_key = f"{file_type}_path"
    if path_key not in paths:
        logger.error(f"Download path for {file_type} not found in paths: {paths}")
        return "File not found", 404
    
    file_path = paths[path_key]
    if not os.path.exists(file_path):
        logger.error(f"File not found on disk: {file_path}")
        return "File not found", 404
    
    mime_type = 'text/csv' if file_type in ['utterances', 'call_summary'] else 'application/json' if file_type == 'json' else 'text/plain'
    return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path), mimetype=mime_type)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=os.environ.get('FLASK_DEBUG', 'False').lower() == 'true')