import base64
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import pytz
import requests
from flask import Flask, render_template, request, send_file, session

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', os.urandom(24))  # Secure secret key
logger = logging.getLogger(__name__)

# Configure logging to a file for download and stdout for Render
log_dir = "/tmp"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file_path = os.path.join(log_dir, "app.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger.info("Starting Gong Wizard Web Flask - Version 2025-04-21")

# Constants
GONG_BASE_URL = "https://us-11211.api.gong.io/v2"
SF_TZ = pytz.timezone('America/Los_Angeles')

# Global set for target domains
TARGET_DOMAINS = set()

# Product mappings
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

# Precompile regex patterns for Occupancy Analytics
for product in PRODUCT_MAPPINGS:
    if product == "Occupancy Analytics":
        PRODUCT_MAPPINGS[product] = [re.compile(pattern, re.IGNORECASE) for pattern in PRODUCT_MAPPINGS[product]]

def normalize_domain(url):
    """Extract and normalize a domain from a URL."""
    if not url or url in ["N/A", "Unknown"]:
        return ""
    domain = re.sub(r'^https?://', '', str(url).lower())
    domain = re.sub(r'^www\.', '', domain)
    domain = domain.split('/')[0]
    return domain.strip()

def load_target_domains_from_sheet(sheet_id="1HMAQ3eNhXhCAfcxPqQwds1qn1ZW8j6Sc1oCM9_TLjtQ"):
    """Load target domains from a public Google Sheet."""
    target_domains = set()
    try:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            domains_list = df.iloc[:, 0].dropna().astype(str).tolist()
            for domain in domains_list:
                normalized = normalize_domain(domain)
                if normalized:
                    target_domains.add(normalized)
            logger.info(f"Loaded {len(target_domains)} target domains from Google Sheet")
        else:
            logger.error(f"Failed to fetch Google Sheet: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Error loading domains from Google Sheet: {str(e)}")
    return target_domains

@app.before_first_request
def initialize():
    """Load target domains once when the app starts."""
    global TARGET_DOMAINS
    TARGET_DOMAINS = load_target_domains_from_sheet()
    logger.info(f"Initialized with {len(TARGET_DOMAINS)} target domains")

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
                    raise GongAPIError(response.status_code, "Authentication failed: Invalid API keys or permissions")
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
        params = {"fromDateTime": from_date, "toDateTime": to_date}
        call_ids = []
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self.api_call("GET", "calls", params=params)
            calls = data.get("calls", [])
            call_ids.extend(str(call.get("id", "")) for call in calls if call.get("id"))
            cursor = data.get("records", {}).get("cursor")
            if not cursor:
                break
            time.sleep(1)
        logger.info(f"Fetched {len(call_ids)} call IDs")
        return call_ids

    def fetch_call_details(self, call_ids):
        batch_size = 10
        for i in range(0, len(call_ids), batch_size):
            batch_ids = call_ids[i:i + batch_size]
            body = {
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
            data = self.api_call("POST", "calls/extensive", json=body)
            for call in data.get("calls", []):
                yield call

    def fetch_transcript(self, call_ids):
        batch_size = 10
        for i in range(0, len(call_ids), batch_size):
            batch_ids = call_ids[i:i + batch_size]
            body = {"filter": {"callIds": batch_ids}}
            data = self.api_call("POST", "calls/transcript", json=body)
            for t in data.get("callTranscripts", []):
                call_id = str(t.get("callId", ""))
                transcript = t.get("transcript", [])
                if call_id and isinstance(transcript, list):
                    yield call_id, transcript

def convert_to_sf_time(utc_time):
    if not utc_time:
        return "N/A"
    try:
        utc_dt = datetime.fromisoformat(utc_time.replace("Z", "+00:00"))
        sf_dt = utc_dt.astimezone(SF_TZ)
        return sf_dt.strftime("%m/%d/%y")
    except Exception:
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
            if object_type and get_field(obj, "objectType").lower() != object_type.lower():
                continue
            for field in obj.get("fields", []):
                if not isinstance(field, dict):
                    continue
                if get_field(field, "name").lower() == field_name.lower():
                    value = field.get("value")
                    if value is not None:
                        values.append(str(value))
    return values

def apply_occupancy_analytics_tags(call):
    fields = [
        get_field(call.get("metaData", {}), "title"),
        get_field(call.get("content", {}), "brief"),
        " ".join(str(get_field(kp, "description")) for kp in call.get("content", {}).get("keyPoints", []) if isinstance(kp, dict))
    ]
    text = " ".join(f for f in fields if f).lower()
    return any(pattern.search(text) for pattern in PRODUCT_MAPPINGS["Occupancy Analytics"])

def normalize_call_data(call, transcript):
    try:
        meta_data = call.get("metaData", {})
        content = call.get("content", {})
        parties = call.get("parties", [])
        context = call.get("context", [])

        call_id = get_field(meta_data, "id", "Unknown")
        call_title = get_field(meta_data, "title", "N/A")
        call_date = convert_to_sf_time(get_field(meta_data, "started"))
        account_name = extract_field_values(context, "Name", "Account")[0] if extract_field_values(context, "Name", "Account") else "Unknown"
        account_id = extract_field_values(context, "objectId", "Account")[0] if extract_field_values(context, "objectId", "Account") else "Unknown"
        account_website = extract_field_values(context, "Website", "Account")[0] if extract_field_values(context, "Website", "Account") else "Unknown"
        account_industry = extract_field_values(context, "Industry", "Account")[0] if extract_field_values(context, "Industry", "Account") else ""

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

        normalized_website = normalize_domain(account_website)
        is_owner_org = "yes" if normalized_website in TARGET_DOMAINS else "no"

        return {
            "call_id": call_id,
            "call_title": call_title,
            "call_date": call_date,
            "account_name": account_name,
            "account_id": account_id,
            "account_website": account_website,
            "account_industry": account_industry,
            "products": products,
            "parties": parties,
            "utterances": transcript or [],
            "partial_data": False,
            "owner_org": is_owner_org
        }
    except Exception as e:
        logger.error(f"Normalization error for call {get_field(call.get('metaData', {}), 'id', 'Unknown')}: {str(e)}")
        return {
            "call_id": get_field(call.get("metaData", {}), "id", "Unknown"),
            "call_title": "N/A",
            "call_date": "N/A",
            "account_name": "Unknown",
            "account_id": "Unknown",
            "account_website": "Unknown",
            "account_industry": "",
            "products": [],
            "parties": call.get("parties", []),
            "utterances": transcript or [],
            "partial_data": True,
            "owner_org": "no"
        }

def prepare_utterances_df(calls, selected_products):
    if not calls:
        return pd.DataFrame()
    data = []
    for call in calls:
        products = call.get("products", [])
        selected = [p.lower() for p in selected_products]
        products_lower = [p.lower() for p in products if isinstance(p, str)]
        if products and not any(p in selected for p in products_lower):
            continue
        speaker_info = {get_field(p, "speakerId"): p for p in call["parties"]}
        for utterance in sorted(call["utterances"] or [], key=lambda x: get_field(x, "start", 0)):
            text = " ".join(s.get("text", "") if isinstance(s, dict) else "" for s in (utterance.get("sentences", []) or []))
            if len(text.split()) <= 5:
                continue
            speaker = speaker_info.get(get_field(utterance, "speakerId"), {})
            affiliation = get_field(speaker, "affiliation", "unknown").lower()
            if affiliation == "internal":
                continue
            topic = get_field(utterance, "topic", "N/A")
            if topic.lower() in ["call setup", "small talk"]:
                continue
            data.append({
                "call_id": call["call_id"],
                "call_date": call["call_date"],
                "account_id": call["account_id"],
                "account_name": call["account_name"],
                "account_website": call["account_website"],
                "account_industry": call["account_industry"],
                "products": "|".join(products) if products else "",
                "owner_org": call["owner_org"],
                "speaker_name": get_field(speaker, "name", "Unknown"),
                "speaker_job_title": get_field(speaker, "jobTitle", ""),
                "speaker_affiliation": affiliation,
                "speaker_email_address": get_field(speaker, "emailAddress", ""),
                "utterance_text": text,
                "topic": topic
            })
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values(["call_date", "call_id"], ascending=[False, True])
    return df

def prepare_call_summary_df(calls, selected_products):
    if not calls:
        return pd.DataFrame()
    data = []
    for call in calls:
        products = call.get("products", [])
        selected = [p.lower() for p in selected_products]
        products_lower = [p.lower() for p in products if isinstance(p, str)]
        filtered_out = "yes" if products and not any(p in selected for p in products_lower) else "no"
        data.append({
            "call_id": call["call_id"],
            "call_date": call["call_date"],
            "filtered_out": filtered_out,
            "product_tags": "|".join(products) if products else "",
            "owner_org": call["owner_org"],
            "account_name": call["account_name"],
            "account_website": call["account_website"],
            "account_industry": call["account_industry"]
        })
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values("call_date", ascending=False)
    return df

def prepare_json_output(calls, selected_products):
    if not calls:
        return {"filtered_calls": [], "non_filtered_calls": []}
    filtered_calls = []
    non_filtered_calls = []
    for call in calls:
        products = call.get("products", [])
        selected = [p.lower() for p in selected_products]
        products_lower = [p.lower() for p in products if isinstance(p, str)]
        speaker_info = {get_field(p, "speakerId"): p for p in call["parties"]}
        call_data = {
            "call_id": call["call_id"],
            "call_date": call["call_date"],
            "product_tags": "|".join(products) if products else "",
            "owner_org": call["owner_org"],
            "account_name": call["account_name"],
            "account_website": call["account_website"],
            "account_industry": call["account_industry"],
            "utterances": [
                {
                    "timestamp": get_field(u, "start", "N/A"),
                    "speaker_name": get_field(speaker_info.get(get_field(u, "speakerId"), {}), "name", "Unknown"),
                    "speaker_affiliation": get_field(speaker_info.get(get_field(u, "speakerId"), {}), "affiliation", "unknown"),
                    "utterance_text": " ".join(s.get("text", "") if isinstance(s, dict) else "" for s in (u.get("sentences", []) or [])),
                    "topic": get_field(u, "topic", "N/A")
                } for u in sorted(call["utterances"] or [], key=lambda x: get_field(x, "start", 0))
            ]
        }
        if products and any(p in selected for p in products_lower):
            filtered_calls.append(call_data)
        else:
            non_filtered_calls.append(call_data)
    filtered_calls = sorted(filtered_calls, key=lambda x: datetime.strptime(x["call_date"], "%m/%d/%y") if x["call_date"] != "N/A" else datetime.min, reverse=True)
    non_filtered_calls = sorted(non_filtered_calls, key=lambda x: datetime.strptime(x["call_date"], "%m/%d/%y") if x["call_date"] != "N/A" else datetime.min, reverse=True)
    return {"filtered_calls": filtered_calls, "non_filtered_calls": non_filtered_calls}

@app.route('/')
def index():
    end_date = datetime.today()
    start_date = end_date - timedelta(days=7)
    return render_template('index.html', start_date=start_date.strftime('%Y-%m-%d'), end_date=end_date.strftime('%Y-%m-%d'), products=ALL_PRODUCT_TAGS, access_key="", secret_key="", message="", show_download=False)

@app.route('/health')
def health():
    """Health check endpoint for Render."""
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

    # Validate inputs
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
        logger.info(f"Fetched {len(call_ids)} call IDs")
        
        if not call_ids:
            form_state["message"] = "No calls found for the selected date range."
            return render_template('index.html', **form_state)

        full_data = []
        dropped_calls = 0
        transcripts = {}
        logger.info("Fetching transcripts")
        for call_id, transcript in client.fetch_transcript(call_ids):
            transcripts[call_id] = transcript
        logger.info(f"Fetched transcripts for {len(transcripts)} calls")

        logger.info("Fetching and normalizing call details")
        for call in client.fetch_call_details(call_ids):
            call_id = get_field(call.get("metaData", {}), "id")
            if not call_id:
                dropped_calls += 1
                continue
            normalized = normalize_call_data(call, transcripts.get(call_id, []))
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

        temp_dir = tempfile.mkdtemp()
        session['temp_dir'] = temp_dir
        start_date_str = start_dt.strftime("%d%b%y").lower()
        end_date_str = end_dt.strftime("%d%b%y").lower()
        session['utterances_path'] = os.path.join(temp_dir, f"utterances_gong_{start_date_str}_to_{end_date_str}.csv")
        session['call_summary_path'] = os.path.join(temp_dir, f"call_summary_gong_{start_date_str}_to_{end_date_str}.csv")
        session['json_path'] = os.path.join(temp_dir, f"call_data_gong_{start_date_str}_to_{end_date_str}.json")
        session['log_path'] = log_file_path

        utterances_df.to_csv(session['utterances_path'], index=False)
        call_summary_df.to_csv(session['call_summary_path'], index=False)
        with open(session['json_path'], 'w') as f:
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
    if 'utterances_path' not in session:
        return "No data", 400
    return send_file(
        session['utterances_path'],
        mimetype='text/csv',
        as_attachment=True,
        download_name=os.path.basename(session['utterances_path'])
    )

@app.route('/download/call_summary')
def download_call_summary():
    if 'call_summary_path' not in session:
        return "No data", 400
    return send_file(
        session['call_summary_path'],
        mimetype='text/csv',
        as_attachment=True,
        download_name=os.path.basename(session['call_summary_path'])
    )

@app.route('/download/json')
def download_json():
    if 'json_path' not in session:
        return "No data", 400
    return send_file(
        session['json_path'],
        mimetype='application/json',
        as_attachment=True,
        download_name=os.path.basename(session['json_path'])
    )

@app.route('/download/logs')
def download_logs():
    if 'log_path' not in session:
        return "No logs", 400
    return send_file(
        session['log_path'],
        mimetype='text/plain',
        as_attachment=True,
        download_name="logs.txt"
    )

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))  # Use Render's PORT or default to 10000
    app.run(host='0.0.0.0', port=port, debug=False)