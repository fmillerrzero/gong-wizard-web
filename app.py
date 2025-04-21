import pandas as pd
import requests
import base64
import json
import time
from datetime import datetime, timedelta, date
import logging
import pytz
import re
import io
from flask import Flask, render_template, request, session, Response

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

# Define product mappings
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
# Precompile regex patterns for efficiency
for product in PRODUCT_MAPPINGS:
    if product == "Occupancy Analytics":
        PRODUCT_MAPPINGS[product] = [re.compile(pattern, re.IGNORECASE) for pattern in PRODUCT_MAPPINGS[product]]

# Configuration
GONG_BASE_URL = "https://us-11211.api.gong.io/v2"
SF_TZ = pytz.timezone('America/Los_Angeles')

# Custom exception for Gong API errors
class GongAPIError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

# --- API Interaction Functions ---
class GongAPIClient:
    def __init__(self, access_key, secret_key):
        self.base_url = GONG_BASE_URL
        self.session = requests.Session()
        credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
        self.session.headers.update({"Authorization": f"Basic {credentials}"})

    def api_call(self, method, endpoint, **kwargs):
        """Unified function for API calls with retry logic."""
        url = f"{self.base_url}/{endpoint}"
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = self.session.request(method, url, **kwargs, timeout=30)
                logger.info(f"API Response status for {url}: {response.status_code}")
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (401, 403):
                    raise GongAPIError(f"Authentication failed: {response.text}")
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                    logger.warning(f"Rate limit hit, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                elif 500 <= response.status_code < 600:
                    logger.warning(f"Server error {response.status_code}, retrying...")
                    time.sleep(2 ** attempt * 2)
                    continue
                else:
                    raise GongAPIError(f"API error: {response.status_code} - {response.text}")
            except requests.RequestException as e:
                if attempt == max_attempts - 1:
                    raise GongAPIError(f"Network error: {str(e)}")
                time.sleep(2 ** attempt * 1)
        raise GongAPIError("Max retries exceeded")

    def fetch_call_list(self, from_date, to_date):
        """Fetch list of call IDs for a date range."""
        params = {"fromDateTime": from_date, "toDateTime": to_date}
        call_ids = []
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self.api_call("GET", "calls", params=params)
            calls = data.get("calls", [])
            call_ids.extend(str(call.get("metaData", {}).get("id", "")) for call in calls if call.get("metaData", {}).get("id"))
            cursor = data.get("records", {}).get("cursor")
            if not cursor:
                break
            time.sleep(1)
        return call_ids

    def fetch_call_details(self, call_ids):
        """Fetch detailed call data for a list of call IDs."""
        calls = []
        batch_size = 100
        for i in range(0, len(call_ids), batch_size):
            batch_ids = call_ids[i:i + batch_size]
            body = {
                "filter": {"callIds": batch_ids},
                "contentSelector": {
                    "context": "Extended",
                    "exposedFields": {
                        "parties": True,
                        "content": {"trackers": True, "brief": True, "keyPoints": True, "callOutcome": True, "highlights": True},
                        "media": True,
                        "crmAssociations": True
                    }
                }
            }
            data = self.api_call("POST", "calls/extensive", json=body)
            calls.extend(data.get("calls", []))
        return calls

    def fetch_transcript(self, call_ids):
        """Fetch transcripts for a list of call IDs."""
        transcripts = {}
        batch_size = 100
        for i in range(0, len(call_ids), batch_size):
            batch_ids = call_ids[i:i + batch_size]
            body = {"filter": {"callIds": batch_ids}}
            data = self.api_call("POST", "calls/transcript", json=body)
            for t in data.get("callTranscripts", []):
                call_id = str(t.get("callId", ""))
                transcript = t.get("transcript", [])
                if call_id and isinstance(transcript, list):
                    transcripts[call_id] = transcript
        return transcripts

# --- Data Processing Functions ---
def convert_to_sf_time(utc_time):
    """Convert UTC timestamp to San Francisco time (MM/DD/YY)."""
    try:
        utc_dt = datetime.fromisoformat(utc_time.replace("Z", "+00:00"))
        sf_dt = utc_dt.astimezone(SF_TZ)
        return sf_dt.strftime("%m/%d/%y")
    except Exception:
        return "N/A"

def get_field(data, key, default=""):
    """Safely get a field from a dictionary with case-insensitive keys."""
    if not isinstance(data, dict):
        return default
    for k in data:
        if k.lower() == key.lower():
            return data[k] or default
    return default

def extract_field_values(context, field_name, object_type=None):
    """Extract values for a field from context data."""
    values = []
    for ctx in context or []:
        for obj in ctx.get("objects", []):
            if object_type and get_field(obj, "objectType").lower() != object_type.lower():
                continue
            for field in obj.get("fields", []):
                if get_field(field, "name").lower() == field_name.lower():
                    value = field.get("value")
                    if value is not None:
                        values.append(str(value))
    return values

def apply_occupancy_analytics_tags(call):
    """Check if call matches Occupancy Analytics regex patterns."""
    fields = [
        get_field(call.get("metaData", {}), "title"),
        get_field(call.get("content", {}), "brief"),
        " ".join(str(get_field(kp, "description")) for kp in call.get("content", {}).get("keyPoints", []))
    ]
    text = " ".join(f for f in fields if f).lower()
    return any(pattern.search(text) for pattern in PRODUCT_MAPPINGS["Occupancy Analytics"])

def format_speaker(speaker):
    """Format speaker as 'Name, Title' or available part."""
    name = get_field(speaker, "name", "")
    title = get_field(speaker, "jobTitle", "")
    if name and title:
        return f"{name}, {title}"
    return name or title or ""

def normalize_call_data(call, transcript):
    """Normalize call data into a consistent structure."""
    if not call or not isinstance(call.get("metaData"), dict) or not get_field(call["metaData"], "id"):
        return None

    trackers = call.get("content", {}).get("trackers", [])
    tracker_counts = {}
    for tracker in trackers:
        tracker_name = get_field(tracker, "name").lower()
        tracker_count = tracker.get("count", 0)
        phrase_count = sum(phrase.get("count", 0) for phrase in tracker.get("phrases", []))
        tracker_counts[tracker_name] = max(tracker_count, phrase_count)

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

    context = call.get("context", [])
    account_names = extract_field_values(context, "Name", "Account")
    account_ids = extract_field_values(context, "objectId", "Account")
    account_websites = extract_field_values(context, "Website", "Account")
    industries = extract_field_values(context, "Industry", "Account")
    opportunity_names = extract_field_values(context, "Name", "Opportunity")

    # Identify primary speakers by affiliation
    parties = call.get("parties", [])
    external_speaker = next((p for p in parties if get_field(p, "affiliation").lower() == "external"), {})
    internal_speaker = next((p for p in parties if get_field(p, "affiliation").lower() == "internal"), {})
    unknown_speaker = next((p for p in parties if get_field(p, "affiliation").lower() == "unknown"), {})

    # Collect non-product trackers
    other_topics = sorted(
        [name for name, count in tracker_counts.items() if count > 0 and not any(name in [t.lower() for t in PRODUCT_MAPPINGS[p]] for p in PRODUCT_MAPPINGS if p != "Occupancy Analytics")],
        key=lambda x: tracker_counts[x],
        reverse=True
    )

    return {
        "metaData": call.get("metaData", {}),
        "content": call.get("content", {}),
        "parties": parties,
        "utterances": transcript or [],
        "products": products,
        "account_name": account_names[0] if account_names else "Unknown",
        "account_id": account_ids[0] if account_ids else "Unknown",
        "account_website": account_websites[0] if account_websites else "Unknown",
        "account_industry": industries[0] if industries else "",
        "opportunity_name": opportunity_names[0] if opportunity_names else "",
        "call_outcome": get_field(call.get("content", {}), "callOutcome", "N/A"),
        "highlights": call.get("content", {}).get("highlights", []),
        "primary_external_speaker": format_speaker(external_speaker),
        "primary_internal_speaker": format_speaker(internal_speaker),
        "primary_unknown_speaker": format_speaker(unknown_speaker),
        "other_topics": other_topics
    }

def prepare_utterances_df(calls, selected_products):
    """Prepare DataFrame for filtered utterances, sorted newest first."""
    data = []
    for call in calls:
        products = call.get("products", [])
        if not any(p in selected_products for p in products):
            continue
        speaker_info = {get_field(p, "speakerId"): p for p in call["parties"]}
        for utterance in call["utterances"]:
            text = " ".join(s.get("text", "") for s in utterance.get("sentences", []))
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
                "call_id": f'"{get_field(call["metaData"], "id")}"',
                "call_title": get_field(call["metaData"], "title", "N/A"),
                "call_date": convert_to_sf_time(get_field(call["metaData"], "started")),
                "account_id": call["account_id"],
                "account_name": call["account_name"],
                "account_website": call["account_website"],
                "account_industry": call["account_industry"],
                "products": "|".join(products),
                "speaker_name": get_field(speaker, "name", "Unknown"),
                "speaker_job_title": get_field(speaker, "jobTitle", ""),
                "speaker_affiliation": affiliation,
                "speaker_email_address": get_field(speaker, "emailAddress", ""),
                "utterance_text": text,
                "topic": topic
            })
    df = pd.DataFrame(data)
    if not df.empty:
        df.sort_values(by=["call_date", "call_id"], ascending=[False, True], inplace=True)
    return df

def prepare_call_summary_df(calls, selected_products):
    """Prepare DataFrame for call summaries, sorted newest first."""
    data = []
    for call in calls:
        products = call.get("products", [])
        filtered_out = "yes" if not any(p in selected_products for p in products) else "no"
        data.append({
            "call_id": f'"{get_field(call["metaData"], "id")}"',
            "call_date": convert_to_sf_time(get_field(call["metaData"], "started")),
            "filtered_out": filtered_out,
            "product_tags": "|".join(sorted(products)),
            "other_topics": "|".join(call["other_topics"]),
            "primary_external_speaker": call["primary_external_speaker"],
            "primary_internal_speaker": call["primary_internal_speaker"],
            "primary_unknown_speaker": call["primary_unknown_speaker"],
            "account_name": call["account_name"],
            "account_website": call["account_website"],
            "account_industry": call["account_industry"]
        })
    df = pd.DataFrame(data)
    if not df.empty:
        df.sort_values(by="call_date", ascending=False, inplace=True)
    return df

def prepare_json_output(calls, selected_products):
    """Prepare JSON output for filtered and non-filtered calls, sorted newest first."""
    filtered_calls = []
    non_filtered_calls = []
    for call in calls:
        products = call.get("products", [])
        call_data = {
            "call_id": f'"{get_field(call["metaData"], "id")}"',
            "call_title": get_field(call["metaData"], "title", "N/A"),
            "call_date": convert_to_sf_time(get_field(call["metaData"], "started")),
            "call_duration": get_field(call["metaData"], "duration", "N/A"),
            "call_summary": get_field(call["content"], "brief", "N/A"),
            "key_points": [get_field(kp, "description", "") for kp in call["content"].get("keyPoints", [])],
            "product_tags": products,
            "other_topics": call["other_topics"],
            "account_name": call["account_name"],
            "account_website": call["account_website"],
            "account_industry": call["account_industry"],
            "opportunity_name": call["opportunity_name"],
            "primary_internal_speaker": call["primary_internal_speaker"],
            "primary_external_speaker": call["primary_external_speaker"],
            "primary_unknown_speaker": call["primary_unknown_speaker"],
            "utterances": [
                {
                    "timestamp": get_field(u, "start", "N/A"),
                    "speaker_name": get_field(speaker_info.get(get_field(u, "speakerId"), {}), "name", "Unknown"),
                    "speaker_title": get_field(speaker_info.get(get_field(u, "speakerId"), {}), "jobTitle", ""),
                    "speaker_affiliation": get_field(speaker_info.get(get_field(u, "speakerId"), {}), "affiliation", "unknown"),
                    "speaker_email": get_field(speaker_info.get(get_field(u, "speakerId"), {}), "emailAddress", ""),
                    "utterance_text": " ".join(s.get("text", "") for s in u.get("sentences", [])),
                    "topic": get_field(u, "topic", "N/A")
                } for u in call["utterances"] if len(" ".join(s.get("text", "") for s in u.get("sentences", [])).split()) > 5
                and get_field(speaker_info.get(get_field(u, "speakerId"), {}), "affiliation", "unknown").lower() != "internal"
                and get_field(u, "topic", "N/A").lower() not in ["call setup", "small talk"]
            ]
        }
        speaker_info = {get_field(p, "speakerId"): p for p in call["parties"]}
        if any(p in selected_products for p in products):
            filtered_calls.append(call_data)
        else:
            non_filtered_calls.append(call_data)
    filtered_calls.sort(key=lambda x: x["call_date"], reverse=True)
    non_filtered_calls.sort(key=lambda x: x["call_date"], reverse=True)
    return {"filtered_calls": filtered_calls, "non_filtered_calls": non_filtered_calls}

# --- Flask Routes ---
@app.route('/')
def index():
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    return render_template('index.html', start_date=start_date.strftime('%Y-%m-%d'), end_date=end_date.strftime('%Y-%m-%d'), products=ALL_PRODUCT_TAGS)

@app.route('/process', methods=['POST'])
def process():
    access_key = request.form.get('access_key')
    secret_key = request.form.get('secret_key')
    products = request.form.getlist('products') or ALL_PRODUCT_TAGS
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')

    if not access_key or not secret_key:
        return render_template('index.html', message="Missing API keys", start_date=start_date, end_date=end_date, products=products)

    try:
        client = GongAPIClient(access_key, secret_key)
        start_date_utc = datetime.strptime(start_date, '%Y-%m-%d').isoformat() + "Z"
        end_date_utc = datetime.strptime(end_date, '%Y-%m-%d').isoformat() + "Z"
        call_ids = client.fetch_call_list(start_date_utc, end_date_utc)
        if not call_ids:
            return render_template('index.html', message="No calls found", start_date=start_date, end_date=end_date, products=products)

        details = client.fetch_call_details(call_ids)
        transcripts = client.fetch_transcript(call_ids)

        full_data = []
        for call in details:
            call_id = get_field(call.get("metaData", {}), "id")
            if call_id:
                normalized = normalize_call_data(call, transcripts.get(call_id, []))
                if normalized:
                    full_data.append(normalized)

        if not full_data:
            return render_template('index.html', message="No valid call data", start_date=start_date, end_date=end_date, products=products)

        utterances_df = prepare_utterances_df(full_data, products)
        call_summary_df = prepare_call_summary_df(full_data, products)
        json_data = prepare_json_output(full_data, products)

        session['utterances_csv'] = utterances_df.to_csv(index=False)
        session['call_summary_csv'] = call_summary_df.to_csv(index=False)
        session['json_data'] = json.dumps(json_data, indent=2)

        return render_template('index.html', message="Processing complete", show_download=True, start_date=start_date, end_date=end_date, products=products)
    except GongAPIError as e:
        return render_template('index.html', message=f"API Error: {e.message}", start_date=start_date, end_date=end_date, products=products)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return render_template('index.html', message="Unexpected error", start_date=start_date, end_date=end_date, products=products)

@app.route('/download/utterances')
def download_utterances():
    if 'utterances_csv' not in session:
        return "No data", 400
    return Response(session['utterances_csv'], mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=utterances.csv"})

@app.route('/download/call_summary')
def download_call_summary():
    if 'call_summary_csv' not in session:
        return "No data", 400
    return Response(session['call_summary_csv'], mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=call_summary.csv"})

@app.route('/download/json')
def download_json():
    if 'json_data' not in session:
        return "No data", 400
    return Response(session['json_data'], mimetype='application/json', headers={"Content-Disposition": "attachment;filename=data.json"})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000, debug=True)