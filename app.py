import pandas as pd
import requests
import base64
import json
import time
from datetime import datetime, timedelta, date
import logging
import pytz
import re
from io import StringIO
from flask import Flask, render_template, request, session, Response

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
log_stream = StringIO()
handler = logging.StreamHandler(log_stream)
logger.addHandler(handler)

# Constants
GONG_BASE_URL = "https://us-11211.api.gong.io/v2"
SF_TZ = pytz.timezone('America/Los_Angeles')

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
                elif response.status_code == 401:
                    logger.error("Authentication failed: Invalid API keys or insufficient permissions")
                    return None
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                    logger.warning(f"Rate limit hit, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"API error: {response.status_code} - {response.text}")
                    return None
            except requests.RequestException as e:
                if attempt == max_attempts - 1:
                    logger.error(f"Network error: {str(e)}")
                    return None
                time.sleep(2 ** attempt * 1)
        return None

    def fetch_call_list(self, from_date, to_date):
        params = {"fromDateTime": from_date, "toDateTime": to_date}
        call_ids = []
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self.api_call("GET", "calls", params=params)
            if not data:
                logger.error("Failed to fetch call list")
                break
            calls = data.get("calls", [])
            call_ids.extend(str(call.get("id", "")) for call in calls if call.get("id"))
            cursor = data.get("records", {}).get("cursor")
            if not cursor:
                break
            time.sleep(1)
        logger.info(f"Fetched {len(call_ids)} call IDs")
        return call_ids

    def fetch_call_details(self, call_ids):
        batch_size = 10  # Smaller batch size to limit memory usage
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
            if data:
                for call in data.get("calls", []):
                    yield call

    def fetch_transcript(self, call_ids):
        batch_size = 10  # Smaller batch size to limit memory usage
        for i in range(0, len(call_ids), batch_size):
            batch_ids = call_ids[i:i + batch_size]
            body = {"filter": {"callIds": batch_ids}}
            data = self.api_call("POST", "calls/transcript", json=body)
            if data:
                for t in data.get("callTranscripts", []):
                    call_id = str(t.get("callId", ""))
                    transcript = t.get("transcript", [])
                    if call_id and isinstance(transcript, list):
                        yield call_id, transcript

# Helper functions
def convert_to_sf_time(utc_time):
    if utc_time is None:
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
            return v or default
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
        "utterances": transcript or []
    }

# Output preparation functions
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
    return {"filtered_calls": filtered_calls, "non_filtered_calls": non_filtered_calls}

# Flask routes
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

    # Clear session to prevent large cookie size
    session.clear()

    # Validate date inputs
    if not start_date or not end_date:
        log_data = log_stream.getvalue()
        message = "Missing start or end date.<br><br>Logs:<br>" + log_data.replace('\n', '<br>')
        return render_template('index.html', message=message, start_date=start_date, end_date=end_date, products=products)

    # Validate date format
    date_format = '%Y-%m-%d'
    try:
        datetime.strptime(start_date, date_format)
        datetime.strptime(end_date, date_format)
    except ValueError:
        log_data = log_stream.getvalue()
        message = "Invalid date format. Use YYYY-MM-DD.<br><br>Logs:<br>" + log_data.replace('\n', '<br>')
        return render_template('index.html', message=message, start_date=start_date, end_date=end_date, products=products)

    if not access_key or not secret_key:
        log_data = log_stream.getvalue()
        message = "Missing API keys.<br><br>Logs:<br>" + log_data.replace('\n', '<br>')
        return render_template('index.html', message=message, start_date=start_date, end_date=end_date, products=products)

    try:
        client = GongAPIClient(access_key, secret_key)
        utc = pytz.UTC
        start_dt = utc.localize(datetime.strptime(start_date, '%Y-%m-%d'))
        end_dt = utc.localize(datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59))
        start_date_utc = start_dt.isoformat().replace('+00:00', 'Z')
        end_date_utc = end_dt.isoformat().replace('+00:00', 'Z')
        call_ids = client.fetch_call_list(start_date_utc, end_date_utc)
        if not call_ids:
            log_data = log_stream.getvalue()
            message = "No calls found.<br><br>Logs:<br>" + log_data.replace('\n', '<br>')
            return render_template('index.html', message=message, start_date=start_date, end_date=end_date, products=products)

        # Process calls in batches to avoid memory issues
        full_data = []
        transcripts = {}
        for call_id, transcript in client.fetch_transcript(call_ids):
            transcripts[call_id] = transcript

        for call in client.fetch_call_details(call_ids):
            call_id = get_field(call.get("metaData", {}), "id")
            if call_id:
                normalized = normalize_call_data(call, transcripts.get(call_id, []))
                if normalized:
                    full_data.append(normalized)
                    # Clear memory periodically
                    if len(full_data) >= 10:
                        utterances_df = prepare_utterances_df(full_data, products)
                        call_summary_df = prepare_call_summary_df(full_data, products)
                        json_data = prepare_json_output(full_data, products)
                        # Append to session incrementally
                        session['utterances_csv'] = (session.get('utterances_csv', '') + utterances_df.to_csv(index=False))[:10000]
                        session['call_summary_csv'] = (session.get('call_summary_csv', '') + call_summary_df.to_csv(index=False))[:10000]
                        session['json_data'] = (session.get('json_data', '') + json.dumps(json_data, indent=2, default=str))[:10000]
                        full_data = []  # Reset to free memory

        # Process any remaining data
        if full_data:
            utterances_df = prepare_utterances_df(full_data, products)
            call_summary_df = prepare_call_summary_df(full_data, products)
            json_data = prepare_json_output(full_data, products)
            session['utterances_csv'] = (session.get('utterances_csv', '') + utterances_df.to_csv(index=False))[:10000]
            session['call_summary_csv'] = (session.get('call_summary_csv', '') + call_summary_df.to_csv(index=False))[:10000]
            session['json_data'] = (session.get('json_data', '') + json.dumps(json_data, indent=2, default=str))[:10000]

        return render_template('index.html', message="Processing complete", show_download=True, start_date=start_date, end_date=end_date, products=products)
    except Exception as e:
        logger.error(f"Error in process: {str(e)}")
        log_data = log_stream.getvalue()
        message = "Unexpected error: " + str(e) + ".<br><br>Logs:<br>" + log_data.replace('\n', '<br>')
        return render_template('index.html', message=message, start_date=start_date, end_date=end_date, products=products)

@app.route('/download/utterances')
def download_utterances():
    if 'utterances_csv' not in session:
        return "No data", 400
    response = Response(session['utterances_csv'], mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=utterances.csv"})
    session.pop('utterances_csv', None)  # Clear after download
    return response

@app.route('/download/call_summary')
def download_call_summary():
    if 'call_summary_csv' not in session:
        return "No data", 400
    response = Response(session['call_summary_csv'], mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=call_summary.csv"})
    session.pop('call_summary_csv', None)  # Clear after download
    return response

@app.route('/download/json')
def download_json():
    if 'json_data' not in session:
        return "No data", 400
    response = Response(session['json_data'], mimetype='application/json', headers={"Content-Disposition": "attachment;filename=data.json"})
    session.pop('json_data', None)  # Clear after download
    return response

@app.route('/download/logs')
def download_logs():
    log_data = log_stream.getvalue()
    return Response(log_data, mimetype='text/plain', headers={"Content-Disposition": "attachment;filename=logs.txt"})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)