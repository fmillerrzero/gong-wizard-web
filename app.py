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
from flask import Flask, render_template, request, send_file
import io

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
GONG_API_BASE = "https://us-11211.api.gong.io/v2"
SF_TZ = pytz.timezone('America/Los_Angeles')

# Product mappings
PRODUCT_MAPPINGS = {
    "IAQ Monitoring": ["Air Quality"],
    "ODCV": ["ODCV"],
    "Secure Air": ["Filter", "Filtration"],
    "Occupancy Analytics": [
        r'capacity',
        r'connect[\s-]?(dashboard|platform)',
        r'cowork(ers?|r)',
        r'density',
        r'dwell[\s-]?time',
        r'group[\s-]?sizes?',
        r'hot[\s-]?desks?',
        r'occupancy[\s-]?analytics',
        r'real[\s-]?time[\s-]?apis?',
        r'real[\s-]?time[\s-]?occupancy',
        r'room[\s-]?reservations?',
        r'space[\s-]?types?',
        r'stream[\s-]?apis?',
        r'utilizations?',
        r'vergesense',
        r'workplace[\s-]?(strategy|strategists)',
        r'heat[\s-]?maps?'
    ]
}
ALL_PRODUCT_TAGS = list(PRODUCT_MAPPINGS.keys())

class GongAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Gong API Error {status_code}: {message}")

def create_auth_header(access_key: str, secret_key: str) -> Dict[str, str]:
    if not access_key or not secret_key:
        raise ValueError("Access key and secret key must not be empty")
    credentials = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
    return {"Authorization": f"Basic {credentials}"}

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

def fetch_call_list(session: requests.Session, from_date: str, to_date: str) -> List[str]:
    url = f"{GONG_API_BASE}/calls"
    params = {"fromDateTime": from_date, "toDateTime": to_date}
    call_ids = []
    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            page_params = dict(params)
            while True:
                response = session.get(url, params=page_params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    call_ids.extend(str(call["id"]) for call in data.get("calls", []))
                    cursor = data.get("pagination", {}).get("next")
                    if not cursor:
                        break
                    page_params["cursor"] = cursor
                    time.sleep(1)
                elif response.status_code in (401, 403):
                    raise GongAPIError(response.status_code, f"Authentication failed: {response.text}")
                elif is_retryable_error(response.status_code):
                    wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                    if response.status_code == 429 and "daily limit" in response.text.lower():
                        raise GongAPIError(429, "Daily API call limit (10,000) exceeded. Try again tomorrow.")
                    logger.warning(f"Retryable error {response.status_code}, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    raise GongAPIError(response.status_code, f"API error: {response.text}")
            break
        except requests.RequestException as e:
            if attempt < max_attempts - 1:
                time.sleep((2 ** attempt) * 1)
            else:
                raise GongAPIError(0, f"Network error: {str(e)}")
    return call_ids

def fetch_call_details(session: requests.Session, call_ids: List[str]) -> List[Dict[str, Any]]:
    url = f"{GONG_API_BASE}/calls/extensive"
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
                    response = session.post(url, json=request_body, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        call_details.extend(data.get("calls", []))
                        cursor = data.get("pagination", {}).get("next")
                        if not cursor:
                            break
                        time.sleep(1)
                        break
                    elif response.status_code in (401, 403):
                        raise GongAPIError(response.status_code, "Permission denied: Check API key permissions or required scopes (api:calls:read:extensive, api:calls:read:media-url).")
                    elif is_retryable_error(response.status_code):
                        wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                        if response.status_code == 429 and "daily limit" in response.text.lower():
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
    return call_details

def fetch_transcript(session: requests.Session, call_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    url = f"{GONG_API_BASE}/calls/transcript"
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
                    response = session.post(url, json=request_body, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        transcripts = data.get("callTranscripts", [])
                        for t in transcripts:
                            call_id = t.get("callId")
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
                        raise GongAPIError(response.status_code, "Permission denied: Check API key permissions or required scopes (api:calls:read:extensive, api:calls:read:media-url).")
                    elif is_retryable_error(response.status_code):
                        wait_time = int(response.headers.get("Retry-After", (2 ** attempt) * 2))
                        if response.status_code == 429 and "daily limit" in response.text.lower():
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
        call.get("metaData", {}).get("title", ""),
        call.get("content", {}).get("brief", ""),
        call.get("context", [{}])[0].get("objects", [{}])[0].get("fields", [{}])[0].get("value", ""),
        " ".join(km.get("description", "") for km in call.get("content", {}).get("keyPoints", []))
    ]
    for field in fields:
        if field is None:
            continue
        for pattern in PRODUCT_MAPPINGS["Occupancy Analytics"]:
            if re.search(pattern, str(field).lower()):
                return True
    return False

def normalize_call_data(call_data: Dict[str, Any], transcript: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not call_data or "metaData" not in call_data or not call_data.get("metaData", {}).get("id"):
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

    account_context = next((ctx for ctx in processed_data["context"] if any(obj.get("objectType") == "Account" for obj in ctx.get("objects", []))), {})
    account_name = "Unknown"
    account_id = "Unknown"
    account_website = "Unknown"
    for obj in account_context.get("objects", []):
        if obj.get("objectType") == "Account":
            account_id = str(obj.get("objectId", "Unknown"))
            for field in obj.get("fields", []):
                if field.get("name") == "Name":
                    account_name = field.get("value", "Unknown")
                if field.get("name") == "Website":
                    account_website = field.get("value", "Unknown")
                if field.get("name") == "Industry":
                    processed_data["account_industry"] = field.get("value", "")

    opportunity_context = next((ctx for ctx in processed_data["context"] if any(obj.get("objectType") == "Opportunity" for obj in ctx.get("objects", []))), {})
    for obj in opportunity_context.get("objects", []):
        if obj.get("objectType") == "Opportunity":
            for field in obj.get("fields", []):
                if field.get("name") == "Name" and field.get("value"):
                    processed_data["opportunity_name"] = field.get("value")
                    break
            if processed_data["opportunity_name"]:
                break

    processed_data["account_name"] = account_name
    processed_data["account_id"] = account_id
    processed_data["account_website"] = account_website

    trackers = processed_data.get("content", {}).get("trackers", [])
    tracker_counts = {t.get("name", ""): t.get("count", 0) for t in trackers if isinstance(t, dict)}
    for product, trackers_or_patterns in PRODUCT_MAPPINGS.items():
        if product == "Occupancy Analytics":
            if apply_occupancy_analytics_tags(call_data):
                processed_data["products"].append(product)
        else:
            for tracker in trackers_or_patterns:
                if tracker in tracker_counts and tracker_counts[tracker] > 0:
                    processed_data["products"].append(product)
                    break

    for tracker in trackers:
        if tracker.get("count", 0) > 0 and tracker.get("name") not in sum([PRODUCT_MAPPINGS[p] for p in PRODUCT_MAPPINGS if p != "Occupancy Analytics"], []):
            processed_data["other_topics"].append({"name": tracker.get("name"), "count": tracker.get("count")})

    return processed_data

def format_speaker(speaker: Dict[str, Any]) -> str:
    name = speaker.get("name", "").strip()
    title = speaker.get("jobTitle", "").strip()
    if name and title:
        return f"{name}, {title}"
    return name or title or ""

def get_primary_speakers(call: Dict[str, Any]) -> Tuple[str, str, str]:
    speaker_counts = {}
    for utterance in call.get("utterances", []):
        speaker_id = utterance.get("speakerId", "")
        if speaker_id:
            speaker_counts[speaker_id] = speaker_counts.get(speaker_id, 0) + 1

    internal_speaker = external_speaker = unknown_speaker = ""
    max_internal = max_external = max_unknown = 0

    for party in call.get("parties", []):
        speaker_id = party.get("speakerId", "")
        if speaker_id not in speaker_counts:
            continue
        count = speaker_counts[speaker_id]
        affiliation = party.get("affiliation", "unknown").lower()

        if affiliation == "internal" and count > max_internal:
            internal_speaker = format_speaker(party)
            max_internal = count
        elif affiliation == "external" and count > max_external:
            external_speaker = format_speaker(party)
            max_external = count
        elif affiliation == "unknown" and count > max_unknown:
            unknown_speaker = format_speaker(party)
            max_unknown = count

    return internal_speaker, external_speaker, unknown_speaker

def prepare_call_summary_df(calls: List[Dict[str, Any]], selected_products: List[str]) -> pd.DataFrame:
    data = []
    for call in calls:
        if not call or "metaData" not in call:
            continue

        call_id = f'"{str(call["metaData"].get("id", ""))}"'
        call_date = convert_to_sf_time(call["metaData"].get("started", ""))
        products = sorted(set(p for p in call.get("products", []) if p in ALL_PRODUCT_TAGS))
        products_str = "|".join(products) if products else "none"
        other_topics = sorted(call.get("other_topics", []), key=lambda x: x["count"], reverse=True)
        other_topics_str = "|".join(t["name"] for t in other_topics) if other_topics else "none"
        internal_speaker, external_speaker, unknown_speaker = get_primary_speakers(call)

        filtered_out = "yes" if products and not any(p in selected_products for p in products) and "Select All" not in selected_products else "no"

        data.append({
            "call_id": call_id,
            "call_date": call_date,
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

        call_id = f'"{str(call["metaData"].get("id", ""))}"'
        call_title = call["metaData"].get("title", "N/A")
        call_date = convert_to_sf_time(call["metaData"].get("started", ""))
        account_id = call.get("account_id", "N/A")
        account_name = call.get("account_name", "N/A")
        account_website = call.get("account_website", "N/A")
        account_industry = call.get("account_industry", "")
        products = sorted(set(p for p in call.get("products", []) if p in ALL_PRODUCT_TAGS))
        products_str = "|".join(products) if products else "none"

        if products and not any(p in selected_products for p in products) and "Select All" not in selected_products:
            continue

        speaker_info = {p.get("speakerId", ""): p for p in call.get("parties", [])}
        for utterance in sorted(call.get("utterances", []), key=lambda x: x.get("sentences", [{}])[0].get("start", 0)):
            sentences = utterance.get("sentences", [])
            if not sentences:
                continue

            text = " ".join(s.get("text", "") for s in sentences)
            if len(text.split()) <= 5:
                continue

            speaker_id = utterance.get("speakerId", "")
            speaker = speaker_info.get(speaker_id, {})
            affiliation = speaker.get("affiliation", "unknown").lower()
            if affiliation == "internal":
                continue

            topic = utterance.get("topic", "N/A")
            if topic.lower() in ["call setup", "small talk"]:
                continue

            start_time = sentences[0].get("start", 0)
            end_time = sentences[-1].get("end", 0)
            duration = (end_time - start_time) // 60000

            data.append({
                "call_id": call_id,
                "call_title": call_title,
                "call_date": call_date,
                "account_id": account_id,
                "account_name": account_name,
                "account_website": account_website,
                "account_industry": account_industry,
                "products": products_str,
                "speaker_name": speaker.get("name", "Unknown"),
                "speaker_job_title": speaker.get("jobTitle", ""),
                "speaker_affiliation": affiliation,
                "speaker_email_address": speaker.get("emailAddress", ""),
                "utterance_duration": duration,
                "utterance_text": text,
                "topic": topic,
                "quality": "high"
            })

    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values(["call_date", "call_id"], ascending=[False, True])
    return df

def prepare_json_output(calls: List[Dict[str, Any]], selected_products: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    filtered_calls = []
    non_filtered_calls = []

    for call in sorted(calls, key=lambda x: x["metaData"].get("started", ""), reverse=True):
        if not call or "metaData" not in call:
            continue

        call_id = f'"{str(call["metaData"].get("id", ""))}"'
        call_date = convert_to_sf_time(call["metaData"].get("started", ""))
        products = sorted(set(p for p in call.get("products", []) if p in ALL_PRODUCT_TAGS))
        if products and not any(p in selected_products for p in products) and "Select All" not in selected_products:
            is_filtered = False
        else:
            is_filtered = True

        call_data = {
            "call_id": call_id,
            "call_title": call["metaData"].get("title", "N/A"),
            "call_date": call_date,
            "call_duration": (call["metaData"].get("duration", 0) // 60000),
            "call_summary": call["content"].get("brief", "N/A"),
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

        speaker_info = {p.get("speakerId", ""): p for p in call.get("parties", [])}
        for utterance in sorted(call.get("utterances", []), key=lambda x: x.get("sentences", [{}])[0].get("start", 0)):
            sentences = utterance.get("sentences", [])
            if not sentences:
                continue

            text = " ".join(s.get("text", "") for s in sentences)
            speaker_id = utterance.get("speakerId", "")
            speaker = speaker_info.get(speaker_id, {})
            start_time = sentences[0].get("start", 0)
            end_time = sentences[-1].get("end", 0)

            call_data["utterances"].append({
                "timestamp": (start_time // 60000),
                "speaker_name": speaker.get("name", "Unknown"),
                "speaker_title": speaker.get("jobTitle", ""),
                "speaker_affiliation": speaker.get("affiliation", "unknown").lower(),
                "speaker_email": speaker.get("emailAddress", ""),
                "utterance_text": text,
                "topic": utterance.get("topic", "N/A")
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
    return render_template('index.html', start_date=start_date, end_date=end_date)

@app.route('/process', methods=['POST'])
def process():
    access_key = request.form['access_key']
    secret_key = request.form['secret_key']
    time_range = request.form['time_range']
    products = request.form.getlist('products')

    if not access_key or not secret_key:
        return render_template('index.html', message="Please provide both Gong Access Key and Secret Key.")

    if time_range == 'last7':
        start_date = date.today() - timedelta(days=7)
        end_date = date.today()
    elif time_range == 'last30':
        start_date = date.today() - timedelta(days=30)
        end_date = date.today()
    elif time_range == 'last90':
        start_date = date.today() - timedelta(days=90)
        end_date = date.today()
    else:
        try:
            start_date = datetime.strptime(request.form['start_date'], '%Y-%m-%d').date()
            end_date = datetime.strptime(request.form['end_date'], '%Y-%m-%d').date()
        except ValueError:
            return render_template('index.html', message="Invalid date format. Use YYYY-MM-DD.")

    if start_date > end_date:
        return render_template('index.html', message="Start date cannot be after end date.")

    try:
        session = requests.Session()
        session.headers.update(create_auth_header(access_key, secret_key))

        call_ids = fetch_call_list(
            session,
            start_date.isoformat() + "T00:00:00Z",
            end_date.isoformat() + "T23:59:59Z"
        )

        if not call_ids:
            return render_template('index.html', message="No calls found for the selected date range.")

        details = fetch_call_details(session, call_ids)
        transcripts = fetch_transcript(session, call_ids)

        full_data = []
        dropped_calls = 0
        for i, call in enumerate(details):
            call_id = str(call.get("metaData", {}).get("id", ""))
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
            return render_template('index.html', message="No valid call data retrieved.")

        utterances_df = prepare_utterances_df(full_data, products)
        call_summary_df = prepare_call_summary_df(full_data, products)
        json_data = prepare_json_output(full_data, products)

        start_date_str = start_date.strftime("%d%b%y").lower()
        end_date_str = end_date.strftime("%d%b%y").lower()

        # Store dataframes and JSON in memory for download
        utterances_csv = utterances_df.to_csv(index=False, encoding='utf-8-sig')
        call_summary_csv = call_summary_df.to_csv(index=False, encoding='utf-8-sig')
        json_output = json.dumps(json_data, indent=4, ensure_ascii=False, default=str)

        # Save files temporarily for download (in memory)
        global utterances_file, call_summary_file, json_file
        utterances_file = io.BytesIO(utterances_csv.encode('utf-8'))
        call_summary_file = io.BytesIO(call_summary_csv.encode('utf-8'))
        json_file = io.BytesIO(json_output.encode('utf-8'))

        message += f"\nTotal calls processed: {len(full_data)}\nFiltered utterances: {len(utterances_df)}"
        return render_template('index.html', message=message, show_download=True)

    except GongAPIError as e:
        return render_template('index.html', message=f"API Error: {e.message}")
    except Exception as e:
        logger.exception("Unexpected error in process")
        return render_template('index.html', message=f"Unexpected error: {str(e)}")

@app.route('/download/utterances')
def download_utterances():
    start_date_str = (date.today() - timedelta(days=7)).strftime("%d%b%y").lower()
    end_date_str = date.today().strftime("%d%b%y").lower()
    return send_file(
        utterances_file,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f"filtered_utterances_gong_{start_date_str}_to_{end_date_str}.csv"
    )

@app.route('/download/call_summary')
def download_call_summary():
    start_date_str = (date.today() - timedelta(days=7)).strftime("%d%b%y").lower()
    end_date_str = date.today().strftime("%d%b%y").lower()
    return send_file(
        call_summary_file,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f"call_summary_gong_{start_date_str}_to_{end_date_str}.csv"
    )

@app.route('/download/json')
def download_json():
    start_date_str = (date.today() - timedelta(days=7)).strftime("%d%b%y").lower()
    end_date_str = date.today().strftime("%d%b%y").lower()
    return send_file(
        json_file,
        mimetype='application/json',
        as_attachment=True,
        download_name=f"call_data_gong_{start_date_str}_to_{end_date_str}.json"
    )

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)