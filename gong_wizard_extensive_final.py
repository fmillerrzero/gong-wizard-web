#!/usr/bin/env python3
"""
Forrest's Gong GPT Builder - Professional Edition
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import requests
import json
import csv
import os
import threading
import time
from datetime import datetime
import logging
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fuzzywuzzy import fuzz
from urllib.parse import urlparse

# Setup logging
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
log_file = f"{log_dir}/gong_wizard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = "gong_wizard_config.json"
BASE_URL = "https://us-11211.api.gong.io"
FETCH_STATS = {"started_at": None, "completed_at": None, "total_calls": 0}

def create_session(max_retries):
    session = requests.Session()
    retry_strategy = Retry(total=max_retries, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def format_duration(value):
    if isinstance(value, int):
        minutes = value // 60
        seconds = value % 60
        return f"{minutes} min {seconds} sec"
    return 'N/A'

def load_normalized_orgs():
    try:
        with open("normalized_orgs.csv", newline='', encoding='utf-8') as csvfile:
            return list(csv.DictReader(csvfile))
    except FileNotFoundError:
        logger.error("normalized_orgs.csv not found.")
        return []
    except Exception as e:
        logger.error(f"Error loading normalized_orgs.csv: {str(e)}")
        return []

def load_industry_mapping():
    try:
        with open("industry_mapping.csv", newline='', encoding='utf-8') as csvfile:
            return {row["Industry (API)"]: row["Industry (Normalized)"] for row in csv.DictReader(csvfile)}
    except FileNotFoundError:
        logger.error("industry_mapping.csv not found.")
        return {}
    except Exception as e:
        logger.error(f"Error loading industry_mapping.csv: {str(e)}")
        return {}

normalized_orgs = load_normalized_orgs()
industry_mapping = load_industry_mapping()

def normalize_org(account_name, website, industry_api):
    domain = urlparse(website).netloc.lower() if website and website != 'N/A' else ''
    for org in normalized_orgs:
        if domain and org.get("Primary external org domain", "").lower() == domain:
            return org.get("Org name", account_name), org.get("FINAL", industry_api), industry_api
    
    best_match = None
    highest_score = 0
    for org in normalized_orgs:
        score = fuzz.token_sort_ratio(account_name.lower(), org.get("Org name", "").lower())
        if score > highest_score and score > 80:
            highest_score = score
            best_match = org
    
    if best_match:
        return best_match.get("Org name", account_name), best_match.get("FINAL", industry_api), industry_api
    
    for org in normalized_orgs:
        if industry_api == org.get("FINAL"):
            return account_name, industry_api, industry_api
    
    normalized_industry = industry_mapping.get(industry_api, None)
    if normalized_industry:
        return account_name, normalized_industry, industry_api
    
    return account_name, industry_api, industry_api

class SectionManager:
    def __init__(self):
        self.current_section = None
        self.sections = {}
        self.divider = "____________________________"

    def start_section(self, section_name):
        if self.current_section:
            self._print_current_section()
        self.current_section = section_name
        if section_name not in self.sections:
            self.sections[section_name] = []
        self.sections[section_name].append(f"{section_name}\n")

    def add_message(self, message):
        if self.current_section:
            self.sections[self.current_section].append(message + "\n")
        else:
            print(message + "\n")

    def end_section(self):
        if self.current_section:
            self._print_current_section()
            self.current_section = None

    def _print_current_section(self):
        if self.current_section in self.sections:
            messages = self.sections[self.current_section]
            for message in messages:
                print(message, end="")
            print(self.divider)

    def finalize(self):
        self.end_section()

class SafeRedirectText:
    def __init__(self, text_widget):
        self.text_widget = text_widget
        self.buffer = ""
        self.last_update = time.time()
        self.update_interval = 0.05

    def write(self, string):
        self.buffer += string
        current_time = time.time()
        if current_time - self.last_update > self.update_interval:
            self._flush_buffer()

    def _flush_buffer(self):
        if not self.buffer:
            return
        try:
            self.text_widget.configure(state="normal")
            self.text_widget.insert(tk.END, self.buffer)
            self.text_widget.see(tk.END)
            self.text_widget.configure(state="disabled")
            self.text_widget.update_idletasks()
            self.buffer = ""
            self.last_update = time.time()
        except Exception as e:
            logger.error(f"Error updating text widget: {str(e)}")

    def flush(self):
        self._flush_buffer()

class GongWizardApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Forrest's Gong GPT Builder")
        self.root.geometry("800x600")
        self.root.minsize(800, 600)
        self.root.configure(bg="#ffffff")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._configure_styles()

        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        self.frame_credentials = ttk.Frame(self.notebook)
        self.frame_output = ttk.Frame(self.notebook)
        self.frame_run = ttk.Frame(self.notebook)

        self.notebook.add(self.frame_credentials, text="Credentials")
        self.notebook.add(self.frame_output, text="Output")
        self.notebook.add(self.frame_run, text="Run")

        for i in range(1, 3):
            self.notebook.tab(i, state="disabled")

        self.access_key = tk.StringVar()
        self.secret_key = tk.StringVar()
        self.from_date = tk.StringVar()  # No default date
        self.to_date = tk.StringVar()    # No default date
        self.output_folder = tk.StringVar(value=os.path.expanduser("~/Desktop"))
        self.cancel_flag = threading.Event()
        self.log_text = tk.Text(self.frame_run, wrap=tk.WORD)
        self.process_thread = None

        self._setup_credentials_frame()
        self._setup_output_frame()
        self._setup_run_frame()

        self.current_step = 0
        self.original_stdout = sys.stdout
        self.redirect_text = SafeRedirectText(self.log_text)
        sys.stdout = self.redirect_text
        self._load_settings()

    def _configure_styles(self):
        style = ttk.Style()
        style.configure("TButton", foreground="black")

    def _setup_credentials_frame(self):
        ttk.Label(self.frame_credentials, text="Gong API Credentials").pack(pady=15)
        ttk.Label(self.frame_credentials, text="Enter your Gong API keys").pack(pady=5)

        creds_frame = ttk.Frame(self.frame_credentials)
        creds_frame.pack(fill=tk.X, padx=15, pady=10)

        ttk.Label(creds_frame, text="Access Key:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=5)
        ttk.Entry(creds_frame, textvariable=self.access_key, width=50).grid(row=0, column=1, sticky=tk.W, padx=10, pady=5)
        ttk.Label(creds_frame, text="Secret Key:").grid(row=1, column=0, sticky=tk.W, padx=10, pady=5)
        ttk.Entry(creds_frame, textvariable=self.secret_key, width=50, show="*").grid(row=1, column=1, sticky=tk.W, padx=10, pady=5)

        date_frame = ttk.Frame(self.frame_credentials)
        date_frame.pack(fill=tk.X, padx=15, pady=10)
        ttk.Label(date_frame, text="From Date (YYYY-MM-DD):").grid(row=0, column=0, sticky=tk.W, padx=10, pady=5)
        ttk.Entry(date_frame, textvariable=self.from_date, width=20).grid(row=0, column=1, sticky=tk.W, padx=10, pady=5)
        ttk.Label(date_frame, text="To Date (YYYY-MM-DD):").grid(row=1, column=0, sticky=tk.W, padx=10, pady=5)
        ttk.Entry(date_frame, textvariable=self.to_date, width=20).grid(row=1, column=1, sticky=tk.W, padx=10, pady=5)

        button_frame = ttk.Frame(self.frame_credentials)
        button_frame.pack(fill=tk.X, pady=15)
        ttk.Button(button_frame, text="Next >", command=self._validate_credentials).pack(side=tk.RIGHT, padx=15)

    def _setup_output_frame(self):
        ttk.Label(self.frame_output, text="Output Configuration").pack(pady=15)
        ttk.Label(self.frame_output, text="Choose folder to save output").pack(pady=5)
        ttk.Label(self.frame_output, text="Note: CSV excludes specified topics, short utterances, and internal speakers").pack(pady=5)

        file_frame = ttk.Frame(self.frame_output)
        file_frame.pack(fill=tk.X, padx=15, pady=10)

        ttk.Label(file_frame, text="Output Folder:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=5)
        ttk.Entry(file_frame, textvariable=self.output_folder, width=50, state='readonly').grid(row=0, column=1, sticky=tk.W, padx=10, pady=5)
        ttk.Button(file_frame, text="Choose Folder...", command=self._choose_output_folder).grid(row=0, column=2, sticky=tk.W, padx=10, pady=5)

        button_frame = ttk.Frame(self.frame_output)
        button_frame.pack(fill=tk.X, pady=15)
        ttk.Button(button_frame, text="< Back", command=lambda: self._go_to_step(0)).pack(side=tk.LEFT, padx=15)
        ttk.Button(button_frame, text="Next >", command=self._validate_output).pack(side=tk.RIGHT, padx=15)

    def _setup_run_frame(self):
        ttk.Label(self.frame_run, text="Run Fetch").pack(pady=15)
        ttk.Label(self.frame_run, text="Click 'Run' to fetch Gong data").pack(pady=5)

        self.log_text.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)
        self.log_text.configure(state="disabled")

        button_frame = ttk.Frame(self.frame_run)
        button_frame.pack(fill=tk.X, pady=15)
        ttk.Button(button_frame, text="< Back", command=lambda: self._go_to_step(1)).pack(side=tk.LEFT, padx=15)
        self.run_button = ttk.Button(button_frame, text="Run", command=self._start_fetch)
        self.run_button.pack(side=tk.RIGHT, padx=15)
        self.cancel_button = ttk.Button(button_frame, text="Cancel", command=self._cancel_fetch, state="disabled")
        self.cancel_button.pack(side=tk.RIGHT, padx=15)

    def _go_to_step(self, step):
        self.notebook.select(step)
        self.current_step = step

    def _load_settings(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.access_key.set(config.get("access_key", ""))
                self.secret_key.set(config.get("secret_key", ""))
                self.from_date.set(config.get("from_date", ""))  # Load from config, empty if not present
                self.to_date.set(config.get("to_date", ""))      # Load from config, empty if not present
                self.output_folder.set(config.get("output_folder", self.output_folder.get()))
            except Exception as e:
                logger.error(f"Error loading settings: {str(e)}")
                messagebox.showerror("Error", f"Failed to load settings: {str(e)}")

    def _save_settings(self):
        config = {
            "access_key": self.access_key.get(),
            "secret_key": self.secret_key.get(),
            "from_date": self.from_date.get(),
            "to_date": self.to_date.get(),
            "output_folder": self.output_folder.get()
        }
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
        except Exception as e:
            logger.error(f"Error saving settings: {str(e)}")
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")

    def _validate_credentials(self):
        access_key = self.access_key.get().strip()
        secret_key = self.secret_key.get().strip()
        if not access_key or not secret_key:
            messagebox.showerror("Error", "Please enter both Access Key and Secret Key.")
            return
        from_date = self.from_date.get().strip()
        to_date = self.to_date.get().strip()
        if not from_date or not to_date:
            messagebox.showerror("Error", "Please enter both From Date and To Date.")
            return
        try:
            datetime.strptime(from_date, "%Y-%m-%d")
            datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Error", "Dates must be in YYYY-MM-DD format.")
            return
        self._save_settings()
        self.notebook.tab(1, state="normal")
        self._go_to_step(1)

    def _choose_output_folder(self):
        folder = filedialog.askdirectory(initialdir=self.output_folder.get())
        if folder:
            self.output_folder.set(folder)
            self._save_settings()

    def _validate_output(self):
        if not os.path.isdir(self.output_folder.get()):
            messagebox.showerror("Error", "Please select a valid output folder.")
            return
        self.notebook.tab(2, state="normal")
        self._go_to_step(2)

    def _start_fetch(self):
        logger.info("Starting fetch process")
        self._save_settings()
        self.cancel_flag.clear()
        self.run_button.config(text="Running...", state="disabled")
        self.cancel_button.config(state="normal")
        self.log_text.configure(state="normal")
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state="disabled")
        FETCH_STATS["started_at"] = datetime.now()
        FETCH_STATS["total_calls"] = 0

        config = {
            "access_key": self.access_key.get(),
            "secret_key": self.secret_key.get(),
            "from_date": self.from_date.get(),
            "to_date": self.to_date.get(),
            "output_folder": self.output_folder.get(),
            "batch_size": 20,
            "timeout": 60,
            "max_retries": 5,
            "excluded_topics": ["Call Setup", "Small Talk", "Wrap-up"],
            "excluded_affiliations": ["Internal"],
            "min_word_count": 5
        }

        self.process_thread = threading.Thread(target=self._run_process, args=(config,), daemon=True)
        self.process_thread.start()

    def _cancel_fetch(self):
        self.cancel_flag.set()
        self.run_button.config(state="normal", text="Run")
        self.cancel_button.config(state="disabled")
        logger.info("Fetch cancelled")

    def _run_process(self, config):
        try:
            FETCH_STATS["started_at"] = datetime.now()
            from_date = datetime.strptime(config['from_date'], "%Y-%m-%d")
            to_date = datetime.strptime(config['to_date'], "%Y-%m-%d")
            from_date_str = from_date.strftime("%d%b%y")
            to_date_str = to_date.strftime("%d%b%y")
            base_filename = f"gong_data_{from_date_str}-{to_date_str}"
            json_path = os.path.join(config['output_folder'], f"JSON_{base_filename}.json")
            quality_csv_path = os.path.join(config['output_folder'], f"quality_{base_filename}.csv")
            summary_csv_path = os.path.join(config['output_folder'], f"summary_{base_filename}.csv")

            print(f"Starting fetch from {config['from_date']} to {config['to_date']}")

            session = create_session(config["max_retries"])
            auth = (config['access_key'], config['secret_key'])
            logger.info(f"Using credentials - Access Key: {config['access_key'][:5]}..., Secret Key: {config['secret_key'][:5]}...")
            section_mgr = SectionManager()

            # Step 1: Fetch call list
            section_mgr.start_section("FETCHING")
            section_mgr.add_message("📞 Call list...")
            all_calls = []
            cursor = None
            params = {
                "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                "toDateTime": f"{config['to_date']}T23:59:59-00:00"
            }

            while True:
                if self.cancel_flag.is_set():
                    section_mgr.add_message("❌ Cancelled during call list fetch")
                    return
                if cursor:
                    params["cursor"] = cursor
                resp = session.get(f"{BASE_URL}/v2/calls", headers={"Content-Type": "application/json"}, params=params, auth=auth, timeout=config["timeout"])
                if resp.status_code != 200:
                    section_mgr.add_message(f"❌ Error fetching call list: {resp.status_code} - {resp.text}")
                    raise Exception(f"Failed to fetch call list: {resp.status_code} - {resp.text}")
                data = resp.json()
                all_calls.extend(data.get("calls", []))
                FETCH_STATS["total_calls"] = len(all_calls)
                section_mgr.add_message(f"Fetched {FETCH_STATS['total_calls']} calls so far...")
                cursor = data.get("records", {}).get("cursor")
                if not cursor:
                    break
                time.sleep(1)

            section_mgr.add_message(f"✅ Fetched {FETCH_STATS['total_calls']} call IDs")
            
            # Step 2: Get call IDs
            call_ids = [call["id"] for call in all_calls]

            # Step 3: Fetch detailed metadata
            section_mgr.add_message("📋 Metadata...")
            section_mgr.add_message("📜 Transcripts...")
            full_data = []
            batch_size = config["batch_size"]

            for i in range(0, len(call_ids), batch_size):
                if self.cancel_flag.is_set():
                    section_mgr.add_message("❌ Cancelled during metadata fetch")
                    return
                batch = call_ids[i:i + batch_size]
                request_body = {
                    "filter": {
                        "callIds": batch,
                        "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                        "toDateTime": f"{config['to_date']}T23:59:59-00:00"
                    },
                    "contentSelector": {
                        "context": "Extended",
                        "exposedFields": {
                            "parties": True,
                            "content": {
                                "structure": True,
                                "topics": True,
                                "trackers": True,
                                "brief": True,
                                "keyPoints": True,
                                "callOutcome": True
                            },
                            "interaction": {
                                "speakers": True,
                                "personInteractionStats": True,
                                "questions": True,
                                "video": True
                            },
                            "collaboration": {
                                "publicComments": True
                            },
                            "media": True
                        }
                    }
                }
                r = session.post(f"{BASE_URL}/v2/calls/extensive", headers={"Content-Type": "application/json"}, json=request_body, auth=auth, timeout=config["timeout"])
                if r.status_code != 200:
                    section_mgr.add_message(f"❌ Error fetching metadata: {r.status_code} - {r.text[:200]}")
                    raise Exception(f"Failed to fetch metadata: {r.status_code} - {r.text}")
                calls_data = r.json().get("calls", [])
                call_metadata = {call_data["metaData"]["id"]: call_data for call_data in calls_data if "metaData" in call_data and "id" in call_data["metaData"]}

                transcript_request = {
                    "filter": {
                        "callIds": batch,
                        "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                        "toDateTime": f"{config['to_date']}T23:59:59-00:00"
                    }
                }
                transcript_response = session.post(f"{BASE_URL}/v2/calls/transcript", headers={"Content-Type": "application/json"}, json=transcript_request, auth=auth, timeout=config["timeout"])
                if transcript_response.status_code != 200:
                    section_mgr.add_message(f"❌ Error fetching transcripts: {transcript_response.status_code} - {transcript_response.text[:200]}")
                    raise Exception(f"Failed to fetch transcripts: {transcript_response.status_code} - {transcript_response.text}")
                transcripts_batch = {t["callId"]: t["transcript"] for t in transcript_response.json().get("callTranscripts", [])}

                for call_id in batch:
                    if call_id in call_metadata and call_id in transcripts_batch:
                        call = next((c for c in all_calls if c["id"] == call_id), None)
                        if call:
                            call_date_str = "unknown-date"
                            if call.get("started"):
                                try:
                                    call_date_obj = datetime.fromisoformat(call["started"].replace('Z', '+00:00'))
                                    call_date_str = call_date_obj.strftime("%Y-%m-%d")
                                except (ValueError, TypeError):
                                    call_date_str = str(call.get("started", "unknown-date"))[:10]

                            call_id_prefix = str(call_id)[:5] if call_id and len(str(call_id)) >= 5 else str(call_id)
                            short_call_id = f"{call_id_prefix}_{call_date_str}"
                            utterances_with_short_id = [{**utterance, "short_call_id": short_call_id} for utterance in transcripts_batch[call_id]]

                            call_meta = call_metadata[call_id]
                            if 'parties' in call_meta:
                                for party in call_meta['parties']:
                                    if party.get('affiliation') == "Unknown":
                                        party['affiliation'] = "External"

                            call_data = {
                                "call_id": call_id,
                                "short_call_id": short_call_id,
                                "call_metadata": call_meta,
                                "utterances": utterances_with_short_id
                            }
                            full_data.append(call_data)
            section_mgr.end_section()

            # Step 4: Normalize organization data
            section_mgr.start_section("NORMALIZING")
            for call_data in full_data:
                account_context = next((ctx for ctx in call_data['call_metadata'].get('context', []) if any(obj.get('objectType') == 'Account' for obj in ctx.get('objects', []))), {})
                industry = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Industry'), 'N/A')
                website = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Website'), 'N/A')
                account_name = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Name'), 'N/A')
                normalized_account, normalized_industry, industry_api = normalize_org(account_name, website, industry)
                call_data['industry_api'] = industry_api
                call_data['account_api'] = account_name
                call_data['industry_normalized'] = normalized_industry
                call_data['account_normalized'] = normalized_account
            section_mgr.end_section()

            # Step 5: Save JSON
            section_mgr.start_section("SAVING")
            section_mgr.add_message("📁 JSON: Full Transcripts...")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(full_data, f, indent=4)
            section_mgr.add_message(f"Saved JSON to {json_path}")

            # Step 6: Save quality CSV (formerly detailed)
            section_mgr.add_message("📊 CSV: Quality Utterances...")
            total_utterances = 0
            filtered_utterances_by_topic = 0
            filtered_utterances_by_length = 0
            filtered_utterances_by_affiliation = 0
            row_count = 0

            with open(quality_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                headers = [
                    'SHORT_CALL_ID', 'CALL_ID', 'CALL_TITLE', 'CALL_DATE', 'DURATION_SECONDS', 'CALL_URL',
                    'ACCOUNT_API', 'INDUSTRY_API', 'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED',
                    'SPEAKER_ID', 'SPEAKER_EMAIL', 'SPEAKER_NAME', 'SPEAKER_JOB_TITLE', 'SPEAKER_AFFILIATION',
                    'TOPIC', 'UTTERANCE_START', 'UTTERANCE_END', 'UTTERANCE_TEXT'
                ]
                csv_writer.writerow(headers)
                for call_data in full_data:
                    call_id = call_data['call_id']
                    short_call_id = call_data['short_call_id']
                    meta = call_data['call_metadata'].get('metaData', {})
                    call_title = meta.get('title', 'N/A')
                    call_date = meta.get('started', 'N/A')
                    duration = meta.get('duration', 'N/A')
                    call_url = meta.get('url', 'N/A')
                    industry = call_data.get('industry_api', 'N/A')
                    account_name = call_data.get('account_api', 'N/A')
                    normalized_account = call_data.get('account_normalized', 'N/A')
                    normalized_industry = call_data.get('industry_normalized', 'Unknown')
                    parties = call_data['call_metadata'].get('parties', [])
                    speaker_info = {party.get('speakerId'): {
                        'email': party.get('emailAddress', 'N/A'),
                        'name': party.get('name', 'N/A'),
                        'title': party.get('title', 'Unknown'),
                        'affiliation': party.get('affiliation', 'N/A')
                    } for party in parties if party.get('speakerId')}
                    utterances = call_data.get('utterances', [])
                    for utterance in utterances:
                        total_utterances += 1
                        speaker_id = utterance.get('speakerId', 'N/A')
                        topic = utterance.get('topic', 'N/A')
                        if topic in config["excluded_topics"]:
                            filtered_utterances_by_topic += 1
                            continue
                        sentences = utterance.get('sentences', [])
                        if not sentences:
                            continue
                        utterance_text = " ".join(sentence.get('text', 'N/A') for sentence in sentences)
                        word_count = len(utterance_text.split())
                        if word_count <= config["min_word_count"]:
                            filtered_utterances_by_length += 1
                            continue
                        speaker = speaker_info.get(speaker_id, {'email': 'N/A', 'name': 'N/A', 'title': 'Unknown', 'affiliation': 'N/A'})
                        if speaker['affiliation'] in config["excluded_affiliations"]:
                            filtered_utterances_by_affiliation += 1
                            continue
                        start_time = sentences[0].get('start', 'N/A') if sentences else 'N/A'
                        end_time = sentences[-1].get('end', 'N/A') if sentences else 'N/A'
                        row = [
                            str(short_call_id), f'"{call_id}"', str(call_title), str(call_date), str(duration), str(call_url),
                            str(account_name), str(industry), str(normalized_account), str(normalized_industry),
                            str(speaker_id), str(speaker['email']), str(speaker['name']), str(speaker['title']), str(speaker['affiliation']),
                            str(topic), str(start_time), str(end_time), str(utterance_text)
                        ]
                        csv_writer.writerow(row)
                        row_count += 1

            total_filtered = filtered_utterances_by_topic + filtered_utterances_by_length + filtered_utterances_by_affiliation
            topic_percent = (filtered_utterances_by_topic / total_utterances * 100) if total_utterances > 0 else 0
            length_percent = (filtered_utterances_by_length / total_utterances * 100) if total_utterances > 0 else 0
            internal_percent = (filtered_utterances_by_affiliation / total_utterances * 100) if total_utterances > 0 else 0

            # Step 7: Save summary CSV
            section_mgr.add_message("📈 CSV: Call Summaries...")
            max_external_speakers = 0
            max_internal_speakers = 0
            for call_data in full_data:
                parties = call_data['call_metadata'].get('parties', [])
                speaking_parties = [party for party in parties if party.get('speakerId')]
                external_speakers = [party for party in speaking_parties if party.get('affiliation') == 'External']
                internal_speakers = [party for party in speaking_parties if party.get('affiliation') == 'Internal']
                max_external_speakers = max(max_external_speakers, len(external_speakers))
                max_internal_speakers = max(max_internal_speakers, len(internal_speakers))
            
            with open(summary_csv_path, 'w', newline='', encoding='utf-8') as summary_file:
                summary_writer = csv.writer(summary_file, quoting=csv.QUOTE_MINIMAL)
                summary_headers = [
                    'CALL_ID', 'SHORT_CALL_ID', 'CALL_TITLE', 'CALL_START_TIME', 'DURATION', 'MEETING_URL',
                    'INDUSTRY', 'WEBSITE', 'ACCOUNT_NAME', 'INDUSTRY_API', 'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED',
                    'OPPORTUNITY_NAME', 'LEAD_SOURCE', 'OPPORTUNITY_TYPE', 'DEAL_STAGE', 'FORECAST_CATEGORY'
                ]
                for i in range(1, max_external_speakers + 1):
                    summary_headers.append(f'EXTERNAL_PARTICIPANT_{i}_NAME')
                    summary_headers.append(f'EXTERNAL_PARTICIPANT_{i}_JOB_TITLE')
                
                # Only add internal participant columns for as many as we actually need
                for i in range(1, max_internal_speakers + 1):
                    summary_headers.append(f'INTERNAL_PARTICIPANT_{i}_NAME')
                    summary_headers.append(f'INTERNAL_PARTICIPANT_{i}_JOB_TITLE')
                
                summary_headers.extend([
                    'TOTAL_SPEAKERS', 'INTERNAL_SPEAKERS', 'EXTERNAL_SPEAKERS',
                    'TRACKER_AIR_QUALITY', 'TRACKER_AUTHORITY', 'TRACKER_BUDGET', 'TRACKER_COMPETITION',
                    'TRACKER_CONNECT', 'TRACKER_CUSTOMER_PAIN_POINTS', 'TRACKER_PRODUCT_DIFFERENTIATION',
                    'TRACKER_ENERGY_SAVINGS', 'TRACKER_FILTER_MENTIONS', 'TRACKER_INSTALL',
                    'TRACKER_OBJECTIONS', 'TRACKER_ODCV_MENTIONS', 'TRACKER_R_ZERO_COMPETITORS',
                    'TRACKER_TIMING', 'PRICING_DURATION', 'NEXT_STEPS_DURATION', 'CALL_BRIEF', 'KEY_POINTS'
                ])
                summary_writer.writerow(summary_headers)

                for call_data in full_data:
                    call_id = call_data['call_id']
                    short_call_id = call_data['short_call_id']
                    meta = call_data['call_metadata'].get('metaData', {})
                    title = meta.get('title', 'N/A')
                    started = meta.get('started', 'N/A')
                    duration = meta.get('duration', 'N/A')
                    meeting_url = meta.get('meetingUrl', 'N/A')
                    industry = call_data.get('industry_api', 'N/A')
                    account_context = next((ctx for ctx in call_data['call_metadata'].get('context', []) if any(obj.get('objectType') == 'Account' for obj in ctx.get('objects', []))), {})
                    website = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Website'), 'N/A')
                    account_name = call_data.get('account_api', 'N/A')
                    normalized_account = call_data.get('account_normalized', 'N/A')
                    normalized_industry = call_data.get('industry_normalized', 'Unknown')
                    opportunity_context = next((ctx for ctx in call_data['call_metadata'].get('context', []) if any(obj.get('objectType') == 'Opportunity' for obj in ctx.get('objects', []))), {})
                    deal_name = next((field.get('value', 'N/A') for obj in opportunity_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Name'), 'N/A')
                    lead_source = next((field.get('value', 'N/A') for obj in opportunity_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'LeadSource'), 'N/A')
                    deal_type = next((field.get('value', 'N/A') for obj in opportunity_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Type'), 'N/A')
                    deal_stage = next((field.get('value', 'N/A') for obj in opportunity_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'StageName'), 'N/A')
                    deal_forecast = next((field.get('value', 'N/A') for obj in opportunity_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'ForecastCategoryName'), 'N/A')
                    parties = call_data['call_metadata'].get('parties', [])
                    speaking_parties = [party for party in parties if party.get('speakerId')]
                    speaker_durations = {speaker.get('speakerId'): speaker.get('talkTime', 0) for speaker in call_data['call_metadata'].get('interaction', {}).get('speakers', [])}
                    for party in speaking_parties:
                        party['talkTime'] = speaker_durations.get(party.get('speakerId'), 0)
                    external_speakers = sorted([party for party in speaking_parties if party.get('affiliation') == 'External'], key=lambda x: x['talkTime'], reverse=True)
                    internal_speakers = sorted([party for party in speaking_parties if party.get('affiliation') == 'Internal'], key=lambda x: x['talkTime'], reverse=True)

                    total_speakers = len(speaking_parties)
                    internal_speakers_count = sum(1 for party in speaking_parties if party.get('affiliation') == 'Internal')
                    external_speakers_count = sum(1 for party in speaking_parties if party.get('affiliation') == 'External')
                    trackers = {tracker.get('name', '').lower(): tracker.get('count', 0) for tracker in call_data['call_metadata'].get('content', {}).get('trackers', [])}
                    topics = {topic.get('name', '').lower(): topic.get('duration', 0) for topic in call_data['call_metadata'].get('content', {}).get('topics', [])}
                    air_quality = trackers.get('air quality', 0)
                    authority = trackers.get('authority', 0)
                    budget = trackers.get('budget', 0)
                    competition = trackers.get('competition', 0)
                    connect = trackers.get('connect', 0)
                    customer_pain = trackers.get('customer pain', 0)
                    differentiation = trackers.get('differentiation', 0)
                    energy_savings = trackers.get('energy savings', 0)
                    filter_count = trackers.get('filter', 0)
                    install = trackers.get('install', 0)
                    objections = trackers.get('objections', 0)
                    odcv = trackers.get('odcv', 0)
                    rzero_competitors = trackers.get('r-zero competitors', 0)
                    timing = trackers.get('timing', 0)
                    pricing_duration = topics.get('pricing', 0)
                    next_steps_duration = topics.get('next steps', 0)
                    brief = call_data['call_metadata'].get('content', {}).get('brief', 'N/A')
                    key_points = '; '.join(point.get('text', '') for point in call_data['call_metadata'].get('content', {}).get('keyPoints', []))
                    summary_row = [
                        f'"{call_id}"', str(short_call_id), str(title), str(started), format_duration(duration), str(meeting_url),
                        str(industry), str(website), str(account_name), str(industry), str(normalized_account), str(normalized_industry),
                        str(deal_name), str(lead_source), str(deal_type), str(deal_stage), str(deal_forecast)
                    ]
                    
                    # Add external participants
                    for i in range(max_external_speakers):
                        if i < len(external_speakers):
                            summary_row.append(str(external_speakers[i].get('name', 'N/A')))
                            summary_row.append(str(external_speakers[i].get('title', 'Unknown')))
                        else:
                            summary_row.append('')
                            summary_row.append('')
                    
                    # Add only the actual internal participants 
                    for i in range(max_internal_speakers):
                        if i < len(internal_speakers):
                            summary_row.append(str(internal_speakers[i].get('name', 'N/A')))
                            summary_row.append(str(internal_speakers[i].get('title', 'Unknown')))
                        else:
                            summary_row.append('')
                            summary_row.append('')

                    summary_row.extend([
                        str(total_speakers), str(internal_speakers_count), str(external_speakers_count),
                        str(air_quality), str(authority), str(budget), str(competition), str(connect),
                        str(customer_pain), str(differentiation), str(energy_savings), str(filter_count),
                        str(install), str(objections), str(odcv), str(rzero_competitors), str(timing),
                        format_duration(pricing_duration), format_duration(next_steps_duration),
                        str(brief), str(key_points)
                    ])
                    summary_writer.writerow(summary_row)
            
            section_mgr.add_message(f"✅ Saved to {config['output_folder']}")

            # Step 8: Summary
            section_mgr.end_section()
            section_mgr.start_section("EXCLUDING")
            section_mgr.add_message(f"💬 Idle chatter... {filtered_utterances_by_topic:,} ({round(topic_percent)}%)")
            section_mgr.add_message(f"✂️ Filler phrases... {filtered_utterances_by_length:,} ({round(length_percent)}%)")
            section_mgr.add_message(f"🧑‍💼 Internal speakers... {filtered_utterances_by_affiliation:,} ({round(internal_percent)}%)")
            section_mgr.end_section()
            section_mgr.start_section("SUMMARY")
            section_mgr.add_message(f"💬 Utterances Processed: {total_utterances}")
            section_mgr.add_message(f"🗑️ Excluded utterances: {total_filtered}")
            section_mgr.add_message(f"📥 Included utterances: {row_count}")
            section_mgr.end_section()
            section_mgr.finalize()

            FETCH_STATS["completed_at"] = datetime.now()
            stats_path = os.path.join(config['output_folder'], "fetch_stats.json")
            with open(stats_path, 'w', encoding='utf-8') as f:
                stats = FETCH_STATS.copy()
                stats["started_at"] = stats["started_at"].isoformat() if stats["started_at"] else None
                stats["completed_at"] = stats["completed_at"].isoformat() if stats["completed_at"] else None
                stats["duration_seconds"] = (FETCH_STATS["completed_at"] - FETCH_STATS["started_at"]).total_seconds() if stats["completed_at"] and stats["started_at"] else None
                json.dump(stats, f, indent=2)

        except Exception as e:
            section_mgr.add_message(f"❌ Error: {str(e)}")
            section_mgr.finalize()
            logger.error(f"Error during processing: {str(e)}")
            self.run_button.config(state="normal", text="Run")
            self.cancel_button.config(state="disabled")
            return

        self.run_button.config(state="normal", text="Run")
        self.cancel_button.config(state="disabled")
        messagebox.showinfo("Success", "Fetch process completed successfully!")

    def _on_close(self):
        if self.process_thread and self.process_thread.is_alive():
            if messagebox.askyesno("Confirm Exit", "A fetch is in progress. Do you want to cancel and exit?"):
                self.cancel_flag.set()
                self.root.after(500, self.root.destroy)
        else:
            self.root.destroy()

def main():
    try:
        root = tk.Tk()
        app = GongWizardApp(root)
        root.mainloop()
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        messagebox.showerror("Startup Error", f"Fatal error: {str(e)}")

if __name__ == "__main__":
    main()