# gong_wizard_extensive_final.py (Web-safe version)

import requests
import json
import csv
import os
import time
from datetime import datetime
import logging
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fuzzywuzzy import fuzz
from urllib.parse import urlparse

# Load config passed from Streamlit
with open("gong_wizard_config.json") as f:
    CONFIG = json.load(f)

ACCESS_KEY = CONFIG["gong_api_key"]
SECRET_KEY = os.environ.get("GONG_SECRET_KEY", "")  # Optional: we can pass this securely later
START_DATE = CONFIG["start_date"]
END_DATE = CONFIG["end_date"]
OUTPUT_PATH = CONFIG["output_path"]
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Dummy transcript fetch logic for now (replace with real API logic)
def main():
    with open(os.path.join(OUTPUT_PATH, "fetch_stats.json"), "w") as f:
        json.dump({
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": datetime.utcnow().isoformat(),
            "duration_seconds": 0,
            "total_calls": 0
        }, f, indent=2)

    with open(os.path.join(OUTPUT_PATH, "summary_placeholder.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["call_id", "title", "date"])
        writer.writerow(["1234", "Example Call", START_DATE])

    print(f"✅ Done. Output folder: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()