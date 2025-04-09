import streamlit as st
import subprocess
import datetime
import os
import json

st.set_page_config(page_title="Gong Wizard", layout="centered")
st.title("🚀 Gong Wizard Web App")

# --- Inputs ---
st.subheader("Step 1: Enter Gong API Config")
api_key = st.text_input("Gong API Key", type="password")
start_date = st.date_input("Start Date", value=datetime.date.today())
end_date = st.date_input("End Date", value=datetime.date.today())

st.subheader("Step 2: Upload Mapping Files")
uploaded_orgs = st.file_uploader("Upload normalized_orgs.csv", type=["csv"])
uploaded_industries = st.file_uploader("Upload industry_mapping.csv", type=["csv"])

output_dir = st.text_input("Output Folder Name", value="gong_output")
run_button = st.button("Run Gong Wizard")

# --- Execution ---
if run_button:
    if not api_key or not uploaded_orgs or not uploaded_industries:
        st.error("Please fill in all required fields.")
    else:
        os.makedirs(output_dir, exist_ok=True)

        orgs_path = os.path.join(output_dir, "normalized_orgs.csv")
        inds_path = os.path.join(output_dir, "industry_mapping.csv")

        with open(orgs_path, "wb") as f:
            f.write(uploaded_orgs.getbuffer())
        with open(inds_path, "wb") as f:
            f.write(uploaded_industries.getbuffer())

        config = {
            "gong_api_key": api_key,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "output_path": output_dir
        }

        config_path = os.path.join(output_dir, "gong_wizard_config.json")
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        st.info("Running Gong Wizard script...")
        try:
            subprocess.run(
                ["python3", "gong_wizard_extensive_final.py"],
                check=True,
                cwd=os.getcwd()
            )
            st.success("✅ Gong Wizard run complete. Check the output folder for results.")
        except subprocess.CalledProcessError as e:
            st.error("❌ Error while running Gong Wizard. See logs for details.")
