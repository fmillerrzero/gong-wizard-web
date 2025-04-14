# Main processing (webhook mode removed, Edit 3)
if process_button:
    if not access_key or not secret_key:
        st.error("Please enter your Gong API credentials.")
        st.stop()
    
    config = {
        "access_key": access_key,
        "secret_key": secret_key,
        "from_date": start_date.strftime("%Y-%m-%d"),
        "to_date": end_date.strftime("%Y-%m-%d"),
        "output_folder": ".",
        "excluded_topics": ["Call Setup", "Small Talk", "Wrap-up"],
        "excluded_affiliations": ["Internal"],
        "min_word_count": 8  # Update 5: Changed from 5 to 8
    }
    
    status_container = st.container()
    with status_container:
        st.subheader("Processing Status")
        status = st.empty()
        
        try:
            status.info("Starting Gong data fetching process...")
            BASE_URL = "https://us-11211.api.gong.io"
            session = requests.Session()
            auth = (config['access_key'], config['secret_key'])
            
            # Fetch call list
            status.info("Fetching call list...")
            all_calls = []
            cursor = None
            params = {
                "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                "toDateTime": f"{config['to_date']}T23:59:59-00:00"
            }
            
            while True:
                if cursor:
                    params["cursor"] = cursor
                resp = session.get(
                    f"{BASE_URL}/v2/calls", 
                    headers={"Content-Type": "application/json"}, 
                    params=params, 
                    auth=auth, 
                    timeout=30
                )
                if resp.status_code != 200:
                    status.error(f"Error fetching call list: {resp.status_code} - {resp.text}")
                    st.stop()
                data = resp.json()
                all_calls.extend(data.get("calls", []))
                status.info(f"Fetched {len(all_calls)} calls so far...")
                cursor = data.get("records", {}).get("cursor")
                if not cursor:
                    break
                time.sleep(1)
            
            status.success(f"✅ Successfully fetched {len(all_calls)} calls")
            call_ids = [call["id"] for call in all_calls]

            # Fetch detailed metadata and transcripts
            status.info("Fetching metadata and transcripts...")
            full_data = []
            batch_size = 20
            
            for i in range(0, len(call_ids), batch_size):
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
                r = session.post(f"{BASE_URL}/v2/calls/extensive", headers={"Content-Type": "application/json"}, json=request_body, auth=auth, timeout=60)
                if r.status_code != 200:
                    status.error(f"Error fetching metadata: {r.status_code} - {r.text}")
                    st.stop()
                calls_data = r.json().get("calls", [])
                call_metadata = {call_data["metaData"]["id"]: call_data for call_data in calls_data if "metaData" in call_data and "id" in call_data["metaData"]}

                transcript_request = {
                    "filter": {
                        "callIds": batch,
                        "fromDateTime": f"{config['from_date']}T00:00:00-00:00",
                        "toDateTime": f"{config['to_date']}T23:59:59-00:00"
                    }
                }
                transcript_response = session.post(f"{BASE_URL}/v2/calls/transcript", headers={"Content-Type": "application/json"}, json=transcript_request, auth=auth, timeout=60)
                if transcript_response.status_code != 200:
                    status.error(f"Error fetching transcripts: {transcript_response.status_code} - {transcript_response.text}")
                    st.stop()
                transcripts_batch = {t["callId"]: t["transcript"] for t in transcript_response.json().get("callTranscripts", [])}

                for call_id in batch:
                    if call_id in call_metadata and call_id in transcripts_batch:
                        call = call_metadata[call_id]
                        call_date_str = "unknown-date"
                        if call.get("metaData", {}).get("started"):
                            call_date_obj = datetime.fromisoformat(call["metaData"]["started"].replace('Z', '+00:00'))
                            call_date_str = call_date_obj.strftime("%Y-%m-%d")
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

            # Normalize orgs
            status.info("Normalizing organizations...")
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

            # Save JSON
            status.info("Saving JSON...")
            start_date_str = start_date.strftime("%d%b%y").lower()  # e.g., 07apr25
            end_date_str = end_date.strftime("%d%b%y").lower()      # e.g., 14apr25
            json_path = f"json_gong_{start_date_str}_to_{end_date_str}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(full_data, f, indent=4)

            # Save quality CSV
            status.info("Saving quality CSV...")
            quality_csv_path = f"utterances_gong_{start_date_str}_to_{end_date_str}.csv"
            with open(quality_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                headers = [
                    'CALL_ID', 'SHORT_CALL_ID', 'CALL_TITLE', 'CALL_DATE', 
                    'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED', 
                    'SPEAKER_JOB_TITLE', 'UTTERANCE_DURATION', 'UTTERANCE_TEXT',
                    'TOPIC'
                ]
                csv_writer.writerow(headers)
                for call_data in full_data:
                    call_id = call_data['call_id']
                    short_call_id = call_data['short_call_id']
                    meta = call_data['call_metadata'].get('metaData', {})
                    call_title = meta.get('title', 'N/A')
                    call_date = meta.get('started', 'N/A')
                    normalized_account = call_data.get('account_normalized', 'N/A')
                    normalized_industry = call_data.get('industry_normalized', 'Unknown')
                    parties = call_data['call_metadata'].get('parties', [])
                    speaker_info = {party.get('speakerId'): {
                        'name': party.get('name', 'N/A'),
                        'title': party.get('title', 'Unknown')
                    } for party in parties if party.get('speakerId')}
                    utterances = call_data.get('utterances', [])
                    for utterance in utterances:
                        speaker_id = utterance.get('speakerId', 'N/A')
                        sentences = utterance.get('sentences', [])
                        if not sentences:
                            continue
                        utterance_text = " ".join(sentence.get('text', 'N/A') for sentence in sentences)
                        word_count = len(utterance_text.split())
                        topic = utterance.get('topic', 'N/A')
                        # Skip utterances with excluded topics
                        if topic in config["excluded_topics"]:
                            continue
                        if word_count <= config["min_word_count"]:
                            continue
                        speaker = speaker_info.get(speaker_id, {'name': 'N/A', 'title': 'Unknown'})
                        start_time = sentences[0].get('start', 'N/A') if sentences else 'N/A'
                        end_time = sentences[-1].get('end', 'N/A') if sentences else 'N/A'
                        try:
                            utterance_duration = int(end_time) - int(start_time)
                        except (ValueError, TypeError):
                            utterance_duration = 'N/A'
                        row = [
                            f'"{call_id}"', str(short_call_id), str(call_title), str(call_date),
                            str(normalized_account), str(normalized_industry),
                            str(speaker['title']),
                            str(utterance_duration), str(utterance_text),
                            str(topic)
                        ]
                        csv_writer.writerow(row)

            # Save summary CSV
            status.info("Saving summary CSV...")
            summary_csv_path = f"summary_gong_{start_date_str}_to_{end_date_str}.csv"
            with open(summary_csv_path, 'w', newline='', encoding='utf-8') as summary_file:
                summary_writer = csv.writer(summary_file, quoting=csv.QUOTE_MINIMAL)
                summary_headers = [
                    'CALL_ID', 'SHORT_CALL_ID', 'CALL_TITLE', 'CALL_START_TIME', 'CALL_DATE',
                    'DURATION', 'MEETING_URL', 'WEBSITE',
                    'ACCOUNT_NORMALIZED', 'INDUSTRY_NORMALIZED',
                    'OPPORTUNITY_NAME', 'LEAD_SOURCE', 'OPPORTUNITY_TYPE',
                    'DEAL_STAGE', 'FORECAST_CATEGORY',
                    'EXTERNAL_PARTICIPANTS', 'INTERNAL_PARTICIPANTS',
                    'TOTAL_SPEAKERS', 'INTERNAL_SPEAKERS', 'EXTERNAL_SPEAKERS',
                    'TRACKERS_ALL', 'PRICING_DURATION', 'NEXT_STEPS_DURATION',
                    'CALL_BRIEF', 'KEY_POINTS'
                ]
                summary_writer.writerow(summary_headers)
                for call_data in full_data:
                    call_id = call_data['call_id']
                    short_call_id = call_data['short_call_id']
                    meta = call_data['call_metadata'].get('metaData', {})
                    title = meta.get('title', 'N/A')
                    started = meta.get('started', 'N/A')
                    call_date = 'N/A'
                    if started != 'N/A':
                        try:
                            call_date_obj = datetime.fromisoformat(started.replace('Z', '+00:00'))
                            call_date = call_date_obj.strftime("%Y-%m-%d")
                        except ValueError:
                            call_date = 'N/A'
                    duration = meta.get('duration', 'N/A')
                    meeting_url = meta.get('meetingUrl', 'N/A')
                    normalized_account = call_data.get('account_normalized', 'N/A')
                    normalized_industry = call_data.get('industry_normalized', 'Unknown')
                    account_context = next((ctx for ctx in call_data['call_metadata'].get('context', []) if any(obj.get('objectType') == 'Account' for obj in ctx.get('objects', []))), {})
                    website = next((field.get('value', 'N/A') for obj in account_context.get('objects', []) for field in obj.get('fields', []) if field.get('name') == 'Website'), 'N/A')
                    opportunity = next((obj for obj in account_context.get('objects', []) if obj.get('objectType') == 'Opportunity'), {})
                    opportunity_name = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'Name'), 'N/A')
                    lead_source = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'LeadSource'), 'N/A')
                    opportunity_type = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'Type'), 'N/A')
                    deal_stage = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'StageName'), 'N/A')
                    forecast_category = next((field.get('value', 'N/A') for field in opportunity.get('fields', []) if field.get('name') == 'ForecastCategoryName'), 'N/A')
                    parties = call_data['call_metadata'].get('parties', [])
                    external_participants = sum(1 for party in parties if party.get('affiliation') == 'External')
                    internal_participants = sum(1 for party in parties if party.get('affiliation') == 'Internal')
                    total_speakers = len(set(utterance.get('speakerId') for utterance in call_data.get('utterances', []) if utterance.get('speakerId')))
                    internal_speakers = len(set(utterance.get('speakerId') for utterance in call_data.get('utterances', []) if utterance.get('speakerId') in [party.get('speakerId') for party in parties if party.get('affiliation') == 'Internal']))
                    external_speakers = len(set(utterance.get('speakerId') for utterance in call_data.get('utterances', []) if utterance.get('speakerId') in [party.get('speakerId') for party in parties if party.get('affiliation') == 'External']))
                    trackers = call_data['call_metadata'].get('content', {}).get('trackers', [])
                    trackers_all = ";".join([f"{tracker.get('name', 'N/A')}:{tracker.get('count', 0)}" for tracker in trackers]) if trackers else 'N/A'
                    topics = call_data['call_metadata'].get('content', {}).get('topics', [])
                    pricing_duration = next((topic.get('duration', 0) for topic in topics if topic.get('name') == 'Pricing'), 0)
                    next_steps_duration = next((topic.get('duration', 0) for topic in topics if topic.get('name') == 'Next Steps'), 0)
                    call_brief = call_data['call_metadata'].get('content', {}).get('brief', 'N/A')
                    key_points = call_data['call_metadata'].get('content', {}).get('keyPoints', [])
                    key_points_str = ";".join([point.get('text', 'N/A') for point in key_points]) if key_points else 'N/A'
                    summary_row = [
                        f'"{call_id}"', str(short_call_id), str(title), str(started), str(call_date),
                        str(duration), str(meeting_url), str(website),
                        str(normalized_account), str(normalized_industry),
                        str(opportunity_name), str(lead_source), str(opportunity_type),
                        str(deal_stage), str(forecast_category),
                        str(external_participants), str(internal_participants),
                        str(total_speakers), str(internal_speakers), str(external_speakers),
                        str(trackers_all), str(pricing_duration), str(next_steps_duration),
                        str(call_brief), str(key_points_str)
                    ]
                    summary_writer.writerow(summary_row)

            # Display the summary CSV
            df = pd.read_csv(summary_csv_path)
            st.subheader("Call Summary")
            st.dataframe(df)
            
            # Download buttons in the desired order
            with open(summary_csv_path, 'r') as file:
                csv_data = file.read()
            st.download_button(
                label="Download Summary CSV",
                data=csv_data,
                file_name=summary_csv_path,
                mime="text/csv",
            )

            with open(quality_csv_path, 'r') as file:
                csv_data = file.read()
            st.download_button(
                label="Download Utterances CSV",
                data=csv_data,
                file_name=quality_csv_path,
                mime="text/csv",
            )

            with open(json_path, 'r') as file:
                json_data = file.read()
            st.download_button(
                label="Download Full Transcript JSON",
                data=json_data,
                file_name=json_path,
                mime="application/json",
            )

            # Save fetch stats
            fetch_stats = {
                "started_at": datetime.now().isoformat(),
                "completed_at": datetime.now().isoformat(),
                "total_calls": len(call_ids)
            }
            with open("fetch_stats.json", "w") as f:
                json.dump(fetch_stats, f, indent=2)
            
            status.success("✅ Processing complete!")
            
        except Exception as e:
            status.error(f"Error during processing: {str(e)}")
            st.error(f"An error occurred: {str(e)}")