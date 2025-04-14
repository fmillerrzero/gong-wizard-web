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
            # --- Fix: Skip utterances with excluded topics ---
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