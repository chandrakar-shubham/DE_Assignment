
import functions_framework


@functions_framework.http
def extract_hubspot_data(request):
  import requests
  from datetime import datetime, timezone,date
  import pandas as pd
  import json
  import datetime
  import logging
  from google.cloud import bigquery
  import pytz
  '''extract raw data meetings data in form of JSON''' 
  api_key = 'pat-na1-0234b5dc-627f-4bad-887c-449363cdb8d9'
  # Define the API endpoints for Meetings
  meetings_endpoint = 'https://api.hubapi.com/crm/v3/objects/meetings'
  meet_properties = [
        "hs_meeting_title",
        "hs_activity_type",
        "hs_object_id",
        "hs_lastmodifieddate",
        "hs_meeting_body",
        "hs_meeting_outcome",
        "hs_createdate",
        "updatedAt",
        "createdAt",
        "id"]
  limit = 100

  # Define the time frame for data extraction
  start_date = '1672511400'
  end_date = '1675103400'

  # Set up the headers with the API key
  headers = {'Authorization': f'Bearer {api_key}'}

  # Extract Meetings data
  meet_params = {
        'startTimestamp': start_date,
        'endTimestamp': end_date,
        'properties': meet_properties,
        'limit': limit}
  counter = 1
  total_data = []
  pages = 1000  
  while counter <= pages:
    meet_response = requests.get(meetings_endpoint, headers=headers, params=meet_params)
    meet_data = meet_response.json()
    total_data.extend(meet_data['results'])
    if 'paging' in meet_data and 'next' in meet_data['paging']:
      meet_params['after'] = meet_data['paging']['next']['after']
    else:
      break
    counter += 1
  
  def correct_json(start_date, data, end_date):
    '''correctly fomart json data'''
    extracted_data = []
    if not data:
      return extracted_data
    else:
      for result in data:
        created_at = result['createdAt'][:10]
        created_at = date.fromisoformat(created_at)
        if start_date <= created_at <= end_date:
          extracted_item = {
                    "hs_meeting_title": result["properties"].get("hs_meeting_title"),
                    "hs_activity_type": result["properties"].get("hs_activity_type"),
                    "hs_object_id": result["properties"].get("hs_object_id"),
                    "hs_lastmodifieddate": result["properties"].get("hs_lastmodifieddate"),
                    "hs_meeting_body": result["properties"].get("hs_meeting_body"),
                    "hs_meeting_outcome": result["properties"].get("hs_meeting_outcome"),
                    "hs_createdate": result["properties"].get("hs_createdate"),
                    "updatedAt": result["updatedAt"],
                    "createdAt": result["createdAt"],
                    "id": result["id"]}
          extracted_data.append(extracted_item)

    return extracted_data
  start_date = datetime.date(2023, 1, 1)
  end_date = datetime.date(2023, 1, 31)
  meeting_data = correct_json(start_date, total_data, end_date)

  #convert to df
  df = pd.DataFrame(meeting_data)

  #change dtype
  df['hs_meeting_title'] = df['hs_meeting_title']
  df['hs_activity_type'] = df['hs_activity_type']
  df['hs_object_id'] = df['hs_object_id'].astype(int)
  df['hs_lastmodifieddate'] = pd.to_datetime(df['hs_lastmodifieddate'].str[:10], format='%Y-%m-%d')
  df['hs_meeting_body'] = df['hs_meeting_body']
  df['hs_meeting_outcome'] = df['hs_meeting_outcome']
  df['hs_createdate'] = pd.to_datetime(df['hs_createdate'].str[:10], format='%Y-%m-%d')
  df['updatedAt'] = pd.to_datetime(df['updatedAt'].str[:10], format='%Y-%m-%d')
  df['createdAt'] = pd.to_datetime(df['createdAt'].str[:10], format='%Y-%m-%d')
  df['id'] = df['id'].astype(int)

  # Construct a BigQuery client object.
  client = bigquery.Client()

  #table id
  table_id = "essential-haiku-389809.hubspot.meeting"

  job_config = bigquery.LoadJobConfig(schema = [
    bigquery.SchemaField("hs_meeting_title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hs_activity_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hs_object_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("hs_lastmodifieddate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("hs_meeting_body", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hs_meeting_outcome", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hs_createdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("updatedAt", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("createdAt", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE")],
        write_disposition="WRITE_TRUNCATE")
  
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
  job.result()  # Wait for the job to complete.
  logging.info(f"{len(df)} Records uploaded in {table_id}.")

  return "Job Complete"