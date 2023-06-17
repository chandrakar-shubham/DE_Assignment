
import functions_framework


@functions_framework.http
def extract_hubspot_data_deals(request):
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
  deals_endpoint = 'https://api.hubapi.com/crm/v3/objects/deals'
  deals_properties = ["hs_object_id",
                    "hs_lastmodifieddate",
                    "dealname",
                    "pipeline",
                    "createdate",
                    "closedate",
                    "updatedAt",
                    "amount",
                    "createdAt",
                    "dealstage",
                    "id"]
  limit = 100

  # Define the time frame for data extraction
  start_date = '1672511400'
  end_date = '1675103400'

  # Set up the headers with the API key
  headers = {'Authorization': f'Bearer {api_key}'}

  # Extract Meetings data
  deals_params = {
        'startTimestamp': start_date,
        'endTimestamp': end_date,
        'properties': deals_properties,
        'limit': limit}
  counter = 1
  total_data = []
  pages = 1000  
  while counter <= pages:
    deals_response = requests.get(deals_endpoint, headers=headers, params=deals_params)
    deals_data = deals_response.json()
    total_data.extend(deals_data['results'])
    if 'paging' in deals_data and 'next' in deals_data['paging']:
      deals_params['after'] = deals_data['paging']['next']['after']
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
                    "hs_object_id": result["properties"].get("hs_object_id"),
                    "hs_lastmodifieddate": result["properties"].get("hs_lastmodifieddate"),
                    "dealname": result["properties"].get("dealname"),
                    "pipeline": result["properties"].get("pipeline"),
                    "createdate": result["properties"].get("createdate"),
                    "closedate": result["properties"].get("closedate"),
                    "amount": result["properties"].get("amount"),
                    "updatedAt": result["updatedAt"],
                    "createdAt": result["createdAt"],
                    "dealstage": result["properties"].get("dealstage"),
                    "id": result["id"]}
          extracted_data.append(extracted_item)

    return extracted_data
  start_date = datetime.date(2023, 1, 1)
  end_date = datetime.date(2023, 1, 31)
  deals_data = correct_json(start_date, total_data, end_date)

  #convert to df
  df = pd.DataFrame(deals_data)

  #change dtype
  
  df['hs_object_id'] = df['hs_object_id'].astype(int)
  df['hs_lastmodifieddate'] = pd.to_datetime(df['hs_lastmodifieddate'].str[:10], format='%Y-%m-%d')
  df['createdate'] = pd.to_datetime(df['createdate'].str[:10], format='%Y-%m-%d')
  df['closedate'] = pd.to_datetime(df['closedate'].str[:10], format='%Y-%m-%d')
  df['amount'] = pd.to_numeric(df['amount'])
  df['updatedAt'] = pd.to_datetime(df['updatedAt'].str[:10], format='%Y-%m-%d')
  df['createdAt'] = pd.to_datetime(df['createdAt'].str[:10], format='%Y-%m-%d')
  df['id'] = df['id'].astype(int)

  # Construct a BigQuery client object.
  client = bigquery.Client()

  #table id
  table_id = "essential-haiku-389809.hubspot.deals"

  job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
  
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
  job.result()  # Wait for the job to complete.
  logging.info(f"{len(df)} Records uploaded in {table_id}.")

  return "deals data upload job complete"