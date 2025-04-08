#!/usr/bin/env python

import json
import time
import requests
from datatime import datatime, timedelta

# number of lines to fetch
# limit=5000
def fetchLokiLogs(query, start_time, end_time, limit=5000):
    # Change base_url as per requirements
    base_url = "https://loki.example.org"
    api_endpoint = f"{base_url}/loki/api/v1/query_range"

    # token is created by encoding the username and password in base64 for
    # authorization. For example: my token is ab324drsc==
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'BASIC ab324drsc=='
    }

    all_results = []
    current_end = end_time

    while current_end > start_time:
        params = {
            "query": query,
            "start": start_time.isoformat() + "Z",
            "end": current_end_time.isoformat() + "Z",
            "limit": limit,
            "step": "1m",
            "direction": "backward"
        }
        try:
            print(f"Sending Request for range: {start_time} to {current_end}")
            response = requests.get(api_endpoint, params=params, headers=headers, timeout=300)
            request_response_code = response.status_code
            #print(f"response status_code: {request_response_code}")
            if response.status_code == 200:
                data = response.json()
                results = data.get("data", {}).get("result", [])

                if not results:
                    print(f"No data found for time range: {start_time} to {current_end}")
                    break

                all_results.extend(results)
                ## Update the end time for the next iteration
                # Get the timestamp of the last entry
                last_timestamp = results[-1]['values'][-1][0]
                current_end = datetime.fromtimestamp(int(last_timestamp) / 1e9)
                print(f"New pagination end time: {current_end}")
            else:
                print(f"Error: {response.status_code}, {response.text}")
                return []

        except requests.exceptions.Timeout:
            print(f"Timeout occurred for interval: {start_time} to {current_end}")
            break

    return all_results

def generateTimeInterval(query, start_time, end_time, interval_minutes):
    current_start = end_time
    while current_start > start_time:
        current_end = current_start
        current_start = max(current_start - timedelta(minutes=interval_minutes), start_time)
        print(f"Querying interval: {current_start} to {current_end}")
        results = fetchLokiLogs(query, current_start, current_end)
        if results:
            for result in results:
                for value in result.get("values", []):
                    yield value
        else:
            print(f"No data found for interval: {current_start} to {current_end}")

# Example Usage
# Change below query as per requirements
query = '{cluster="ops-tools", namespace="loki-dev", job="loki-dev/frontend"}'
end_time = datatime.now()
start_time = end_time - timedelta(hours=696) # 696 hours (29 days) ago
# interval range
interval_minutes = 1440 # 1440 mins --> 24 hrs
# Change below file path as per requirements
output_file_path = '/Users/abc/Downloads/output_pagination.txt'

with open(output_file_path, 'a') as f:
    f.write("Start of Log Entries\n")
    for log_entry in generateTimeInterval(query, start_time, end_time, interval_minutes):
        # log line structure --> <timestamp> <log>
        timestamp, log_line = log_entry
        log_data = json.loads(log_line)
        log_message = log_data['log']
        f.write(f"{log_message}\n")
    f.write("End of Log Entries")
