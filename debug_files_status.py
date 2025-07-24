#!/usr/bin/env python3
"""
Debug script to check file statuses
"""

import requests
import json

BASE_URL = "http://localhost:8000/api"
HEADERS = {'Content-Type': 'application/json'}

def main():
    try:
        response = requests.get(f"{BASE_URL}/all-files/", headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            print(f"Response type: {type(data)}")
            print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            print(f"Raw response: {data}")
            
            if isinstance(data, dict) and 'files' in data:
                files = data['files']
                print(f"\nFound {len(files)} files")
                print("\nFile Details:")
                for i, file_info in enumerate(files):
                    if isinstance(file_info, dict):
                        print(f"{i+1}. ID: {file_info.get('id', 'N/A')}")
                        print(f"   Name: {file_info.get('file_name', 'Unknown')}")
                        print(f"   Status: {file_info.get('status', 'Unknown')}")
                        print(f"   Records: {file_info.get('total_records', 0)}")
                    else:
                        print(f"{i+1}. Raw data: {file_info}")
                    print()
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 