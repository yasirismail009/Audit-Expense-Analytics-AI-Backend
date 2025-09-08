#!/usr/bin/env python3
"""
Test script for the redesigned FileAnalysisStatisticsView API
"""

import requests
import json
import sys

def test_file_analysis_statistics():
    """Test the file analysis statistics API"""
    
    # Base URL - adjust this to match your Django server
    base_url = "http://localhost:8000"
    
    # Test endpoint
    endpoint = "/core/file-analysis-statistics/"
    
    # You'll need to provide a valid file ID
    # This should be a UUID of an existing DataFile in your database
    file_id = input("Enter a valid file ID (UUID): ").strip()
    
    if not file_id:
        print("No file ID provided. Exiting.")
        return
    
    # Construct the full URL
    url = f"{base_url}{endpoint}{file_id}/"
    
    print(f"Testing API endpoint: {url}")
    print("=" * 50)
    
    try:
        # Make the GET request
        response = requests.get(url, timeout=30)
        
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("\n✅ SUCCESS: API returned data successfully!")
            
            # Parse and display the response
            try:
                data = response.json()
                print("\n📊 Response Data Structure:")
                print(json.dumps(data, indent=2, default=str))
                
                # Display key statistics
                if 'file_info' in data:
                    print(f"\n📁 File Info:")
                    print(f"  - File Name: {data['file_info'].get('file_name', 'N/A')}")
                    print(f"  - Client: {data['file_info'].get('client_name', 'N/A')}")
                    print(f"  - Total Records: {data['file_info'].get('total_records', 0)}")
                
                if 'transaction_statistics' in data:
                    print(f"\n💳 Transaction Statistics:")
                    print(f"  - Total Transactions: {data['transaction_statistics'].get('total_transactions', 0)}")
                    print(f"  - Total Amount: {data['transaction_statistics'].get('total_amount', 0)}")
                    print(f"  - Unique Accounts: {data['transaction_statistics'].get('unique_accounts', 0)}")
                    print(f"  - Unique Users: {data['transaction_statistics'].get('unique_users', 0)}")
                
                if 'analysis_coverage' in data:
                    print(f"\n🔍 Analysis Coverage:")
                    print(f"  - Total Analyses: {data['analysis_coverage'].get('total_analyses', 0)}")
                    print(f"  - Analysis Types: {', '.join(data['analysis_coverage'].get('analysis_types', []))}")
                
                if 'summary_metrics' in data:
                    print(f"\n📈 Summary Metrics:")
                    print(f"  - Total Anomalies: {data['summary_metrics'].get('total_anomalies', 0)}")
                    print(f"  - Anomaly Percentage: {data['summary_metrics'].get('anomaly_percentage', 0):.2f}%")
                    print(f"  - Overall Risk Score: {data['summary_metrics'].get('overall_risk_score', 0)}")
                    print(f"  - Risk Level: {data['summary_metrics'].get('risk_level', 'N/A')}")
                
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON response: {e}")
                print(f"Raw response: {response.text}")
                
        elif response.status_code == 404:
            print("\n❌ NOT FOUND: File with the specified ID was not found")
            print("Make sure you're using a valid file ID from your database")
            
        elif response.status_code == 500:
            print("\n❌ SERVER ERROR: Internal server error occurred")
            try:
                error_data = response.json()
                print(f"Error details: {error_data}")
            except:
                print(f"Raw error response: {response.text}")
                
        else:
            print(f"\n❌ UNEXPECTED STATUS: {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ CONNECTION ERROR: Could not connect to the server")
        print("Make sure your Django server is running on the specified URL")
        
    except requests.exceptions.Timeout:
        print("❌ TIMEOUT: Request timed out")
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {e}")

if __name__ == "__main__":
    print("🧪 File Analysis Statistics API Test")
    print("=" * 50)
    test_file_analysis_statistics()
