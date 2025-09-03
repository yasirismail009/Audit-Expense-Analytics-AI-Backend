#!/usr/bin/env python3
"""
Check the actual response structure from analysis endpoints
"""

import requests
import json

BASE_URL = "http://localhost:8000/api"
FILE_ID = "96c59456-e1cd-491e-a8da-7f1b0084c081"

def check_endpoint_structure(endpoint, name):
    """Check the structure of a specific endpoint"""
    print(f"\n🔍 Checking {name} structure...")
    print("-" * 50)
    
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}/{FILE_ID}/")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Status: {response.status_code}")
            print(f"📊 Response keys: {list(result.keys())}")
            
            # Check for unified fields
            unified_fields = ['analysis_summary', 'anomaly_list', 'chart_data', 'risk_assessment', 'audit_recommendations', 'compliance_assessment', 'export_data']
            
            print(f"\n🔍 Unified fields check:")
            for field in unified_fields:
                if field in result:
                    print(f"   ✅ {field}: Present")
                    if isinstance(result[field], dict):
                        print(f"      📊 Type: dict with {len(result[field])} keys")
                    elif isinstance(result[field], list):
                        print(f"      📊 Type: list with {len(result[field])} items")
                    else:
                        print(f"      📊 Type: {type(result[field])}")
                else:
                    print(f"   ❌ {field}: Missing")
            
            # Check for old analysis_info field
            if 'analysis_info' in result:
                print(f"\n❌ Found old analysis_info field!")
            else:
                print(f"\n✅ No analysis_info field found (correct)")
                
        else:
            print(f"❌ Status: {response.status_code}")
            print(f"📄 Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Main function"""
    print("🔍 RESPONSE STRUCTURE ANALYSIS")
    print("=" * 50)
    
    endpoints = [
        ('duplicate-analysis', 'Duplicate Analysis'),
        ('backdated-analysis', 'Backdated Analysis'),
        ('user-analysis', 'User Analysis'),
        ('unusual-days-analysis', 'Unusual Days Analysis'),
        ('closing-entries-analysis', 'Closing Entries Analysis'),
        ('holiday-analysis', 'Holiday Analysis')
    ]
    
    for endpoint, name in endpoints:
        check_endpoint_structure(endpoint, name)

if __name__ == "__main__":
    main()
