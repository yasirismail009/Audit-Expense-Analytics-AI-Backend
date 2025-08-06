import requests
import json

# Test the holiday analysis API
url = "http://localhost:8000/api/holiday-analysis/d1ad25f9-79be-4d07-a2df-7b715a606677/"

try:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        
        print("✅ Holiday Analysis API Test Results:")
        print(f"Overall Risk Score: {data['summary']['overall_risk_score']}")
        print(f"Holiday Transactions: {data['summary']['total_holiday_postings']}")
        print(f"Holiday Percentage: {data['summary']['holiday_percentage']}")
        print(f"Risk Level: {data['summary']['risk_level']}")
        print(f"Total Holiday Amount: {data['summary']['total_holiday_amount']}")
        
        # Check if risk score is not 0
        if data['summary']['overall_risk_score'] > 0:
            print("✅ Overall Risk Score is now working correctly!")
        else:
            print("❌ Overall Risk Score is still 0")
            
    else:
        print(f"❌ API request failed with status code: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"❌ Error testing API: {e}") 