#!/usr/bin/env python3
"""
Test script to verify API fixes are working
"""

import requests
import json
import time

def test_api_endpoints():
    """Test the main API endpoints"""
    base_url = "http://localhost:8000/api"
    
    print("=== API Endpoint Test ===\n")
    
    # Test 1: Check if server is running
    try:
        response = requests.get(f"{base_url}/")
        print(f"✅ Server Status: {response.status_code}")
        if response.status_code == 200:
            print(f"   Response: {response.json()}")
    except requests.exceptions.ConnectionError:
        print("❌ Server not running. Please start Django server with: python manage.py runserver")
        return
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    print()
    
    # Test 2: List expense sheets
    try:
        response = requests.get(f"{base_url}/sheets/")
        print(f"✅ List Sheets: {response.status_code}")
        if response.status_code == 200:
            sheets = response.json()
            print(f"   Found {len(sheets)} expense sheets")
            if sheets:
                print(f"   First sheet: {sheets[0].get('display_name', 'Unknown')}")
        else:
            print(f"   Error: {response.text}")
    except Exception as e:
        print(f"❌ Error listing sheets: {e}")
    
    print()
    
    # Test 3: Analyze a specific sheet (if available)
    try:
        response = requests.get(f"{base_url}/sheets/")
        if response.status_code == 200:
            sheets = response.json()
            if sheets:
                sheet_id = sheets[0]['id']
                print(f"🔍 Testing analysis for sheet ID: {sheet_id}")
                
                # Start analysis
                response = requests.post(f"{base_url}/sheets/{sheet_id}/analyze/")
                print(f"✅ Analysis Request: {response.status_code}")
                
                if response.status_code == 200:
                    result = response.json()
                    print(f"   Analysis completed successfully!")
                    print(f"   Sheet: {result.get('sheet_name', 'Unknown')}")
                    print(f"   Risk Level: {result.get('analysis_summary', {}).get('risk_level', 'Unknown')}")
                    print(f"   Fraud Score: {result.get('analysis_summary', {}).get('overall_fraud_score', 0):.1f}")
                    print(f"   Flagged Expenses: {result.get('analysis_summary', {}).get('total_flagged_expenses', 0)}")
                    
                    # Check for advanced metrics
                    advanced_metrics = result.get('advanced_metrics', {})
                    if advanced_metrics:
                        print(f"   Advanced Metrics Available:")
                        print(f"     - EVR: ${advanced_metrics.get('expense_velocity_ratio', 0):.2f}/day")
                        print(f"     - ACI: {advanced_metrics.get('approval_concentration_index', 0):.1f}%")
                        print(f"     - PMRS: {advanced_metrics.get('payment_method_risk_score', 0):.1f}%")
                        print(f"     - VCR: {advanced_metrics.get('vendor_concentration_ratio', 0):.1f}%")
                    
                    # Check for flagged expenses
                    flagged_expenses = result.get('flagged_expenses', [])
                    if flagged_expenses:
                        print(f"   Flagged Expenses:")
                        for expense in flagged_expenses[:3]:  # Show first 3
                            print(f"     - ${expense.get('amount', 0)}: {expense.get('description', 'Unknown')} (Score: {expense.get('fraud_score', 0):.1f})")
                        if len(flagged_expenses) > 3:
                            print(f"     ... and {len(flagged_expenses) - 3} more")
                else:
                    print(f"   Error: {response.text}")
            else:
                print("ℹ️  No sheets available for testing")
        else:
            print(f"   Error getting sheets: {response.text}")
    except Exception as e:
        print(f"❌ Error testing analysis: {e}")
    
    print()
    
    # Test 4: Check model training status
    try:
        response = requests.get(f"{base_url}/train/")
        print(f"✅ Model Status: {response.status_code}")
        if response.status_code == 200:
            status = response.json()
            print(f"   Models Available: {status.get('models_available', [])}")
            print(f"   Total Sheets: {status.get('total_sheets', 0)}")
            print(f"   Training Ready: {status.get('training_ready', False)}")
        else:
            print(f"   Error: {response.text}")
    except Exception as e:
        print(f"❌ Error checking model status: {e}")
    
    print("\n=== Test Complete ===")

def test_simple_analytics():
    """Test the simple analytics without Django"""
    print("\n=== Simple Analytics Test ===")
    
    try:
        from test_analytics_simple import SimpleExpenseAnalyzer, create_sample_data
        
        # Create test data
        data = create_sample_data()
        print(f"✅ Created {len(data)} sample expenses")
        
        # Run analysis
        analyzer = SimpleExpenseAnalyzer()
        results = analyzer.analyze_expenses(data)
        
        if results:
            print("✅ Simple analytics completed successfully!")
            print(f"   Total amount: ${results['basic_metrics']['total_amount']:,.2f}")
            print(f"   EVR: ${results['expense_velocity_ratio']:.2f}/day")
            print(f"   ACI: {results['approval_concentration_index']:.1f}%")
            print(f"   PMRS: {results['payment_method_risk_score']:.1f}%")
        else:
            print("❌ Simple analytics failed!")
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("Testing Expense Analytics API Fixes")
    print("="*50)
    
    # Test simple analytics first
    test_simple_analytics()
    
    # Test API endpoints
    test_api_endpoints() 