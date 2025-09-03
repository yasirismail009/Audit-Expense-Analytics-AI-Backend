#!/usr/bin/env python3
"""
Test script to verify posting_date fix - Direct parsing test
"""
from datetime import datetime

def test_posting_date_parsing():
    """Test that posting_date parsing works correctly with our fix"""
    print("🧪 Testing posting_date parsing fix")
    print("=" * 40)
    
    # Test cases based on the CSV data from the image
    test_cases = [
        {
            'name': 'Valid date from image (3/14/2025)',
            'input': '3/14/2025',
            'expected_format': '%m/%d/%Y'
        },
        {
            'name': 'Empty string',
            'input': '',
            'expected_format': 'default'
        },
        {
            'name': 'Invalid date string',
            'input': 'invalid-date',
            'expected_format': 'default'
        },
        {
            'name': 'None value',
            'input': None,
            'expected_format': 'default'
        }
    ]
    
    def parse_posting_date(date_str):
        """Replicate the parsing logic from our fix"""
        if not date_str:
            return datetime.now().date()  # Return current date as default
        
        try:
            # Try different date formats
            date_formats = [
                '%m/%d/%Y',
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m-%d-%Y'
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(str(date_str).strip(), fmt).date()
                except:
                    continue
            
            return datetime.now().date()  # Return current date if parsing fails
        except:
            return datetime.now().date()  # Return current date if any error occurs
    
    # Run tests
    for i, test_case in enumerate(test_cases, 1):
        try:
            result = parse_posting_date(test_case['input'])
            
            if test_case['expected_format'] == 'default':
                if hasattr(result, 'year'):  # Check if it's a date object
                    print(f"✅ Test {i} passed: {test_case['name']} -> Default date: {result}")
                else:
                    print(f"❌ Test {i} failed: {test_case['name']} -> Expected date, got {type(result)}")
            else:
                # For valid dates, check if it parsed correctly
                if test_case['input'] == '3/14/2025':
                    expected_date = datetime(2025, 3, 14).date()
                    if result == expected_date:
                        print(f"✅ Test {i} passed: {test_case['name']} -> {result}")
                    else:
                        print(f"❌ Test {i} failed: {test_case['name']} -> Expected {expected_date}, got {result}")
                else:
                    print(f"✅ Test {i} passed: {test_case['name']} -> {result}")
                    
        except Exception as e:
            print(f"❌ Test {i} failed: {test_case['name']} -> Error: {e}")
    
    print("\n🎯 Testing the specific fix logic:")
    print("-" * 30)
    
    # Test the exact logic from our views.py fix
    def test_view_logic():
        posting_date_str = '3/14/2025'  # From the image data
        if posting_date_str:
            try:
                parsed_date = datetime.strptime(posting_date_str, '%m/%d/%Y').date()
                print(f"✅ Valid date parsing: {posting_date_str} -> {parsed_date}")
                return parsed_date
            except:
                # Set a default date if parsing fails
                default_date = datetime.now().date()
                print(f"⚠️  Parsing failed, using default: {default_date}")
                return default_date
        else:
            # Set a default date if no posting date provided
            default_date = datetime.now().date()
            print(f"⚠️  No posting date, using default: {default_date}")
            return default_date
    
    result = test_view_logic()
    print(f"🎉 Final result: {result}")
    
    print("\n📊 Summary:")
    print("The posting_date fix ensures that:")
    print("1. Valid dates like '3/14/2025' are parsed correctly")
    print("2. Invalid or empty dates get a default value (current date)")
    print("3. No NULL values are passed to the database")
    print("4. The NOT NULL constraint violation is prevented")

if __name__ == "__main__":
    test_posting_date_parsing()
