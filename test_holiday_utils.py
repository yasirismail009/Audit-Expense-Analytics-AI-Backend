#!/usr/bin/env python3
"""
Test script for the holiday utilities module.

This script demonstrates how to use the holiday utility functions
to fetch holiday data for different countries.
"""

import sys
import os
from datetime import datetime, date

# Add the core directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))

from holiday_utils import (
    get_holidays,
    get_holidays_for_year,
    get_holidays_for_month,
    get_supported_countries,
    is_holiday,
    get_saudi_holidays,
    get_us_holidays,
    get_uk_holidays,
    HolidayAPIError
)


def test_basic_usage():
    """Test basic holiday fetching functionality."""
    print("=== Testing Basic Holiday Fetching ===")
    
    try:
        # Test Saudi Arabia holidays for 2024
        print("\n1. Saudi Arabia holidays for 2024:")
        saudi_holidays = get_holidays_for_year('saudiarabian', 2024)
        for holiday in saudi_holidays:
            print(f"  - {holiday.date}: {holiday.name} ({holiday.holiday_type})")
        
        # Test US holidays for 2024
        print("\n2. US holidays for 2024:")
        us_holidays = get_holidays_for_year('usa', 2024)
        for holiday in us_holidays:
            print(f"  - {holiday.date}: {holiday.name} ({holiday.holiday_type})")
        
        # Test UK holidays for 2024
        print("\n3. UK holidays for 2024:")
        uk_holidays = get_holidays_for_year('uk', 2024)
        for holiday in uk_holidays:
            print(f"  - {holiday.date}: {holiday.name} ({holiday.holiday_type})")
            
    except HolidayAPIError as e:
        print(f"Error: {e}")


def test_date_range_queries():
    """Test holiday fetching with specific date ranges."""
    print("\n=== Testing Date Range Queries ===")
    
    try:
        # Test specific date range for Saudi Arabia
        print("\n1. Saudi Arabia holidays from March to June 2024:")
        saudi_holidays = get_holidays('saudiarabian', '2024-03-01', '2024-06-30')
        for holiday in saudi_holidays:
            print(f"  - {holiday.date}: {holiday.name}")
        
        # Test specific date range for US
        print("\n2. US holidays from July to December 2024:")
        us_holidays = get_holidays('usa', '2024-07-01', '2024-12-31')
        for holiday in us_holidays:
            print(f"  - {holiday.date}: {holiday.name}")
            
    except HolidayAPIError as e:
        print(f"Error: {e}")


def test_monthly_queries():
    """Test holiday fetching for specific months."""
    print("\n=== Testing Monthly Queries ===")
    
    try:
        # Test December 2024 for multiple countries
        countries = ['usa', 'uk', 'saudiarabian']
        
        for country in countries:
            print(f"\n{country.upper()} holidays in December 2024:")
            holidays = get_holidays_for_month(country, 2024, 12)
            for holiday in holidays:
                print(f"  - {holiday.date}: {holiday.name}")
                
    except HolidayAPIError as e:
        print(f"Error: {e}")


def test_holiday_check():
    """Test checking if specific dates are holidays."""
    print("\n=== Testing Holiday Date Check ===")
    
    try:
        # Test specific dates
        test_dates = [
            ('2024-12-25', 'Christmas Day'),
            ('2024-07-04', 'Independence Day'),
            ('2024-09-23', 'Saudi National Day'),
        ]
        
        for date_str, expected_holiday in test_dates:
            # Check for US
            is_us_holiday = is_holiday('usa', date_str)
            print(f"{date_str} ({expected_holiday}) - US Holiday: {is_us_holiday}")
            
            # Check for Saudi Arabia
            is_saudi_holiday = is_holiday('saudiarabian', date_str)
            print(f"{date_str} ({expected_holiday}) - Saudi Holiday: {is_saudi_holiday}")
            
    except HolidayAPIError as e:
        print(f"Error: {e}")


def test_convenience_functions():
    """Test convenience functions for specific countries."""
    print("\n=== Testing Convenience Functions ===")
    
    try:
        # Test Saudi Arabia convenience function
        print("\n1. Saudi Arabia holidays (convenience function):")
        saudi_holidays = get_saudi_holidays('2024-01-01', '2024-12-31')
        for holiday in saudi_holidays[:5]:  # Show first 5
            print(f"  - {holiday.date}: {holiday.name}")
        
        # Test US convenience function
        print("\n2. US holidays (convenience function):")
        us_holidays = get_us_holidays('2024-01-01', '2024-12-31')
        for holiday in us_holidays[:5]:  # Show first 5
            print(f"  - {holiday.date}: {holiday.name}")
            
    except HolidayAPIError as e:
        print(f"Error: {e}")


def test_supported_countries():
    """Test getting list of supported countries."""
    print("\n=== Testing Supported Countries ===")
    
    try:
        countries = get_supported_countries()
        print(f"Total supported countries: {len(countries)}")
        print("First 10 countries:", countries[:10])
        print("Last 10 countries:", countries[-10:])
        
    except Exception as e:
        print(f"Error: {e}")


def test_error_handling():
    """Test error handling for invalid inputs."""
    print("\n=== Testing Error Handling ===")
    
    # Test invalid country code
    try:
        get_holidays('invalid_country', '2024-01-01', '2024-12-31')
    except (ValueError, HolidayAPIError) as e:
        print(f"Expected error for invalid country: {e}")
    
    # Test invalid date format
    try:
        get_holidays('usa', 'invalid-date', '2024-12-31')
    except (ValueError, HolidayAPIError) as e:
        print(f"Expected error for invalid date: {e}")


def main():
    """Main test function."""
    print("Holiday Utilities Test Script")
    print("=" * 50)
    
    # Run all tests
    test_basic_usage()
    test_date_range_queries()
    test_monthly_queries()
    test_holiday_check()
    test_convenience_functions()
    test_supported_countries()
    test_error_handling()
    
    print("\n" + "=" * 50)
    print("Test completed!")


if __name__ == "__main__":
    main() 