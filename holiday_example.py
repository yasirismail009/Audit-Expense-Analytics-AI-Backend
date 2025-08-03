#!/usr/bin/env python3
"""
Simple example demonstrating the holiday utility usage.
"""

from core.holiday_utils import (
    get_holidays_for_year,
    get_holidays,
    is_holiday,
    get_supported_countries
)

def main():
    print("🌍 Holiday Utility Example")
    print("=" * 40)
    
    # Example 1: Get all Saudi Arabia holidays for 2024
    print("\n1. Saudi Arabia Holidays 2024:")
    saudi_holidays = get_holidays_for_year('saudiarabian', 2024)
    for holiday in saudi_holidays:
        print(f"   📅 {holiday.date}: {holiday.name}")
    
    # Example 2: Get US holidays for a specific month
    print("\n2. US Holidays in December 2024:")
    us_december_holidays = get_holidays('usa', '2024-12-01', '2024-12-31')
    for holiday in us_december_holidays:
        print(f"   📅 {holiday.date}: {holiday.name}")
    
    # Example 3: Check if specific dates are holidays
    print("\n3. Holiday Check Examples:")
    test_dates = [
        ('2024-12-25', 'Christmas Day'),
        ('2024-07-04', 'Independence Day'),
        ('2024-09-23', 'Saudi National Day'),
    ]
    
    for date_str, holiday_name in test_dates:
        is_us_holiday = is_holiday('usa', date_str)
        is_saudi_holiday = is_holiday('saudiarabian', date_str)
        print(f"   {date_str} ({holiday_name}):")
        print(f"     🇺🇸 US: {'✅' if is_us_holiday else '❌'}")
        print(f"     🇸🇦 Saudi: {'✅' if is_saudi_holiday else '❌'}")
    
    # Example 4: Show supported countries count
    print(f"\n4. Supported Countries: {len(get_supported_countries())} countries")
    print("   Some examples: usa, uk, saudiarabian, canadian, german, french, japanese")

if __name__ == "__main__":
    main() 