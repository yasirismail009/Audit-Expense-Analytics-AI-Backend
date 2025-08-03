# Holiday Utilities Documentation

## Overview

The `holiday_utils.py` module provides a comprehensive utility for fetching holiday data from Google Calendar API for different countries. It supports 150+ countries and territories worldwide, allowing you to retrieve holiday information based on country codes and time ranges.

## Features

- **150+ Countries Supported**: Comprehensive coverage of global holidays
- **Flexible Date Ranges**: Query holidays for specific dates, months, or years
- **Multiple Input Formats**: Accept datetime objects, date objects, or string dates
- **Holiday Types**: Distinguish between public holidays and observances
- **Error Handling**: Robust error handling with custom exceptions
- **Convenience Functions**: Pre-built functions for common countries
- **Type Hints**: Full type annotation support

## Installation

The module requires the following dependencies:

```bash
pip install requests
```

## Quick Start

### Basic Usage

```python
from core.holiday_utils import get_holidays, get_holidays_for_year

# Get all holidays for Saudi Arabia in 2024
holidays = get_holidays_for_year('saudiarabian', 2024)

# Print each holiday
for holiday in holidays:
    print(f"{holiday.date}: {holiday.name} ({holiday.holiday_type})")
```

### Date Range Queries

```python
from core.holiday_utils import get_holidays

# Get holidays for a specific date range
holidays = get_holidays('usa', '2024-07-01', '2024-12-31')

# Using datetime objects
from datetime import datetime
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
holidays = get_holidays('uk', start_date, end_date)
```

### Check if a Date is a Holiday

```python
from core.holiday_utils import is_holiday

# Check if Christmas 2024 is a holiday in the US
is_christmas_holiday = is_holiday('usa', '2024-12-25')
print(f"Is Christmas a US holiday? {is_christmas_holiday}")
```

## API Reference

### Main Functions

#### `get_holidays(country_code, start_date, end_date, include_observances=True)`

Fetch holiday data for a specific country and date range.

**Parameters:**
- `country_code` (str): The country code (e.g., 'usa', 'uk', 'saudiarabian')
- `start_date`: Start date (datetime, date, or 'YYYY-MM-DD' string)
- `end_date`: End date (datetime, date, or 'YYYY-MM-DD' string)
- `include_observances` (bool): Whether to include observances (default: True)

**Returns:**
- `List[HolidayData]`: List of holiday data objects

**Example:**
```python
holidays = get_holidays('saudiarabian', '2024-01-01', '2024-12-31')
```

#### `get_holidays_for_year(country_code, year, include_observances=True)`

Fetch all holidays for a specific country and year.

**Parameters:**
- `country_code` (str): The country code
- `year` (int): The year to fetch holidays for
- `include_observances` (bool): Whether to include observances (default: True)

**Returns:**
- `List[HolidayData]`: List of holiday data objects for the year

**Example:**
```python
holidays = get_holidays_for_year('usa', 2024)
```

#### `get_holidays_for_month(country_code, year, month, include_observances=True)`

Fetch all holidays for a specific country, year, and month.

**Parameters:**
- `country_code` (str): The country code
- `year` (int): The year
- `month` (int): The month (1-12)
- `include_observances` (bool): Whether to include observances (default: True)

**Returns:**
- `List[HolidayData]`: List of holiday data objects for the month

**Example:**
```python
holidays = get_holidays_for_month('uk', 2024, 12)
```

#### `is_holiday(country_code, check_date)`

Check if a specific date is a holiday in the given country.

**Parameters:**
- `country_code` (str): The country code
- `check_date`: The date to check (datetime, date, or 'YYYY-MM-DD' string)

**Returns:**
- `bool`: True if the date is a holiday, False otherwise

**Example:**
```python
is_holiday_today = is_holiday('usa', '2024-07-04')
```

### Utility Functions

#### `get_supported_countries()`

Get a list of all supported country codes.

**Returns:**
- `List[str]`: List of supported country codes

**Example:**
```python
countries = get_supported_countries()
print(f"Supported countries: {countries}")
```

#### `get_holiday_calendar_id(country_code)`

Get the Google Calendar ID for a given country code.

**Parameters:**
- `country_code` (str): The country code

**Returns:**
- `str`: The Google Calendar ID for the country

**Raises:**
- `ValueError`: If the country code is not supported

### Convenience Functions

#### `get_saudi_holidays(start_date, end_date)`

Get holidays for Saudi Arabia.

#### `get_us_holidays(start_date, end_date)`

Get holidays for the United States.

#### `get_uk_holidays(start_date, end_date)`

Get holidays for the United Kingdom.

### Data Classes

#### `HolidayData`

Data class representing holiday information.

**Attributes:**
- `name` (str): Name of the holiday
- `date` (str): Date of the holiday (YYYY-MM-DD format)
- `description` (str): Description of the holiday
- `holiday_type` (str): Type of holiday ("Public holiday" or "Observance")

**Methods:**
- `to_dict()`: Convert to dictionary format
- `__repr__()`: String representation

**Example:**
```python
holiday = HolidayData(
    name="Christmas Day",
    date="2024-12-25",
    description="Public holiday",
    holiday_type="Public holiday"
)
```

### Exceptions

#### `HolidayAPIError`

Custom exception for holiday API errors.

**Example:**
```python
try:
    holidays = get_holidays('usa', '2024-01-01', '2024-12-31')
except HolidayAPIError as e:
    print(f"API Error: {e}")
```

## Supported Countries

The module supports 150+ countries and territories. Here are some examples:

| Country | Code | Country | Code |
|---------|------|---------|------|
| Afghanistan | `af` | United States | `usa` |
| Albania | `al` | United Kingdom | `uk` |
| Algeria | `dz` | Saudi Arabia | `saudiarabian` |
| Australia | `australian` | Canada | `canadian` |
| Austria | `austrian` | Germany | `german` |
| Brazil | `brazilian` | France | `french` |
| China | `china` | Japan | `japanese` |
| India | `indian` | Russia | `russian` |

For a complete list, use:
```python
from core.holiday_utils import get_supported_countries
countries = get_supported_countries()
```

## Usage Examples

### Example 1: Get All Holidays for a Year

```python
from core.holiday_utils import get_holidays_for_year

# Get all US holidays for 2024
us_holidays = get_holidays_for_year('usa', 2024)

print("US Holidays 2024:")
for holiday in us_holidays:
    print(f"  {holiday.date}: {holiday.name}")
```

### Example 2: Check Specific Dates

```python
from core.holiday_utils import is_holiday
from datetime import date

# Check multiple dates
test_dates = [
    date(2024, 12, 25),  # Christmas
    date(2024, 7, 4),    # Independence Day
    date(2024, 1, 1),    # New Year's Day
]

for test_date in test_dates:
    is_holiday_us = is_holiday('usa', test_date)
    print(f"{test_date}: US Holiday = {is_holiday_us}")
```

### Example 3: Compare Holidays Across Countries

```python
from core.holiday_utils import get_holidays_for_month

# Compare December holidays across countries
countries = ['usa', 'uk', 'saudiarabian']

for country in countries:
    holidays = get_holidays_for_month(country, 2024, 12)
    print(f"\n{country.upper()} December 2024 Holidays:")
    for holiday in holidays:
        print(f"  {holiday.date}: {holiday.name}")
```

### Example 4: Filter by Holiday Type

```python
from core.holiday_utils import get_holidays_for_year

# Get only public holidays (exclude observances)
public_holidays = get_holidays_for_year('usa', 2024, include_observances=False)

print("US Public Holidays 2024 (excluding observances):")
for holiday in public_holidays:
    print(f"  {holiday.date}: {holiday.name}")
```

### Example 5: Error Handling

```python
from core.holiday_utils import get_holidays, HolidayAPIError

try:
    # Try to get holidays for an invalid country
    holidays = get_holidays('invalid_country', '2024-01-01', '2024-12-31')
except ValueError as e:
    print(f"Invalid country code: {e}")
except HolidayAPIError as e:
    print(f"API error: {e}")
```

## Configuration

### API Key

The module uses a Google Calendar API key. The current key is embedded in the module, but for production use, consider:

1. Moving the API key to environment variables
2. Using a configuration file
3. Implementing API key rotation

### Rate Limiting

The Google Calendar API has rate limits. For high-volume usage:

1. Implement caching mechanisms
2. Add delays between requests
3. Consider using batch requests

## Testing

Run the test script to verify functionality:

```bash
python test_holiday_utils.py
```

The test script includes:
- Basic functionality tests
- Date range queries
- Monthly queries
- Holiday checking
- Convenience functions
- Error handling
- Supported countries listing

## Performance Considerations

1. **Caching**: Consider implementing caching for frequently requested data
2. **Batch Requests**: For multiple countries, consider batching requests
3. **Connection Pooling**: Use connection pooling for high-volume requests
4. **Error Retry**: Implement retry logic for transient failures

## Troubleshooting

### Common Issues

1. **Invalid Country Code**: Use `get_supported_countries()` to see valid codes
2. **Date Format**: Ensure dates are in 'YYYY-MM-DD' format
3. **API Errors**: Check network connectivity and API key validity
4. **Rate Limiting**: Implement delays between requests

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

To add support for new countries:

1. Add the country code and calendar ID to `COUNTRY_CALENDAR_MAPPING`
2. Update the documentation
3. Add tests for the new country
4. Verify the calendar ID works with the Google Calendar API

## License

This module is part of the analytics project and follows the same licensing terms.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review the test script for usage examples
3. Verify country codes with `get_supported_countries()`
4. Test with a known working country first 