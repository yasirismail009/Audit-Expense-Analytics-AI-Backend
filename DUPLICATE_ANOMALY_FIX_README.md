# Duplicate Anomaly API Fixes

## Overview

This document outlines the fixes applied to the Duplicate Anomaly API to resolve the `'datetime.date' object has no attribute 'dayofweek'` error and improve overall functionality.

## Issues Fixed

### 1. Date Handling Error
**Problem**: The `_generate_training_data` method was trying to access pandas datetime attributes (`dayofweek`, `day`, `month`) on `datetime.date` objects, which don't have these attributes.

**Solution**: 
- Convert `datetime.date` objects to pandas datetime objects using `pd.to_datetime()`
- Properly handle date attributes in the feature generation process
- Added proper null checking for dates

### 2. Data Variable Access Issues
**Problem**: The `data` variable in the analyze method could be `None`, causing attribute access errors.

**Solution**:
- Added null checks before accessing `data.get()` methods
- Set default values when `data` is `None`
- Improved error handling for missing or invalid parameters

### 3. Date Assignment Issues
**Problem**: Direct assignment of `datetime.date` objects to Django DateField attributes was causing type errors.

**Solution**:
- Store parsed dates in intermediate variables before assignment
- Improved date parsing error handling

## API Endpoints

### 1. List Duplicates (GET)
**Endpoint**: `/api/duplicate-anomalies/`

**Parameters**:
- `sheet_id` (required): ID of the sheet/data file to analyze
- `duplicate_threshold` (optional): Minimum count for duplicates (default: 2)
- `date_from` (optional): Start date filter
- `date_to` (optional): End date filter
- `min_amount` (optional): Minimum amount filter
- `max_amount` (optional): Maximum amount filter
- `gl_accounts` (optional): List of GL accounts to filter
- `users` (optional): List of users to filter
- `document_types` (optional): List of document types to filter
- `duplicate_types` (optional): List of duplicate types to include

**Response Structure**:
```json
{
  "sheet_id": "1",
  "total_duplicates": 5,
  "total_transactions_involved": 15,
  "total_amount_involved": 150000.00,
  "type_breakdown": {
    "Type 1 Duplicate": {
      "count": 2,
      "total_transactions": 6,
      "total_amount": 60000.00
    }
  },
  "duplicates": [...],
  "charts_data": {
    "duplicate_flags_breakdown": {
      "labels": ["Type 1", "Type 2", "Type 3", "Type 4", "Type 5", "Type 6"],
      "data": [2, 1, 1, 0, 1, 0]
    },
    "monthly_duplicate_data": [...],
    "user_breakdown": [...],
    "duplicate_type_breakdown": {...},
    "fs_line_breakdown": [...]
  },
  "training_data": {
    "training_features": [...],
    "training_labels": [...],
    "total_samples": 1000,
    "duplicate_samples": 50,
    "non_duplicate_samples": 950,
    "feature_importance": {...},
    "model_metrics": {...}
  }
}
```

### 2. Analyze Duplicates (POST)
**Endpoint**: `/api/duplicate-anomalies/analyze/`

**Request Body**:
```json
{
  "sheet_id": "1",
  "duplicate_threshold": 2,
  "include_all_types": true,
  "date_from": "2024-01-01",
  "date_to": "2024-12-31",
  "min_amount": 1000,
  "max_amount": 1000000,
  "gl_accounts": ["1000", "2000"],
  "users": ["USER1", "USER2"],
  "document_types": ["SA", "SC"],
  "duplicate_types": [1, 2, 3]
}
```

**Response**: Same structure as the GET endpoint with additional `message` field.

## Duplicate Types

The API detects 6 types of duplicates:

1. **Type 1**: Same account number, amount, user, posting date, document date, and source
2. **Type 2**: Same account number, amount, user, posting date, and document date
3. **Type 3**: Same account number, amount, user, and posting date
4. **Type 4**: Same account number, amount, and posting date
5. **Type 5**: Same account number, amount, and document date
6. **Type 6**: Same account number, amount, user, posting date, and document date (different source)

## Sheet ID Logic

The `sheet_id` parameter is used to filter transactions in the following ways:

1. **If sheet_id is a DataFile ID**: Filters transactions uploaded after the file's upload time
2. **If sheet_id is an AnalysisSession ID**: Filters transactions within the session's date range
3. **If sheet_id doesn't exist**: Returns empty results gracefully

## Error Handling

The API now properly handles:

- Missing `sheet_id` parameter (returns 400 error)
- Invalid `sheet_id` values (returns empty results)
- Date parsing errors (skips invalid dates)
- Missing or invalid request data (uses defaults)
- Database query errors (returns 500 error with details)

## Testing

Use the provided test script to verify the API functionality:

```bash
python test_duplicate_anomaly_fix.py
```

The test script covers:
- Basic functionality with valid parameters
- Error handling for missing parameters
- Error handling for invalid parameters
- Response structure validation

## Usage Examples

### Basic Usage
```python
import requests

# Get duplicates for sheet_id 1
response = requests.get('http://localhost:8000/api/duplicate-anomalies/', 
                       params={'sheet_id': '1'})
data = response.json()
print(f"Found {data['total_duplicates']} duplicate groups")
```

### Advanced Analysis
```python
# Run custom analysis
payload = {
    'sheet_id': '1',
    'duplicate_threshold': 3,
    'date_from': '2024-01-01',
    'date_to': '2024-12-31',
    'min_amount': 50000,
    'duplicate_types': [1, 2]  # Only Type 1 and Type 2 duplicates
}

response = requests.post('http://localhost:8000/api/duplicate-anomalies/analyze/', 
                        json=payload)
data = response.json()
print(f"Analysis completed: {data['message']}")
```

### Error Handling
```python
# Handle missing sheet_id
try:
    response = requests.get('http://localhost:8000/api/duplicate-anomalies/')
    if response.status_code == 400:
        error_data = response.json()
        print(f"Error: {error_data['error']}")
except Exception as e:
    print(f"Request failed: {e}")
```

## Performance Considerations

- The API processes transactions in memory for duplicate detection
- Large datasets may require pagination or chunking
- Consider using background tasks for very large datasets
- Training data generation can be resource-intensive

## Future Improvements

1. **Pagination**: Add pagination support for large result sets
2. **Caching**: Implement caching for repeated queries
3. **Background Processing**: Move heavy processing to background tasks
4. **Real-time Updates**: Add WebSocket support for real-time duplicate detection
5. **Advanced Filtering**: Add more sophisticated filtering options
6. **Export Options**: Add CSV/Excel export functionality

## Troubleshooting

### Common Issues

1. **"sheet_id parameter is required"**: Make sure to include the sheet_id parameter
2. **"No transactions found"**: Check if the sheet_id corresponds to existing data
3. **Date parsing errors**: Ensure dates are in the correct format (YYYY-MM-DD)
4. **Memory issues**: Consider reducing the date range or adding filters

### Debug Mode

Enable debug logging by setting the log level to DEBUG in your Django settings:

```python
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'core.views': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
    },
}
```

## Support

For issues or questions regarding the Duplicate Anomaly API:

1. Check the error logs for detailed error messages
2. Verify the request parameters and data format
3. Test with the provided test script
4. Review the API documentation and examples 