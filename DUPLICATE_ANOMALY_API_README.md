# Duplicate Anomaly Detection API

## Overview

The Duplicate Anomaly Detection API provides comprehensive analysis for identifying duplicate journal entries in SAP GL posting data. The API consolidates all duplicate detection functionality into a single endpoint that accepts a `sheet_id` parameter to analyze data for a specific sheet/file.

## API Endpoints

### 1. GET `/api/duplicate-anomalies/`
**Get comprehensive duplicate anomaly data for a specific sheet**

**Parameters:**
- `sheet_id` (required): ID of the sheet/file to analyze
- `duplicate_threshold` (optional): Minimum count for duplicate detection (default: 2)
- `date_from` (optional): Start date filter (YYYY-MM-DD)
- `date_to` (optional): End date filter (YYYY-MM-DD)
- `min_amount` (optional): Minimum amount filter
- `max_amount` (optional): Maximum amount filter
- `gl_accounts` (optional): List of GL accounts to filter
- `users` (optional): List of users to filter
- `document_types` (optional): List of document types to filter
- `duplicate_types` (optional): List of duplicate types to include (1-6)

**Example:**
```bash
curl -X GET 'http://localhost:8000/api/duplicate-anomalies/?sheet_id=YOUR_SHEET_ID&duplicate_threshold=2&date_from=2024-01-01&date_to=2024-12-31'
```

### 2. POST `/api/duplicate-anomalies/analyze/`
**Run comprehensive duplicate analysis with custom parameters**

**Request Body:**
```json
{
  "sheet_id": "YOUR_SHEET_ID",
  "duplicate_threshold": 3,
  "include_all_types": true,
  "duplicate_types": [1, 2, 3],
  "date_from": "2024-01-01",
  "date_to": "2024-12-31",
  "min_amount": 1000,
  "max_amount": 1000000,
  "gl_accounts": ["131005", "131010"],
  "users": ["USER1", "USER2"],
  "document_types": ["DZ", "SA"]
}
```

**Example:**
```bash
curl -X POST 'http://localhost:8000/api/duplicate-anomalies/analyze/' \
  -H 'Content-Type: application/json' \
  -d '{
    "sheet_id": "YOUR_SHEET_ID",
    "duplicate_threshold": 3,
    "include_all_types": false,
    "duplicate_types": [1, 2, 3],
    "date_from": "2024-01-01",
    "date_to": "2024-12-31",
    "min_amount": 1000,
    "max_amount": 1000000
  }'
```

## Response Structure

Both endpoints return the same comprehensive response structure:

```json
{
  "sheet_id": "YOUR_SHEET_ID",
  "total_duplicates": 15,
  "total_transactions_involved": 45,
  "total_amount_involved": 1250000.00,
  "type_breakdown": {
    "Type 1 Duplicate": {
      "count": 5,
      "total_transactions": 15,
      "total_amount": 500000.00
    },
    "Type 2 Duplicate": {
      "count": 3,
      "total_transactions": 9,
      "total_amount": 300000.00
    }
  },
  "duplicates": [
    {
      "type": "Type 1 Duplicate",
      "criteria": "Account Number + Amount",
      "gl_account": "131005",
      "amount": 100000.00,
      "count": 3,
      "risk_score": 30,
      "transactions": [
        {
          "id": "transaction_id",
          "gl_account": "131005",
          "amount": 100000.00,
          "user_name": "USER1",
          "posting_date": "2024-01-15",
          "document_number": "DOC001"
        }
      ]
    }
  ],
  "charts_data": {
    "duplicate_flags_breakdown": {
      "labels": ["Type 1", "Type 2", "Type 3", "Type 4", "Type 5", "Type 6"],
      "data": [5, 3, 2, 2, 2, 1]
    },
    "monthly_duplicate_data": [
      {
        "month": "2024-01",
        "debit_amount": 500000.00,
        "credit_amount": 300000.00,
        "journal_line_count": 20,
        "duplicate_count": 8
      }
    ],
    "user_breakdown": [
      {
        "user_name": "USER1",
        "duplicate_count": 10,
        "total_amount": 400000.00,
        "duplicate_types": ["Type 1 Duplicate", "Type 3 Duplicate"]
      }
    ],
    "duplicate_type_breakdown": {
      "Type 3 Duplicate": {
        "count": 2,
        "users": ["USER1", "USER2"],
        "amounts": [100000.00, 150000.00]
      },
      "Type 4 Duplicate": {
        "count": 2,
        "dates": ["2024-01-15", "2024-01-20"],
        "amounts": [75000.00, 125000.00]
      }
    },
    "fs_line_breakdown": [
      {
        "gl_account": "131005",
        "duplicate_count": 5,
        "total_amount": 500000.00,
        "duplicate_types": ["Type 1 Duplicate", "Type 2 Duplicate"],
        "transaction_count": 15
      }
    ]
  },
  "training_data": {
    "training_features": [[100000.0, 6, 5, 0, 15, 1, 1, 10], ...],
    "training_labels": [1, 0, 1, ...],
    "total_samples": 1000,
    "duplicate_samples": 150,
    "non_duplicate_samples": 850,
    "feature_importance": {
      "amount": 0.85,
      "gl_account_len": 0.45,
      "user_name_len": 0.32,
      "day_of_week": 0.28,
      "day_of_month": 0.15,
      "month": 0.12,
      "is_debit": 0.08,
      "text_len": 0.05
    },
    "model_metrics": {
      "total_samples": 1000,
      "duplicate_samples": 150,
      "non_duplicate_samples": 850,
      "duplicate_ratio": 0.15
    }
  }
}
```

## Duplicate Types

The API detects 6 types of duplicates:

### Type 1: Account Number + Amount
- **Criteria**: Same GL Account + Same Amount
- **Risk Score**: Count × 10
- **Use Case**: Identifies exact duplicate amounts for the same account

### Type 2: Account Number + Source + Amount
- **Criteria**: Same GL Account + Same Document Type + Same Amount
- **Risk Score**: Count × 12
- **Use Case**: Identifies duplicates from the same source document

### Type 3: Account Number + User + Amount
- **Criteria**: Same GL Account + Same User + Same Amount
- **Risk Score**: Count × 15
- **Use Case**: Identifies user-specific duplicate patterns

### Type 4: Account Number + Posted Date + Amount
- **Criteria**: Same GL Account + Same Posting Date + Same Amount
- **Risk Score**: Count × 18
- **Use Case**: Identifies same-day duplicate entries

### Type 5: Account Number + Effective Date + Amount
- **Criteria**: Same GL Account + Same Document Date + Same Amount
- **Risk Score**: Count × 20
- **Use Case**: Identifies duplicates with same effective date

### Type 6: Account Number + Effective Date + Posted Date + User + Source + Amount
- **Criteria**: All fields match
- **Risk Score**: Count × 25
- **Use Case**: Identifies exact duplicates across all dimensions

## Charts Data

The API provides comprehensive charts data for visualization:

### 1. Duplicate Flags Breakdown
- Pie chart showing distribution of duplicate types
- Labels: Type 1, Type 2, Type 3, Type 4, Type 5, Type 6
- Data: Count of each duplicate type

### 2. Monthly Duplicate Data
- Line chart showing duplicate trends over time
- Includes debit/credit amounts and journal line counts
- Monthly aggregation of duplicate activity

### 3. User Breakdown
- Bar chart showing duplicates per user
- Includes duplicate count, total amount, and duplicate types
- Helps identify users with high duplicate activity

### 4. Duplicate Type Breakdown
- Detailed breakdown for Type 3, 4, 5, 6 duplicates
- Shows users, dates, and amounts for each type
- Provides granular analysis of complex duplicates

### 5. FS Line Breakdown
- Bar chart showing duplicates per financial statement line
- Includes GL account details and duplicate types
- Helps identify accounts with high duplicate activity

## Training Data

The API provides machine learning training data:

### Features
1. **amount**: Transaction amount
2. **gl_account_len**: Length of GL account number
3. **user_name_len**: Length of user name
4. **day_of_week**: Day of week (0-6)
5. **day_of_month**: Day of month (1-31)
6. **month**: Month (1-12)
7. **is_debit**: Binary flag for debit transactions
8. **text_len**: Length of transaction text

### Labels
- **1**: Duplicate transaction
- **0**: Non-duplicate transaction

### Feature Importance
- Correlation-based feature importance scores
- Helps identify which features are most predictive of duplicates

## Sheet ID Mapping

The `sheet_id` parameter can be:

1. **DataFile ID**: UUID of an uploaded data file
   - Filters transactions uploaded after this file
   - Uses `created_at >= file.uploaded_at`

2. **AnalysisSession ID**: UUID of an analysis session
   - Filters transactions within session date range
   - Uses session's `date_from` and `date_to`

3. **Custom Identifier**: Custom logic can be implemented
   - Extend the filtering logic in the view

## Error Handling

### Common Errors

1. **Missing sheet_id**:
   ```json
   {
     "error": "sheet_id parameter is required"
   }
   ```

2. **Invalid sheet_id**:
   - Returns empty results instead of error
   - Allows graceful handling of invalid IDs

3. **No transactions found**:
   ```json
   {
     "sheet_id": "YOUR_SHEET_ID",
     "total_duplicates": 0,
     "total_transactions_involved": 0,
     "total_amount_involved": 0.0,
     "type_breakdown": {},
     "duplicates": [],
     "charts_data": {...},
     "training_data": {...}
   }
   ```

## Usage Examples

### Basic Analysis
```bash
# Simple GET request
curl -X GET 'http://localhost:8000/api/duplicate-anomalies/?sheet_id=123e4567-e89b-12d3-a456-426614174000'
```

### Advanced Analysis
```bash
# POST request with custom parameters
curl -X POST 'http://localhost:8000/api/duplicate-anomalies/analyze/' \
  -H 'Content-Type: application/json' \
  -d '{
    "sheet_id": "123e4567-e89b-12d3-a456-426614174000",
    "duplicate_threshold": 3,
    "include_all_types": false,
    "duplicate_types": [1, 2, 3],
    "date_from": "2024-01-01",
    "date_to": "2024-12-31",
    "min_amount": 1000,
    "max_amount": 1000000
  }'
```

### JavaScript Example
```javascript
// GET request
const response = await fetch('/api/duplicate-anomalies/?sheet_id=YOUR_SHEET_ID');
const data = await response.json();

// POST request
const response = await fetch('/api/duplicate-anomalies/analyze/', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    sheet_id: 'YOUR_SHEET_ID',
    duplicate_threshold: 2,
    date_from: '2024-01-01',
    date_to: '2024-12-31'
  })
});
const data = await response.json();
```

## Testing

Run the test script to verify the API:

```bash
python test_duplicate_anomaly_api.py
```

This will test both GET and POST endpoints with various parameters and show example usage.

## Performance Considerations

1. **Large Datasets**: For large datasets, consider using pagination or limiting date ranges
2. **Duplicate Threshold**: Higher thresholds reduce processing time but may miss some duplicates
3. **Filtering**: Use specific filters to reduce the dataset size
4. **Caching**: Consider caching results for frequently accessed sheet IDs

## Security

1. **Authentication**: Ensure proper authentication is implemented
2. **Authorization**: Verify users have access to the requested sheet_id
3. **Input Validation**: All parameters are validated before processing
4. **Rate Limiting**: Consider implementing rate limiting for API endpoints 