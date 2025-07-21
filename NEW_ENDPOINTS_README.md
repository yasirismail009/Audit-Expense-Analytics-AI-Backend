# New File Management Endpoints

This document describes the three new API endpoints created for file management in the SAP Analytics system.

## Overview

The new endpoints provide functionality for:
1. **Listing uploaded files** - Get a list of all uploaded data files with filtering options
2. **Uploading files with automatic analysis** - Upload CSV files and automatically run analysis
3. **Getting file summaries** - Retrieve detailed information about a specific file including statistics and analysis results

## Endpoints

### 1. File List Endpoint

**URL:** `GET /api/file-list/`

**Description:** Retrieves a list of all uploaded data files with optional filtering and summary statistics.

**Query Parameters:**
- `status` (optional): Filter by file status (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `PARTIAL`)
- `date_from` (optional): Filter files uploaded from this date (YYYY-MM-DD)
- `date_to` (optional): Filter files uploaded until this date (YYYY-MM-DD)

**Response:**
```json
{
  "files": [
    {
      "id": "uuid",
      "file_name": "example.csv",
      "file_size": 1024,
      "total_records": 100,
      "processed_records": 95,
      "failed_records": 5,
      "status": "COMPLETED",
      "uploaded_at": "2024-01-15T10:00:00Z",
      "processed_at": "2024-01-15T10:05:00Z",
      "min_date": "2024-01-01",
      "max_date": "2024-01-31",
      "min_amount": "1000.00",
      "max_amount": "1000000.00"
    }
  ],
  "summary": {
    "total_files": 1,
    "total_records": 100,
    "total_processed": 95,
    "total_failed": 5,
    "success_rate": 95.0
  }
}
```

**Example Usage:**
```bash
# Get all files
curl -X GET "http://localhost:8000/api/file-list/"

# Get only completed files
curl -X GET "http://localhost:8000/api/file-list/?status=COMPLETED"

# Get files uploaded in January 2024
curl -X GET "http://localhost:8000/api/file-list/?date_from=2024-01-01&date_to=2024-01-31"
```

### 2. File Upload and Analysis Endpoint

**URL:** `POST /api/file-upload-analysis/`

**Description:** Uploads a CSV file and automatically processes it, then runs analysis on the data.

**Request:**
- Content-Type: `multipart/form-data`
- Body: Form data with `file` field containing the CSV file

**CSV Format Requirements:**
The CSV file must contain the following columns:
- Document Number
- Posting Date (YYYY-MM-DD)
- G/L Account
- Amount in Local Currency
- Local Currency
- Text
- Document Date (YYYY-MM-DD)
- Offsetting Account
- User Name
- Entry Date (YYYY-MM-DD)
- Document Type
- Profit Center
- Cost Center
- Clearing Document
- Segment
- WBS Element
- Plant
- Material
- Invoice Reference
- Billing Document
- Sales Document
- Purchasing Document
- Order Number
- Asset Number
- Network
- Assignment
- Tax Code
- Account Assignment
- Fiscal Year
- Posting Period
- Year/Month

**Response:**
```json
{
  "file": {
    "id": "uuid",
    "file_name": "test_data.csv",
    "file_size": 1024,
    "total_records": 3,
    "processed_records": 3,
    "failed_records": 0,
    "status": "COMPLETED",
    "uploaded_at": "2024-01-15T10:00:00Z",
    "processed_at": "2024-01-15T10:05:00Z"
  },
  "analysis": {
    "session_id": "uuid",
    "total_transactions": 3,
    "total_amount": 245000.0,
    "flagged_transactions": 0,
    "high_value_transactions": 0,
    "analysis_status": "COMPLETED",
    "flag_rate": 0.0,
    "anomaly_summary": {
      "amount_anomalies": 0,
      "timing_anomalies": 0,
      "user_anomalies": 0,
      "account_anomalies": 0,
      "pattern_anomalies": 0
    }
  },
  "message": "File uploaded and analysis completed successfully"
}
```

**Example Usage:**
```bash
curl -X POST "http://localhost:8000/api/file-upload-analysis/" \
  -F "file=@data.csv"
```

### 3. File Summary Endpoint

**URL:** `GET /api/file-summary/{file_id}/`

**Description:** Retrieves detailed information about a specific uploaded file, including statistics and analysis results.

**Path Parameters:**
- `file_id`: UUID of the file to retrieve summary for

**Response:**
```json
{
  "file": {
    "id": "uuid",
    "file_name": "test_data.csv",
    "file_size": 1024,
    "total_records": 3,
    "processed_records": 3,
    "failed_records": 0,
    "status": "COMPLETED",
    "uploaded_at": "2024-01-15T10:00:00Z",
    "processed_at": "2024-01-15T10:05:00Z"
  },
  "statistics": {
    "total_transactions": 3,
    "total_amount": 245000.0,
    "unique_users": 2,
    "unique_accounts": 1,
    "unique_profit_centers": 2,
    "avg_amount": 81666.67,
    "high_value_transactions": 0
  },
  "analysis_sessions": [
    {
      "id": "uuid",
      "session_name": "Analysis for test_data.csv",
      "description": "Automated analysis for uploaded file test_data.csv",
      "status": "COMPLETED",
      "total_transactions": 3,
      "total_amount": "245000.00",
      "flagged_transactions": 0,
      "high_value_transactions": 0
    }
  ],
  "recent_transactions": [
    {
      "id": "uuid",
      "document_number": "1000000003",
      "document_type": "TR",
      "posting_date": "2024-01-17",
      "amount_local_currency": "120000.00",
      "local_currency": "SAR",
      "gl_account": "100000",
      "profit_center": "PC001",
      "user_name": "USER001",
      "fiscal_year": 2024,
      "posting_period": 1,
      "is_high_value": false,
      "is_cleared": false,
      "created_at": "2024-01-15T10:05:00Z"
    }
  ]
}
```

**Example Usage:**
```bash
curl -X GET "http://localhost:8000/api/file-summary/123e4567-e89b-12d3-a456-426614174000/"
```

## Error Handling

All endpoints return appropriate HTTP status codes:

- `200 OK`: Successful GET request
- `201 Created`: Successful file upload
- `400 Bad Request`: Invalid request data or file format
- `404 Not Found`: File not found (for summary endpoint)
- `500 Internal Server Error`: Server error during processing

Error responses include an `error` field with a descriptive message:

```json
{
  "error": "Only CSV files are supported"
}
```

## File Processing Status

Files go through the following statuses during processing:

1. **PENDING**: File uploaded, waiting to be processed
2. **PROCESSING**: File is currently being processed
3. **COMPLETED**: File processed successfully
4. **FAILED**: File processing failed
5. **PARTIAL**: File processed with some errors

## Analysis Features

The automatic analysis includes:

- **Amount Anomaly Detection**: Identifies unusual transaction amounts
- **Timing Anomaly Detection**: Detects unusual posting patterns
- **User Behavior Analysis**: Analyzes user activity patterns
- **Account Usage Analysis**: Monitors account usage patterns
- **Pattern Recognition**: Identifies suspicious transaction patterns
- **Risk Scoring**: Assigns risk scores to transactions (0-100)
- **Risk Level Classification**: Categorizes transactions as LOW, MEDIUM, HIGH, or CRITICAL risk

## Testing

To test the endpoints:

1. Start the Django development server:
   ```bash
   python manage.py runserver
   ```

2. Use the provided test script:
   ```bash
   python test_new_endpoints.py
   ```

3. Test with actual HTTP requests using curl or a tool like Postman.

## Implementation Details

The new endpoints are implemented using Django REST Framework:

- **FileListView**: `generics.ListAPIView` with filtering and aggregation
- **FileUploadAnalysisView**: `generics.CreateAPIView` with file processing and analysis
- **FileSummaryView**: `generics.RetrieveAPIView` with detailed statistics

All views include proper error handling, logging, and validation. The file processing is done synchronously for simplicity, but could be enhanced with background task processing for large files. 