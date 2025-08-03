# New Analysis APIs Documentation

## Overview

This document describes the newly created APIs for Unusual Days Analysis and Closing Entries Analysis, following the same pattern as the existing UserAnalysisView.

## API Endpoints

### 1. Unusual Days Analysis API

**Endpoint:** `GET /api/core/unusual-days-analysis/{file_id}/`

**Description:** Retrieves comprehensive unusual days analysis results for a specific file, including weekend and unusual day postings, risk assessment, and visualization data.

**URL Parameters:**
- `file_id` (UUID): The ID of the data file to analyze

**Response Structure:**
```json
{
  "analysis_info": {
    "analysis_id": "uuid",
    "analysis_date": "2024-01-01T00:00:00Z",
    "processing_duration": 120.5,
    "status": "COMPLETED",
    "analysis_version": "1.0.0"
  },
  "summary": {
    "total_transactions": 1000,
    "weekend_postings": 25,
    "unusual_days_detected": 5,
    "risk_level": "MEDIUM",
    "overall_risk_score": 45.2
  },
  "unusual_days_analysis": {
    "weekend_postings": [...],
    "day_of_week_activity": {...},
    "unusual_days": [...],
    "user_day_patterns": [...],
    "fs_line_day_patterns": [...]
  },
  "risk_assessment": {
    "risk_assessment": {...},
    "weekend_risk_score": 35.0,
    "unusual_pattern_risk_score": 25.0,
    "high_value_weekend_risk_score": 60.0,
    "overall_risk_score": 45.2
  },
  "patterns": {
    "day_of_week_activity": {...},
    "user_day_patterns": [...],
    "fs_line_day_patterns": [...]
  },
  "visualizations": {
    "chart_data": {...},
    "chart_types": [
      "weekend_activity_chart",
      "holiday_activity_chart",
      "day_of_week_activity",
      "monthly_patterns",
      "risk_distribution"
    ]
  },
  "export_data": {
    "weekend_export": [...],
    "unusual_days_export": [...],
    "user_patterns_export": [...]
  }
}
```

**Error Responses:**
- `404`: No unusual days analysis results found for this file
- `500`: Internal server error

### 2. Closing Entries Analysis API

**Endpoint:** `GET /api/core/closing-entries-analysis/{file_id}/`

**Description:** Retrieves comprehensive closing entries analysis results for a specific file, including month-end closing transactions, post-close flag analysis, and risk assessment.

**URL Parameters:**
- `file_id` (UUID): The ID of the data file to analyze

**Response Structure:**
```json
{
  "analysis_info": {
    "analysis_id": "uuid",
    "analysis_date": "2024-01-01T00:00:00Z",
    "processing_duration": 180.3,
    "status": "COMPLETED",
    "analysis_version": "1.0.0"
  },
  "summary": {
    "total_transactions": 1000,
    "closing_entries_count": 50,
    "post_close_entries_count": 15,
    "risk_level": "HIGH",
    "overall_risk_score": 75.8
  },
  "closing_entries_analysis": {
    "closing_entries": [...],
    "post_close_analysis": {...},
    "fs_line_closing": {...},
    "user_closing": {...},
    "month_end_patterns": {...},
    "closing_window_analysis": {...}
  },
  "risk_assessment": {
    "closing_entries_risk": 45.0,
    "post_close_risk": 80.0,
    "high_value_post_close_risk": 90.0,
    "risk_assessment_details": {...}
  },
  "patterns": {
    "month_end_patterns": {...},
    "closing_window_analysis": {...},
    "fs_line_closing": {...},
    "user_closing": {...}
  },
  "visualizations": {
    "chart_data": {...},
    "chart_types": [
      "closing_entries_timeline",
      "post_close_activity",
      "fs_line_closing_distribution",
      "user_closing_activity",
      "month_end_patterns",
      "risk_distribution"
    ]
  },
  "export_data": {
    "closing_entries_export": [...],
    "post_close_export": [...],
    "fs_line_export": [...]
  }
}
```

**Error Responses:**
- `404`: No closing entries analysis results found for this file
- `500`: Internal server error

## Implementation Details

### Views Created

1. **UnusualDaysAnalysisView** (`core/views.py`)
   - Located at lines ~4500-4650
   - Follows the same pattern as UserAnalysisView
   - Provides comprehensive unusual days analysis results

2. **ClosingEntriesAnalysisView** (`core/views.py`)
   - Located at lines ~4650-4800
   - Follows the same pattern as UserAnalysisView
   - Provides comprehensive closing entries analysis results

### URL Configuration

Added to `core/urls.py`:
```python
# Unusual days analysis endpoint
path('unusual-days-analysis/<uuid:file_id>/', views.UnusualDaysAnalysisView.as_view(), name='unusual-days-analysis'),
# Closing entries analysis endpoint
path('closing-entries-analysis/<uuid:file_id>/', views.ClosingEntriesAnalysisView.as_view(), name='closing-entries-analysis'),
```

### Model Integration

Both APIs integrate with existing models:

1. **UnusualDaysAnalysisResult** model
   - Provides methods like `get_weekend_transactions_count()`, `get_unusual_days_count()`, etc.
   - Stores analysis results in JSON fields for flexibility

2. **ClosingEntriesAnalysisResult** model
   - Provides methods like `get_closing_entries_count()`, `get_post_close_entries_count()`, etc.
   - Stores analysis results in JSON fields for flexibility

## Usage Examples

### Python Requests Example

```python
import requests

# Unusual Days Analysis
response = requests.get('http://localhost:8000/api/core/unusual-days-analysis/your-file-id/')
if response.status_code == 200:
    data = response.json()
    print(f"Weekend postings: {data['summary']['weekend_postings']}")
    print(f"Risk level: {data['summary']['risk_level']}")

# Closing Entries Analysis
response = requests.get('http://localhost:8000/api/core/closing-entries-analysis/your-file-id/')
if response.status_code == 200:
    data = response.json()
    print(f"Closing entries: {data['summary']['closing_entries_count']}")
    print(f"Post-close entries: {data['summary']['post_close_entries_count']}")
```

### cURL Examples

```bash
# Unusual Days Analysis
curl -X GET "http://localhost:8000/api/core/unusual-days-analysis/your-file-id/"

# Closing Entries Analysis
curl -X GET "http://localhost:8000/api/core/closing-entries-analysis/your-file-id/"
```

## Testing

A test script `test_new_analysis_apis.py` has been created to verify the APIs work correctly. To use it:

1. Update the `FILE_ID` variable with an actual file ID from your database
2. Run the script: `python test_new_analysis_apis.py`

## Error Handling

Both APIs include comprehensive error handling:

- **404 Not Found**: When no analysis results exist for the specified file
- **500 Internal Server Error**: When unexpected errors occur during processing
- **Detailed error messages**: Include error codes and suggestions for resolution

## Data Structure

### Unusual Days Analysis Data

- **Weekend Postings**: Transactions posted on weekends (Friday/Saturday)
- **Unusual Days**: Patterns detected as unusual based on business rules
- **Day of Week Activity**: Activity patterns by day of the week
- **User Patterns**: User posting patterns by day of week
- **FS Line Patterns**: Financial statement line activity by day of week

### Closing Entries Analysis Data

- **Closing Entries**: Month-end closing transactions
- **Post-Close Analysis**: Transactions with post-close flags
- **FS Line Closing**: Closing entries by financial statement line
- **User Closing**: Closing entries by user
- **Month-End Patterns**: Month-end activity patterns
- **Closing Window Analysis**: Analysis of closing windows

## Risk Assessment

Both APIs provide comprehensive risk assessment:

- **Risk Scores**: Numerical risk scores (0-100)
- **Risk Levels**: Categorical risk levels (LOW, MEDIUM, HIGH, CRITICAL)
- **Risk Factors**: Detailed breakdown of risk factors
- **Recommendations**: Risk-based recommendations and actions

## Export Data

Both APIs provide export-ready data in multiple formats:

- **Structured Data**: Ready for Excel/CSV export
- **Chart Data**: Ready for visualization libraries
- **Summary Data**: High-level summaries for reporting

## Performance Considerations

- **Database Queries**: Optimized to use existing model methods
- **JSON Storage**: Flexible JSON fields for complex data structures
- **Caching**: Results are stored in database for quick retrieval
- **Error Logging**: Comprehensive logging for debugging

## Future Enhancements

Potential improvements for future versions:

1. **Filtering**: Add query parameters for filtering results
2. **Pagination**: Add pagination for large datasets
3. **Real-time Updates**: WebSocket support for real-time analysis updates
4. **Custom Thresholds**: Allow custom risk thresholds
5. **Export Formats**: Support for additional export formats (PDF, XML)

## Integration with Existing System

These new APIs integrate seamlessly with the existing analytics system:

- **Same URL Pattern**: Follows existing API conventions
- **Same Response Structure**: Consistent with other analysis APIs
- **Same Error Handling**: Consistent error responses
- **Same Authentication**: Uses existing authentication mechanisms
- **Same Logging**: Integrates with existing logging system 