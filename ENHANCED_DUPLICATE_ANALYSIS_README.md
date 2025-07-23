# Enhanced Duplicate Analysis System

## Overview

This enhanced duplicate analysis system identifies Journal Lines with identical characteristics and provides comprehensive breakdowns, drilldown capabilities, and export functionality for integration with the Spark Selections Workbook.

## Duplicate Classification Types

The system categorizes duplicates into 6 specific types based on identical characteristics:

### Type 1 Duplicate - Account Number + Amount
- **Criteria**: Identical GL Account Number and Amount
- **Risk Level**: Low to Medium
- **Use Case**: Basic duplicate detection for same account and amount combinations

### Type 2 Duplicate - Account Number + Source + Amount
- **Criteria**: Identical GL Account Number + Document Type/Source + Amount
- **Risk Level**: Medium
- **Use Case**: Detects duplicates within the same source system

### Type 3 Duplicate - Account Number + User + Amount
- **Criteria**: Identical GL Account Number + User Name + Amount
- **Risk Level**: Medium to High
- **Use Case**: Identifies potential user-specific duplicate patterns

### Type 4 Duplicate - Account Number + Posted Date + Amount
- **Criteria**: Identical GL Account Number + Posting Date + Amount
- **Risk Level**: High
- **Use Case**: Detects duplicates posted on the same date

### Type 5 Duplicate - Account Number + Effective Date + Amount
- **Criteria**: Identical GL Account Number + Document Date + Amount
- **Risk Level**: High
- **Use Case**: Identifies duplicates with same effective date

### Type 6 Duplicate - Account Number + Effective Date + Posted Date + User + Source + Amount
- **Criteria**: Identical GL Account Number + Document Date + Posting Date + User + Document Type + Amount
- **Risk Level**: Critical
- **Use Case**: Most comprehensive duplicate detection with all key fields

## API Endpoints

### 1. Get Duplicate Analysis
```
GET /api/duplicate-anomalies/?sheet_id=<sheet_id>
```

**Parameters:**
- `sheet_id` (required): Identifier for the data sheet/analysis
- `date_from`: Start date filter (YYYY-MM-DD)
- `date_to`: End date filter (YYYY-MM-DD)
- `min_amount`: Minimum amount filter
- `max_amount`: Maximum amount filter
- `gl_accounts`: List of GL accounts to filter
- `users`: List of users to filter
- `document_types`: List of document types to filter
- `duplicate_threshold`: Minimum count for duplicate detection (default: 2)
- `duplicate_types`: List of duplicate types to include

### 2. Export to CSV
```
GET /api/duplicate-anomalies/export_csv/?sheet_id=<sheet_id>
```

**Parameters:** Same as above
**Output:** CSV file ready for import into Spark Selections Workbook

## Output Structure

### 1. Breakdown of Duplicate Flags
```json
{
  "type_breakdown": {
    "Type 1 Duplicate": {
      "count": 15,
      "transactions": 45,
      "amount": 125000.00,
      "debit_count": 25,
      "credit_count": 20,
      "debit_amount": 75000.00,
      "credit_amount": 50000.00
    }
  }
}
```

### 2. Debit, Credit Amounts and Journal Line Count per Duplicate and Month
```json
{
  "monthly_breakdown": {
    "2024-01": {
      "duplicate_groups": 8,
      "transactions": 24,
      "amount": 45000.00,
      "debit_count": 15,
      "credit_count": 9,
      "debit_amount": 30000.00,
      "credit_amount": 15000.00
    }
  }
}
```

### 3. Breakdown of Duplicates per Impacted User
```json
{
  "user_breakdown": {
    "USER001": {
      "duplicate_groups": 5,
      "transactions": 18,
      "amount": 35000.00,
      "debit_count": 12,
      "credit_count": 6,
      "debit_amount": 25000.00,
      "credit_amount": 10000.00
    }
  }
}
```

### 4. Breakdown of Duplicates per Impacted FS Line
```json
{
  "fs_line_breakdown": {
    "1001000": {
      "duplicate_groups": 12,
      "transactions": 36,
      "amount": 85000.00,
      "debit_count": 22,
      "credit_count": 14,
      "debit_amount": 55000.00,
      "credit_amount": 30000.00
    }
  }
}
```

### 5. Final Selection Drilldown
```json
{
  "drilldown_data": [
    {
      "duplicate_type": "Type 1 Duplicate",
      "duplicate_criteria": "Account Number + Amount",
      "gl_account": "1001000",
      "amount": 5000.00,
      "duplicate_count": 3,
      "risk_score": 30,
      "transaction_id": "uuid-1",
      "document_number": "DOC001",
      "posting_date": "2024-01-15",
      "document_date": "2024-01-15",
      "user_name": "USER001",
      "document_type": "INVOICE",
      "transaction_type": "DEBIT",
      "text": "Sample transaction text",
      "fiscal_year": "2024",
      "posting_period": "01",
      "profit_center": "PC001",
      "cost_center": "CC001",
      "local_currency": "SAR",
      "debit_count": 2,
      "credit_count": 1,
      "debit_amount": 3500.00,
      "credit_amount": 1500.00
    }
  ]
}
```

## CSV Export Format

The CSV export includes all fields from the drilldown data with the following columns:

1. **Duplicate_Type** - Type of duplicate (Type 1-6)
2. **Duplicate_Criteria** - Description of duplicate criteria
3. **GL_Account** - General Ledger Account Number
4. **Amount** - Transaction amount
5. **Duplicate_Count** - Number of duplicate transactions in group
6. **Risk_Score** - Calculated risk score (0-100)
7. **Transaction_ID** - Unique transaction identifier
8. **Document_Number** - Document reference number
9. **Posting_Date** - Date transaction was posted
10. **Document_Date** - Effective date of document
11. **User_Name** - User who created the transaction
12. **Document_Type** - Type of document (INVOICE, PAYMENT, etc.)
13. **Transaction_Type** - DEBIT or CREDIT
14. **Text** - Transaction description
15. **Fiscal_Year** - Fiscal year
16. **Posting_Period** - Posting period
17. **Profit_Center** - Profit center code
18. **Cost_Center** - Cost center code
19. **Local_Currency** - Currency code
20. **Debit_Count** - Number of debit transactions in group
21. **Credit_Count** - Number of credit transactions in group
22. **Debit_Amount** - Total debit amount in group
23. **Credit_Amount** - Total credit amount in group

## Risk Scoring

Each duplicate type has a different risk multiplier:

- **Type 1**: 10 points per duplicate
- **Type 2**: 12 points per duplicate
- **Type 3**: 15 points per duplicate
- **Type 4**: 18 points per duplicate
- **Type 5**: 20 points per duplicate
- **Type 6**: 25 points per duplicate

Maximum risk score is capped at 100.

## Usage Examples

### 1. Get All Duplicates for a Sheet
```bash
curl "http://localhost:8000/api/duplicate-anomalies/?sheet_id=12345"
```

### 2. Get Specific Duplicate Types
```bash
curl "http://localhost:8000/api/duplicate-anomalies/?sheet_id=12345&duplicate_types=Type%201%20Duplicate&duplicate_types=Type%202%20Duplicate"
```

### 3. Export to CSV
```bash
curl "http://localhost:8000/api/duplicate-anomalies/export_csv/?sheet_id=12345" -o duplicate_analysis.csv
```

### 4. Filter by Date Range and Amount
```bash
curl "http://localhost:8000/api/duplicate-anomalies/?sheet_id=12345&date_from=2024-01-01&date_to=2024-01-31&min_amount=1000&max_amount=50000"
```

## Integration with Spark Selections Workbook

1. **Export Data**: Use the CSV export endpoint to get the final selection data
2. **Import to Spark**: Import the CSV file into the Spark Selections Workbook
3. **Further Filtering**: Use the slicer selections in Spark for additional filtering
4. **Analysis**: Perform detailed analysis on the selected duplicate transactions

## Configuration

### Duplicate Threshold
Set the minimum number of transactions required to be considered a duplicate:
```python
analyzer.analysis_config['duplicate_threshold'] = 2  # Default value
```

### Risk Scoring
Adjust risk multipliers for different duplicate types:
```python
duplicate_types = [
    {'type': 'Type 1 Duplicate', 'risk_multiplier': 10},
    {'type': 'Type 2 Duplicate', 'risk_multiplier': 12},
    # ... etc
]
```

## Performance Considerations

- Large datasets are processed efficiently using pandas DataFrames
- Date grouping is optimized for performance
- Export functionality handles large result sets
- Memory usage is optimized for large transaction volumes

## Error Handling

- Invalid sheet_id returns 400 error
- Missing transactions returns empty results
- Processing errors return 500 error with details
- CSV export handles encoding issues automatically

## Future Enhancements

1. **Machine Learning Integration**: Use ML models to predict duplicate likelihood
2. **Real-time Monitoring**: Continuous duplicate detection for new transactions
3. **Advanced Filtering**: More sophisticated filtering options
4. **Visualization**: Built-in charts and graphs for duplicate analysis
5. **Automated Alerts**: Email notifications for high-risk duplicates 