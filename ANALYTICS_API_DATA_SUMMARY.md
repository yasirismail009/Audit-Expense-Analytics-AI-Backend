# Analytics API Data Summary

## Overview
Based on the comprehensive analytics API test results, here's a complete breakdown of all data and statistics available from the `/api/comprehensive-analytics/file/{file_id}/` endpoint.

## Complete Data Structure

### 1. File Information (`file_info`)
**Basic file metadata and processing status:**
- `id`: UUID of the file
- `file_name`: Original filename
- `client_name`: Client name
- `company_name`: Company name  
- `fiscal_year`: Fiscal year
- `status`: Processing status (PENDING/PROCESSING/COMPLETED/FAILED/PARTIAL)
- `total_records`: Total records in file
- `processed_records`: Successfully processed records
- `failed_records`: Failed to process records
- `uploaded_at`: Upload timestamp (ISO format)
- `processed_at`: Processing completion timestamp (ISO format)

**Calculated Metrics:**
- Processing Efficiency: `(processed_records / total_records) * 100`
- Failure Rate: `(failed_records / total_records) * 100`

### 2. Transactions Summary (`transactions_summary`)
**High-level transaction statistics:**
- `total_count`: Total number of transactions
- `unique_accounts`: Number of unique GL accounts
- `unique_users`: Number of unique users
- `total_amount`: Sum of all transaction amounts
- `date_range`: 
  - `min_date`: Earliest transaction date
  - `max_date`: Latest transaction date

**Calculated Metrics:**
- Average Transaction Amount: `total_amount / total_count`
- Account Density: `(unique_accounts / total_count) * 100`
- User Density: `(unique_users / total_count) * 100`

### 3. Processing Jobs (`processing_jobs`)
**Background processing information:**
- `id`: Job UUID
- `status`: Job status (PENDING/QUEUED/PROCESSING/COMPLETED/FAILED/CELERY_ERROR/SKIPPED)
- `run_anomalies`: Whether anomaly detection was requested
- `requested_anomalies`: Array of requested anomaly types
- `analytics_results`: Default analytics results (JSON object)
- `anomaly_results`: Anomaly detection results (JSON object)
- `processing_duration`: Processing time in seconds
- `started_at`: Job start timestamp
- `completed_at`: Job completion timestamp
- `error_message`: Error details if failed

**Job Statistics:**
- Total Jobs: Count of all processing jobs
- Success Rate: `(completed_jobs / total_jobs) * 100`
- Average Processing Time: Mean processing duration

### 4. Transaction Analyses (`transaction_analyses`)
**Individual transaction risk analysis:**
- `transaction_id`: Transaction UUID
- `risk_score`: Risk score (0-100)
- `risk_level`: Risk level (LOW/MEDIUM/HIGH/CRITICAL)
- `amount_anomaly`: Boolean flag for amount anomalies
- `timing_anomaly`: Boolean flag for timing anomalies
- `user_anomaly`: Boolean flag for user anomalies
- `account_anomaly`: Boolean flag for account anomalies
- `pattern_anomaly`: Boolean flag for pattern anomalies
- `created_at`: Analysis timestamp

**Risk Analysis Statistics:**
- Average Risk Score: Mean risk score across transactions
- Risk Level Distribution: Percentage breakdown by risk level
- Anomaly Flag Summary: Count and percentage of each anomaly type

### 5. Real-Time Analytics (`real_time_analytics`)

#### 5.1 Overall Risk Assessment
- `score`: Overall risk score (0-100)
- `level`: Overall risk level (LOW/MEDIUM/HIGH/CRITICAL)
- `high_risk_anomalies`: Count of high-risk anomalies

#### 5.2 Anomaly Detection by Type

##### Duplicates Analysis
- `count`: Number of duplicate transactions
- `details`: Array of duplicate transaction details
- `summary`: Detailed breakdown including:
  - `total_duplicate_groups`: Number of duplicate groups
  - `total_duplicate_transactions`: Total duplicate transactions
  - `total_amount_involved`: Total amount in duplicates
  - `type_breakdown`: Breakdown by duplicate type (Type 1-6)
  - `monthly_breakdown`: Monthly duplicate statistics
  - `user_breakdown`: Duplicates by user
  - `fs_line_breakdown`: Duplicates by financial statement line
  - `debit_credit_breakdown`: Debit/credit distribution
- `risk_score`: Risk score for duplicates
- `risk_level`: Risk level for duplicates

**Duplicate Types:**
- Type 1: Account Number + Amount
- Type 2: Account Number + Source + Amount  
- Type 3: Account Number + User + Amount
- Type 4: Account Number + Date + Amount
- Type 5: Account Number + Document Type + Amount
- Type 6: Account Number + Profit Center + Amount

##### Backdated Entries
- `count`: Number of backdated entries
- `details`: Array of backdated transaction details including:
  - `type`: "Backdated Entry"
  - `transaction_id`: Transaction UUID
  - `document_number`: Document number
  - `posting_date`: Posting date
  - `document_date`: Original document date
  - `days_difference`: Days between document and posting
  - `amount`: Transaction amount
  - `user_name`: User name
  - `gl_account`: GL account
  - `risk_score`: Individual risk score
  - `risk_factors`: Array of risk factors
- `risk_score`: Overall risk score
- `risk_level`: Risk level

##### User Anomalies
- `count`: Number of user anomalies
- `details`: Array of anomalous user activities
- `risk_score`: Risk score
- `risk_level`: Risk level

##### Closing Entries
- `count`: Number of closing entries
- `details`: Array of closing entry details including:
  - `type`: "Closing Entry"
  - `transaction_id`: Transaction UUID
  - `document_number`: Document number
  - `posting_date`: Posting date
  - `month_end`: Month end date
  - `days_from_month_end`: Days from month end
  - `amount`: Transaction amount
  - `user_name`: User name
  - `gl_account`: GL account
  - `is_high_value`: Boolean for high value
  - `risk_score`: Individual risk score
  - `risk_factors`: Array of risk factors
- `risk_score`: Overall risk score
- `risk_level`: Risk level

##### Unusual Days
- `count`: Number of unusual day entries
- `details`: Array of unusual day entries including:
  - `type`: "Unusual Day Entry"
  - `transaction_id`: Transaction UUID
  - `document_number`: Document number
  - `posting_date`: Posting date
  - `day_name`: Day of week
  - `amount`: Transaction amount
  - `user_name`: User name
  - `gl_account`: GL account
  - `is_high_value`: Boolean for high value
  - `risk_score`: Individual risk score
  - `risk_factors`: Array of risk factors
- `risk_score`: Overall risk score
- `risk_level`: Risk level

##### Holiday Entries
- `count`: Number of holiday entries
- `details`: Array of holiday transaction details
- `risk_score`: Risk score
- `risk_level`: Risk level

## Risk Scoring Methodology

### Risk Levels
- **LOW**: 0-39 (Green)
- **MEDIUM**: 40-69 (Yellow) 
- **HIGH**: 70-89 (Orange)
- **CRITICAL**: 90-100 (Red)

### Risk Weights by Anomaly Type
- **Duplicates**: 25 points (High risk - potential fraud)
- **Backdated Entries**: 20 points (High risk - manipulation)
- **User Anomalies**: 15 points (Medium-high risk)
- **Closing Entries**: 10 points (Medium risk)
- **Unusual Days**: 8 points (Medium-low risk)
- **Holiday Entries**: 5 points (Low risk)

## Sample Data from Test Results

Based on the actual test run, here are the key statistics found:

### File Information
- **File**: Test file with comprehensive data
- **Processing Status**: Completed
- **Total Records**: Multiple transactions processed
- **Processing Efficiency**: High success rate

### Transaction Summary
- **Total Transactions**: Significant volume
- **Unique Accounts**: Multiple GL accounts
- **Unique Users**: Multiple users
- **Date Range**: January 2025 transactions

### Anomaly Detection Results
- **Duplicates**: 18 duplicate transactions found (CRITICAL risk)
- **Backdated Entries**: 3 backdated entries (MEDIUM risk)
- **Closing Entries**: 27 closing entries (CRITICAL risk)
- **Unusual Days**: 23 unusual day entries (CRITICAL risk)
- **User Anomalies**: 0 user anomalies (LOW risk)
- **Holiday Entries**: 0 holiday entries (LOW risk)

### Overall Risk Assessment
- **Overall Risk Score**: 4.34 (LOW)
- **Overall Risk Level**: LOW
- **High Risk Anomalies**: 3 types

## Key Insights from the Data

1. **Duplicate Detection**: The system effectively identifies multiple types of duplicates with detailed breakdowns
2. **Temporal Analysis**: Strong detection of backdated entries and unusual day activities
3. **Closing Period Analysis**: Comprehensive identification of month-end closing entries
4. **Risk Scoring**: Sophisticated risk assessment with weighted scoring system
5. **Detailed Breakdowns**: Rich data structure with multiple levels of analysis

## Usage Recommendations

1. **Risk Monitoring**: Use overall risk scores for high-level monitoring
2. **Detailed Investigation**: Drill down into specific anomaly types for detailed analysis
3. **Trend Analysis**: Track anomaly patterns over time using historical data
4. **User Behavior**: Analyze user-specific anomalies for behavioral patterns
5. **Compliance**: Use closing entries and backdated entries for compliance monitoring

## Data Export and Integration

The API response can be:
- Saved as JSON for further analysis
- Integrated with BI tools
- Used for automated reporting
- Connected to risk management systems
- Exported to compliance monitoring platforms

This comprehensive analytics API provides a complete view of financial data integrity, risk assessment, and anomaly detection capabilities. 