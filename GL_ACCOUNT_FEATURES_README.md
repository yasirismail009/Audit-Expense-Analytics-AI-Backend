# GL Account Features - Enhanced SAP Analytics System

This document describes the enhanced GL Account functionality added to the SAP Analytics system, including GL Account ID tracking, categorization, credit/debit analysis, and Trial Balance generation.

## Overview

The enhanced system now includes comprehensive GL Account management with the following key features:

1. **GL Account Master Data Management**
2. **Credit/Debit Transaction Tracking**
3. **GL Account Analysis with Risk Scoring**
4. **Trial Balance Generation**
5. **GL Account Charts and Visualizations**
6. **Enhanced File Upload with Engagement Details**

## New Models

### GLAccount Model

```python
class GLAccount(models.Model):
    account_id = models.CharField(max_length=20, unique=True)  # GL Account ID
    account_name = models.CharField(max_length=255)            # Account Name
    account_type = models.CharField(max_length=50)             # Asset, Liability, Equity, Revenue, Expense
    account_category = models.CharField(max_length=100)        # Cash, Receivables, etc.
    account_subcategory = models.CharField(max_length=100)     # Subcategory
    normal_balance = models.CharField(max_length=10)           # DEBIT or CREDIT
    is_active = models.BooleanField(default=True)
    
    @property
    def current_balance(self):
        # Calculates current balance based on transactions
```

### Enhanced SAPGLPosting Model

```python
class SAPGLPosting(models.Model):
    # New fields added
    gl_account_ref = models.ForeignKey(GLAccount, ...)        # Reference to GL Account
    transaction_type = models.CharField(max_length=10)        # DEBIT or CREDIT
```

## API Endpoints

### GL Account Management

#### 1. List GL Accounts
```
GET /api/gl-accounts/
```

**Query Parameters:**
- `account_id`: Filter by account ID
- `account_name`: Filter by account name
- `account_type`: Filter by account type (Asset, Liability, etc.)
- `account_category`: Filter by account category
- `normal_balance`: Filter by normal balance (DEBIT/CREDIT)
- `is_active`: Filter by active status

**Response:**
```json
[
  {
    "id": "uuid",
    "account_id": "1000",
    "account_name": "Cash and Cash Equivalents",
    "account_type": "Asset",
    "account_category": "Current Assets",
    "normal_balance": "DEBIT",
    "is_active": true,
    "current_balance": "1000000.00",
    "transaction_count": 5
  }
]
```

#### 2. GL Account Analysis
```
GET /api/gl-accounts/analysis/
```

**Response:**
```json
[
  {
    "account_id": "1000",
    "account_name": "Cash and Cash Equivalents",
    "account_type": "Asset",
    "account_category": "Current Assets",
    "normal_balance": "DEBIT",
    "current_balance": 1000000.00,
    "total_debits": 1500000.00,
    "total_credits": 500000.00,
    "transaction_count": 5,
    "debit_count": 3,
    "credit_count": 2,
    "high_value_transactions": 1,
    "flagged_transactions": 0,
    "risk_score": 15.5,
    "first_transaction_date": "2025-01-15",
    "last_transaction_date": "2025-01-20",
    "avg_transaction_amount": 200000.00,
    "max_transaction_amount": 1000000.00
  }
]
```

#### 3. Trial Balance Generation
```
GET /api/gl-accounts/trial-balance/?date_from=2025-01-01&date_to=2025-01-31
```

**Response:**
```json
[
  {
    "account_id": "1000",
    "account_name": "Cash and Cash Equivalents",
    "account_type": "Asset",
    "account_category": "Current Assets",
    "normal_balance": "DEBIT",
    "opening_debit": 0.00,
    "opening_credit": 0.00,
    "period_debit": 1500000.00,
    "period_credit": 500000.00,
    "closing_debit": 1000000.00,
    "closing_credit": 0.00,
    "net_balance": 1000000.00,
    "transaction_count": 5
  }
]
```

#### 4. GL Account Charts
```
GET /api/gl-accounts/charts/
```

**Response:**
```json
{
  "account_type_distribution": [
    {
      "account_type": "Asset",
      "count": 2,
      "total_balance": 1500000.00,
      "total_transactions": 8
    }
  ],
  "account_category_distribution": [
    {
      "category": "Current Assets",
      "count": 2,
      "total_balance": 1500000.00
    }
  ],
  "top_accounts_by_balance": [
    {
      "account_id": "1000",
      "account_name": "Cash and Cash Equivalents",
      "balance": 1000000.00,
      "account_type": "Asset"
    }
  ],
  "top_accounts_by_transactions": [
    {
      "account_id": "1000",
      "account_name": "Cash and Cash Equivalents",
      "transaction_count": 5,
      "account_type": "Asset"
    }
  ],
  "debit_credit_analysis": {
    "total_debits": 2600000.00,
    "total_credits": 800000.00,
    "debit_count": 4,
    "credit_count": 2,
    "net_movement": 1800000.00
  },
  "risk_distribution": [
    {
      "account_type": "Asset",
      "account_count": 2,
      "high_risk_count": 0,
      "avg_risk_score": 12.5
    }
  ]
}
```

#### 5. Upload GL Account Master Data
```
POST /api/gl-accounts/upload-master-data/
```

**Request:** Multipart form data with CSV file

**CSV Format:**
```csv
Account ID,Account Name,Account Type,Account Category,Account Subcategory,Normal Balance,Is Active
1000,Cash and Cash Equivalents,Asset,Current Assets,Cash,DEBIT,TRUE
2000,Accounts Payable,Liability,Current Liabilities,Payables,CREDIT,TRUE
```

## Enhanced File Upload

The file upload now includes additional engagement details:

### Enhanced Upload Endpoint
```
POST /api/file-upload-analysis/
```

**Request:** Multipart form data
- `file`: CSV file with transactions
- `engagement_id`: Engagement ID for the audit
- `client_name`: Client name
- `company_name`: Company name
- `fiscal_year`: Fiscal year for the audit
- `audit_start_date`: Audit start date
- `audit_end_date`: Audit end date

### Enhanced CSV Format

The CSV file should now include a `Transaction Type` column:

```csv
Document Number,Posting Date,G/L Account,Amount in Local Currency,Transaction Type,Local Currency,Text,Document Date,User Name,Document Type,Fiscal Year,Posting Period
DOC001,2025-01-15,1000,1000000,DEBIT,SAR,Cash deposit,2025-01-15,USER001,DZ,2025,1
DOC002,2025-01-16,2000,500000,CREDIT,SAR,Purchase on credit,2025-01-16,USER002,TR,2025,1
```

## Features and Capabilities

### 1. GL Account Categorization

- **Account Types**: Asset, Liability, Equity, Revenue, Expense
- **Account Categories**: Current Assets, Fixed Assets, Current Liabilities, etc.
- **Account Subcategories**: Cash, Receivables, Payables, etc.
- **Normal Balance**: DEBIT or CREDIT side

### 2. Credit/Debit Tracking

- Automatic transaction type detection
- Support for explicit transaction type in CSV
- Balance calculation based on normal balance side
- Separate tracking of debit and credit amounts

### 3. Trial Balance Features

- **Opening Balances**: Support for opening balance import
- **Period Movements**: Debit and credit movements for the period
- **Closing Balances**: Calculated closing balances
- **Net Balance**: Net balance for each account
- **Date Range Filtering**: Generate TB for specific periods

### 4. GL Account Analysis

- **Balance Analysis**: Current balance calculation
- **Transaction Statistics**: Count and amount statistics
- **Risk Analysis**: Risk scoring based on transaction patterns
- **Activity Analysis**: First/last transaction dates, average amounts
- **High-Value Detection**: Identification of high-value transactions

### 5. Charts and Visualizations

- **Account Type Distribution**: Distribution by account type
- **Account Category Distribution**: Distribution by category
- **Top Accounts**: Top accounts by balance and transaction count
- **Debit vs Credit Analysis**: Overall debit/credit analysis
- **Risk Distribution**: Risk distribution by account type

### 6. Enhanced File Upload

- **Engagement Tracking**: Track engagement details
- **Client Information**: Store client and company information
- **Audit Period**: Define audit start and end dates
- **Fiscal Year**: Specify fiscal year for the audit

## Usage Examples

### 1. Upload GL Account Master Data

```bash
curl -X POST "http://localhost:8000/api/gl-accounts/upload-master-data/" \
  -F "file=@gl_accounts.csv"
```

### 2. Upload Transactions with Enhanced Data

```bash
curl -X POST "http://localhost:8000/api/file-upload-analysis/" \
  -F "file=@transactions.csv" \
  -F "engagement_id=ENG001" \
  -F "client_name=Test Client" \
  -F "company_name=Test Company" \
  -F "fiscal_year=2025" \
  -F "audit_start_date=2025-01-01" \
  -F "audit_end_date=2025-12-31"
```

### 3. Get GL Account Analysis

```bash
curl "http://localhost:8000/api/gl-accounts/analysis/"
```

### 4. Generate Trial Balance

```bash
curl "http://localhost:8000/api/gl-accounts/trial-balance/?date_from=2025-01-01&date_to=2025-01-31"
```

### 5. Get Charts Data

```bash
curl "http://localhost:8000/api/gl-accounts/charts/"
```

## Testing

Run the comprehensive test suite:

```bash
python test_gl_account_features.py
```

This will:
1. Create sample GL Account master data
2. Upload sample transactions with credit/debit information
3. Test all GL Account endpoints
4. Generate sample reports and charts

## Database Migration

After implementing these changes, run:

```bash
python manage.py makemigrations
python manage.py migrate
```

## Benefits

1. **Complete GL Account Tracking**: Full GL Account master data management
2. **Accurate Trial Balance**: Proper debit/credit tracking and balance calculation
3. **Enhanced Analysis**: Comprehensive GL Account analysis with risk scoring
4. **Better Visualization**: Rich charts and reports for GL Account data
5. **Audit Trail**: Complete engagement tracking and audit period management
6. **Standard Compliance**: Proper accounting standards compliance with debit/credit tracking

## Future Enhancements

1. **Opening Balance Import**: Support for importing opening balances
2. **Account Reconciliation**: Automated account reconciliation features
3. **Journal Entry Validation**: Validation of journal entry balancing
4. **Multi-Currency Support**: Enhanced multi-currency support
5. **Account Hierarchy**: Support for account hierarchies and roll-ups
6. **Advanced Reporting**: Additional financial reports (P&L, Balance Sheet) 