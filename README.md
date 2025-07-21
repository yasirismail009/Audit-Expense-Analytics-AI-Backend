# SAP GL Posting Analysis System

A comprehensive Django-based system for analyzing SAP General Ledger (GL) posting data for fraud detection and anomaly identification.

## Features

- **Data Import**: Upload and process SAP GL posting CSV files
- **Anomaly Detection**: Identify unusual patterns in financial transactions
- **Risk Scoring**: Calculate risk scores for individual transactions
- **User Behavior Analysis**: Monitor user activity patterns
- **Account Analysis**: Track G/L account usage patterns
- **Dashboard**: Comprehensive overview of system metrics
- **API Access**: RESTful API for integration with other systems

## System Architecture

### Models

1. **SAPGLPosting**: Main model for SAP GL posting data
2. **DataFile**: Track uploaded data files and processing status
3. **AnalysisSession**: Manage analysis sessions and parameters
4. **TransactionAnalysis**: Store analysis results for individual transactions
5. **SystemMetrics**: Track system performance and usage metrics

### Key Components

- **SAPGLAnalyzer**: Core analytics engine for anomaly detection
- **REST API**: Complete API for data access and analysis
- **Management Commands**: CLI tools for data processing

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd analytics
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Run migrations**:
   ```bash
   python manage.py makemigrations
   python manage.py migrate
   ```

5. **Create superuser** (optional):
   ```bash
   python manage.py createsuperuser
   ```

6. **Run the server**:
   ```bash
   python manage.py runserver
   ```

## Usage

### 1. Data Import

#### Via Management Command
```bash
python manage.py process_sap_data path/to/your/sap_data.csv
```

#### Via API
```bash
curl -X POST -F "file=@sap_data.csv" http://localhost:8000/api/files/upload/
```

### 2. Data Analysis

#### Create Analysis Session
```bash
curl -X POST http://localhost:8000/api/sessions/ \
  -H "Content-Type: application/json" \
  -d '{
    "session_name": "January 2025 Analysis",
    "description": "Analysis of January 2025 transactions",
    "date_from": "2025-01-01",
    "date_to": "2025-01-31"
  }'
```

#### Run Analysis
```bash
curl -X POST http://localhost:8000/api/sessions/{session_id}/run/
```

#### Get Analysis Summary
```bash
curl http://localhost:8000/api/sessions/{session_id}/summary/
```

### 3. Data Querying

#### Get All Transactions
```bash
curl http://localhost:8000/api/postings/
```

#### Filter Transactions
```bash
curl "http://localhost:8000/api/postings/?date_from=2025-01-01&date_to=2025-01-31&min_amount=1000000"
```

#### Get Statistics
```bash
curl http://localhost:8000/api/postings/statistics/
```

#### Get Top Users
```bash
curl http://localhost:8000/api/postings/top-users/
```

### 4. Dashboard

```bash
curl http://localhost:8000/api/dashboard/
```

## API Endpoints

### Data Management
- `GET /api/postings/` - List all transactions
- `GET /api/postings/{id}/` - Get specific transaction
- `POST /api/files/upload/` - Upload CSV file
- `GET /api/files/` - List uploaded files

### Analysis
- `GET /api/sessions/` - List analysis sessions
- `POST /api/sessions/` - Create analysis session
- `POST /api/sessions/{id}/run/` - Run analysis
- `GET /api/sessions/{id}/summary/` - Get analysis summary
- `GET /api/analyses/` - List transaction analyses

### Statistics
- `GET /api/postings/statistics/` - Get transaction statistics
- `GET /api/postings/top-users/` - Get top users
- `GET /api/postings/top-accounts/` - Get top accounts
- `GET /api/dashboard/` - Get dashboard statistics

## CSV File Format

The system expects CSV files with the following columns (column names are flexible):

### Required Columns
- `Document Number` - SAP Document Number
- `Posting Date` - Posting date
- `G/L Account` - General Ledger account
- `Amount in Local Currency` - Transaction amount
- `Local Currency` - Currency code (default: SAR)
- `Text` - Transaction text
- `Document Date` - Document date
- `Offsetting Account` - Offsetting account
- `User Name` - User who posted the transaction
- `Entry Date` - Entry date

### Optional Columns
- `Document type` - Document type (DZ, SA, TR, AB, etc.)
- `Profit Center` - Profit center code
- `Fiscal Year` - Fiscal year
- `Posting period` - Posting period
- `Segment` - Segment code
- `Clearing Document` - Clearing document number
- `Invoice Reference` - Invoice reference
- `Sales Document` - Sales document
- `Assignment` - Assignment field
- `Year/Month` - Year/Month format
- `Cost Center` - Cost center code
- `WBS Element` - Work breakdown structure element
- `Plant` - Plant code
- `Material` - Material number
- `Billing Document` - Billing document
- `Purchasing Document` - Purchasing document
- `Order` - Order number
- `Asset` - Asset number
- `Network` - Network number
- `Tax Code` - Tax code
- `Account Assignment` - Account assignment

## Anomaly Detection

The system detects several types of anomalies:

### 1. Amount Anomalies
- Statistical outliers using Z-score and IQR methods
- High-value transactions (> 1M SAR)
- Unusual amount distributions

### 2. Timing Anomalies
- Rapid successive transactions by the same user
- Unusual posting times (weekends, holidays, etc.)
- Irregular posting patterns

### 3. User Behavior Anomalies
- Unusual user activity levels
- Users posting to unusual accounts
- Users with high transaction volumes

### 4. Account Usage Anomalies
- Unusual G/L account usage patterns
- Accounts with high transaction frequencies
- Unusual account combinations

### 5. Pattern Anomalies
- Machine learning-based pattern detection
- Isolation Forest for outlier detection
- Unusual transaction patterns

## Risk Scoring

Each transaction receives a risk score (0-100) based on:

- **Base Score**: Transaction properties (high value, Arabic text, etc.)
- **Anomaly Scores**: Detected anomalies and their severity
- **User Risk**: User behavior patterns
- **Account Risk**: Account usage patterns

Risk Levels:
- **LOW** (0-29): Normal transactions
- **MEDIUM** (30-59): Some concerns
- **HIGH** (60-79): Significant risk
- **CRITICAL** (80-100): High risk

## Configuration

### Analysis Configuration
The system can be configured by modifying the `analysis_config` in `SAPGLAnalyzer`:

```python
self.analysis_config = {
    'amount_threshold': 1000000,  # 1M SAR
    'timing_window_hours': 24,
    'user_activity_threshold': 100,
    'account_usage_threshold': 50,
    'pattern_similarity_threshold': 0.8
}
```

### Database Configuration
Configure your database in `settings.py`:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}
```

## Development

### Running Tests
```bash
python manage.py test
```

### Creating Migrations
```bash
python manage.py makemigrations
```

### Applying Migrations
```bash
python manage.py migrate
```

### Shell Access
```bash
python manage.py shell
```

## Performance Considerations

- **Batch Processing**: Large files are processed in batches
- **Database Indexing**: Optimized indexes for common queries
- **Caching**: Consider implementing Redis for caching
- **Background Tasks**: Use Celery for large file processing

## Security

- **Input Validation**: All inputs are validated
- **SQL Injection Protection**: Django ORM provides protection
- **File Upload Security**: Only CSV files are accepted
- **API Authentication**: Implement JWT authentication for production

## Troubleshooting

### Common Issues

1. **CSV Import Errors**
   - Check CSV format and column names
   - Ensure proper encoding (UTF-8)
   - Verify date formats

2. **Analysis Failures**
   - Check data quality
   - Verify analysis parameters
   - Review error logs

3. **Performance Issues**
   - Optimize database queries
   - Use batch processing for large files
   - Consider database indexing

### Logs
Check the Django logs for detailed error information:
```bash
tail -f debug.log
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 