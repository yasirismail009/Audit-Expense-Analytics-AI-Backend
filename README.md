# Expense Fraud Analytics System

A comprehensive Django-based system for detecting fraudulent expenses using local AI/ML models.

## Features

- **CSV Upload & Processing**: Upload expense data via CSV files
- **Local AI Analytics**: Multiple ML models running locally for privacy
- **Fraud Detection**: Isolation Forest, XGBoost, Local Outlier Factor, Random Forest
- **Detailed Reports**: Individual expense analysis with risk scores
- **Session Tracking**: Track analysis sessions and results
- **REST API**: Complete API for data access and analysis retrieval

## ML Models Used

1. **Isolation Forest**: Anomaly detection on expense patterns
2. **XGBoost**: Multi-feature fraud prediction
3. **Local Outlier Factor**: Behavioral profiling
4. **Random Forest**: Interpretable fraud detection
5. **DBSCAN**: Duplicate/cluster detection

## Setup Instructions

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Migrations**
   ```bash
   python manage.py migrate
   ```

3. **Create Superuser (Optional)**
   ```bash
   python manage.py createsuperuser
   ```

4. **Run Development Server**
   ```bash
   python manage.py runserver
   ```

## Usage

### 1. Upload and Analyze CSV
```bash
python manage.py analyze_expenses path/to/your/expenses.csv
```

### 2. API Endpoints

- `GET /` - Home page
- `POST /expenses/upload/` - Upload CSV file
- `GET /expenses/` - List all expenses
- `GET /expenses/{expense_id}/analysis/` - Get fraud analysis for specific expense
- `GET /analysis/session/{session_id}/` - Get analysis session summary
- `GET /analysis/session/{session_id}/expenses/` - Get all expenses from a specific expense sheet
- `GET /test-db/` - Test database connection

### 3. Example API Usage

**Get fraud analysis for an expense:**
```bash
curl http://localhost:8000/expenses/EXP001/analysis/
```

**Get analysis session summary:**
```bash
curl http://localhost:8000/analysis/session/{session_id}/
```

**Get all expenses from a specific expense sheet:**
```bash
curl http://localhost:8000/analysis/session/{session_id}/expenses/
```

## Analysis Results

Each expense analysis includes:
- **Fraud Score**: Overall risk score (0-100)
- **Risk Level**: LOW, MEDIUM, HIGH, CRITICAL
- **Model Scores**: Individual scores from each ML model
- **Anomaly Flags**: Specific types of detected anomalies
- **Detailed Analysis**: JSON with model outputs and explanations
- **Expense Sheet Info**: Session ID, file name, and upload date

## Data Privacy

- All ML processing happens locally
- No data leaves your system
- Models train on your own expense data
- Results stored in local database

## CSV Format

Expected columns:
- date, expense_id, category, subcategory, description
- employee, department, amount, currency
- payment_method, vendor_supplier, receipt_number
- status, approved_by, notes

## Project Structure

- `analytics/` - Main Django project settings
- `core/` - Main application with models, views, and analytics
- `core/analytics.py` - ML models and fraud detection logic
- `core/management/commands/analyze_expenses.py` - CSV processing command
- `db.sqlite3` - SQLite database file (created after migrations)

## Database Configuration

The project is configured to use SQLite as the default database. The database file will be created automatically when you run migrations. 