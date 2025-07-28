# Enhanced General Analysis and Anomaly Tracking System

## Overview

The system has been enhanced to provide comprehensive GL account analysis with detailed debit/credit/balance calculations and automatic anomaly tracking that updates individual SAPGLPosting records with risk scores and anomaly types detected.

## 1. Enhanced General Analysis

### **Detailed GL Account Analysis**

The general analysis now performs comprehensive analysis of each GL account with the following calculations:

#### **Trial Balance Summary**
```python
trial_balance_summary = {
    'total_debits': float(total_debits),
    'total_credits': float(total_credits),
    'balance': trial_balance,
    'is_balanced': abs(trial_balance) < 0.01,  # Consider balanced if difference is less than 0.01
    'balance_percentage': (abs(trial_balance) / total_amount * 100) if total_amount > 0 else 0
}
```

#### **Per-Account Analysis**
For each GL account, the system calculates:
- **Debit Amount**: Total debits posted to the account
- **Credit Amount**: Total credits posted to the account
- **Balance**: Net balance (debits - credits)
- **Transaction Counts**: Separate counts for debits, credits, and total transactions
- **Transaction Details**: Complete list of transactions with metadata

```python
gl_account_analysis = {
    'account': '1000000',  # GL Account number
    'debit_amount': 1500000.00,
    'credit_amount': 1200000.00,
    'balance': 300000.00,  # Net balance
    'debit_count': 45,
    'credit_count': 32,
    'total_count': 77,
    'transactions': [
        {
            'id': 'uuid',
            'document_number': 'DOC001',
            'posting_date': '2024-01-15',
            'amount': 50000.00,
            'transaction_type': 'DEBIT',
            'user_name': 'USER001',
            'text': 'Transaction description'
        }
        # ... more transactions
    ]
}
```

#### **User Analysis**
For each user, the system tracks:
- **Debit/Credit Breakdown**: Separate amounts and counts
- **Balance Calculation**: Net position per user
- **Account Usage**: Which accounts the user posted to
- **Transaction History**: Complete transaction list

```python
user_analysis = {
    'user': 'USER001',
    'debit_amount': 2500000.00,
    'credit_amount': 1800000.00,
    'balance': 700000.00,
    'debit_count': 78,
    'credit_count': 45,
    'total_count': 123,
    'accounts_used': ['1000000', '2000000', '3000000'],
    'unique_accounts_count': 3,
    'transactions': [
        # ... transaction details
    ]
}
```

### **Enhanced Statistical Calculations**

#### **Account Statistics**
```python
account_statistics = {
    'accounts_with_debits_only': 15,      # Accounts that only have debit entries
    'accounts_with_credits_only': 8,      # Accounts that only have credit entries
    'accounts_with_both': 25,             # Accounts with both debit and credit entries
    'highest_balance_account': '1000000', # Account with highest net balance
    'lowest_balance_account': '5000000'   # Account with lowest net balance
}
```

#### **User Statistics**
```python
user_statistics = {
    'users_with_debits_only': 12,         # Users who only posted debits
    'users_with_credits_only': 5,         # Users who only posted credits
    'users_with_both': 18,                # Users with both debit and credit entries
    'most_active_user': 'USER001',        # User with most transactions
    'highest_amount_user': 'USER002'      # User with highest total amount
}
```

### **Enhanced Chart Data**
```python
chart_data = {
    'account_balances': {                 # Top 20 accounts by balance
        '1000000': 300000.00,
        '2000000': -150000.00,
        # ... more accounts
    },
    'user_activity': {                    # Top 20 users by transaction count
        'USER001': 123,
        'USER002': 98,
        # ... more users
    },
    'debit_credit_distribution': {        # Overall debit/credit distribution
        'debits': 5000000.00,
        'credits': 4800000.00
    },
    'account_transaction_counts': {       # Transaction counts per account
        '1000000': 77,
        '2000000': 45,
        # ... more accounts
    }
}
```

## 2. Anomaly Tracking System

### **SAPGLPosting Model Enhancements**

The SAPGLPosting model has been enhanced with new fields to track anomalies:

#### **Duplicate Analysis Tracking**
```python
# New fields added to SAPGLPosting model
is_duplicate = models.BooleanField(default=False)
duplicate_type = models.CharField(max_length=20, blank=True, null=True)  # type_1, type_2, etc.
duplicate_risk_score = models.FloatField(default=0.0)
duplicate_analysis_details = models.JSONField(default=dict)
```

#### **Backdated Analysis Tracking**
```python
is_backdated = models.BooleanField(default=False)
backdated_days = models.IntegerField(default=0)
backdated_risk_score = models.FloatField(default=0.0)
backdated_analysis_details = models.JSONField(default=dict)
```

#### **Overall Anomaly Tracking**
```python
overall_risk_score = models.FloatField(default=0.0)
anomaly_types = models.JSONField(default=list)  # ['duplicate', 'backdated']
anomaly_analysis_summary = models.JSONField(default=dict)
```

### **Automatic Transaction Updates**

#### **Duplicate Analysis Updates**
When duplicate analysis runs, it automatically updates SAPGLPosting records:

```python
def _update_transactions_with_duplicate_analysis(self, transactions: List, duplicates: List) -> None:
    """
    Updates SAPGLPosting records with duplicate analysis information
    """
    for duplicate in duplicates:
        t1_id = duplicate['transaction1']['id']
        t2_id = duplicate['transaction2']['id']
        
        # Update both transactions in the duplicate pair
        for t_id in [t1_id, t2_id]:
            transaction = SAPGLPosting.objects.get(id=t_id)
            transaction.is_duplicate = True
            transaction.duplicate_type = duplicate['duplicate_type']
            transaction.duplicate_risk_score = duplicate['risk_score']
            transaction.duplicate_analysis_details = {
                'duplicate_type': duplicate['duplicate_type'],
                'duplicate_type_name': duplicate['duplicate_type_name'],
                'risk_score': duplicate['risk_score'],
                'similarity_score': duplicate['similarity_score'],
                'matching_fields': duplicate['matching_fields'],
                'paired_with': t2_id if t_id == t1_id else t1_id
            }
            transaction.anomaly_types = list(set(transaction.anomaly_types + ['duplicate']))
            transaction.save()
```

#### **Backdated Analysis Updates**
When backdated analysis runs, it updates SAPGLPosting records:

```python
def _update_transactions_with_backdated_analysis(self, transactions: List, backdated_entries: List) -> None:
    """
    Updates SAPGLPosting records with backdated analysis information
    """
    for entry in backdated_entries:
        transaction = SAPGLPosting.objects.get(id=entry['transaction_id'])
        transaction.is_backdated = True
        transaction.backdated_days = entry['days_difference']
        transaction.backdated_risk_score = entry['risk_score']
        transaction.backdated_analysis_details = {
            'days_difference': entry['days_difference'],
            'risk_score': entry['risk_score'],
            'risk_level': entry['risk_level'],
            'document_date': entry['document_date'],
            'posting_date': entry['posting_date']
        }
        transaction.anomaly_types = list(set(transaction.anomaly_types + ['backdated']))
        transaction.save()
```

#### **Overall Analysis Updates**
When overall analysis runs, it consolidates all anomaly information:

```python
def _update_transactions_with_overall_analysis(self, transactions: List, overall_results: List) -> None:
    """
    Updates SAPGLPosting records with overall analysis information
    """
    for result in overall_results:
        transaction = SAPGLPosting.objects.get(id=result['transaction_id'])
        transaction.overall_risk_score = result['overall_risk_score']
        
        # Create comprehensive anomaly summary
        anomaly_summary = {
            'overall_risk_score': result['overall_risk_score'],
            'risk_level': result['risk_level'],
            'recommendations': result.get('recommendations', []),
            'risk_factors': result.get('risk_factors', {}),
            'duplicate_info': {
                'is_duplicate': transaction.is_duplicate,
                'duplicate_type': transaction.duplicate_type,
                'duplicate_risk_score': transaction.duplicate_risk_score
            },
            'backdated_info': {
                'is_backdated': transaction.is_backdated,
                'backdated_days': transaction.backdated_days,
                'backdated_risk_score': transaction.backdated_risk_score
            }
        }
        transaction.anomaly_analysis_summary = anomaly_summary
        transaction.save()
```

## 3. Analysis Flow Integration

### **Complete Analysis Pipeline**

1. **General Analysis**: Performs detailed GL account analysis with debit/credit/balance calculations
2. **Duplicate Analysis**: Identifies duplicates using business rules and updates transaction records
3. **Backdated Analysis**: Identifies backdated entries and updates transaction records
4. **Overall Analysis**: Combines all analyses and calculates overall risk scores
5. **Risk Analysis**: Final risk assessment and classification

### **Data Flow Example**

```python
# 1. Run general analysis
general_results = orchestrator.run_general_analysis(transactions)
# Updates: trial_balance_summary, gl_account_summaries, user_summaries

# 2. Run duplicate analysis
duplicate_results = orchestrator.run_duplicate_analysis(transactions)
# Updates: SAPGLPosting.is_duplicate, duplicate_type, duplicate_risk_score

# 3. Run backdated analysis
backdated_results = orchestrator.run_backdated_analysis(transactions)
# Updates: SAPGLPosting.is_backdated, backdated_days, backdated_risk_score

# 4. Run overall analysis
overall_results = orchestrator.run_overall_analysis(transactions)
# Updates: SAPGLPosting.overall_risk_score, anomaly_analysis_summary
```

## 4. Querying Enhanced Data

### **Query Transactions by Anomaly Type**

```python
# Get all duplicate transactions
duplicate_transactions = SAPGLPosting.objects.filter(is_duplicate=True)

# Get all backdated transactions
backdated_transactions = SAPGLPosting.objects.filter(is_backdated=True)

# Get high-risk transactions
high_risk_transactions = SAPGLPosting.objects.filter(overall_risk_score__gte=70)

# Get transactions with multiple anomalies
multi_anomaly_transactions = SAPGLPosting.objects.filter(
    anomaly_types__contains=['duplicate', 'backdated']
)
```

### **Query by Risk Level**

```python
# Get critical risk transactions
critical_transactions = SAPGLPosting.objects.filter(
    overall_risk_score__gte=90
)

# Get transactions by duplicate type
type_6_duplicates = SAPGLPosting.objects.filter(
    duplicate_type='type_6'
)

# Get backdated transactions by days difference
severe_backdated = SAPGLPosting.objects.filter(
    backdated_days__gt=30
)
```

### **Query Account Analysis**

```python
# Get account with highest balance
highest_balance_account = SAPGLPosting.objects.values('gl_account').annotate(
    debit_total=Sum('amount_local_currency', filter=Q(transaction_type='DEBIT')),
    credit_total=Sum('amount_local_currency', filter=Q(transaction_type='CREDIT')),
    balance=F('debit_total') - F('credit_total')
).order_by('-balance').first()

# Get user activity summary
user_activity = SAPGLPosting.objects.values('user_name').annotate(
    transaction_count=Count('id'),
    total_amount=Sum('amount_local_currency'),
    unique_accounts=Count('gl_account', distinct=True)
).order_by('-transaction_count')
```

## 5. Benefits of Enhanced System

### **1. Comprehensive GL Account Analysis**
- **Detailed Balance Calculation**: Each account shows debits, credits, and net balance
- **Transaction History**: Complete list of transactions per account
- **User Activity Tracking**: Which users posted to which accounts
- **Statistical Insights**: Account and user behavior patterns

### **2. Real-time Anomaly Tracking**
- **Immediate Updates**: Transaction records updated as analysis runs
- **Risk Score Persistence**: Risk scores stored at transaction level
- **Anomaly Type Classification**: Clear identification of anomaly types
- **Comprehensive Summary**: All anomaly information in one place

### **3. Enhanced Reporting Capabilities**
- **Account-level Reports**: Detailed analysis per GL account
- **User-level Reports**: User activity and risk assessment
- **Anomaly Reports**: Filtered views by anomaly type and risk level
- **Trial Balance Reports**: Complete trial balance with balance verification

### **4. Audit Trail Compliance**
- **Complete History**: All analysis results stored in database
- **Risk Score Tracking**: Historical risk scores for trend analysis
- **Anomaly Documentation**: Detailed anomaly information for audit purposes
- **Data Integrity**: Atomic updates ensure data consistency

## 6. Database Schema Summary

### **Enhanced SAPGLPosting Fields**

| Field | Type | Description |
|-------|------|-------------|
| `is_duplicate` | Boolean | Flag for duplicate transactions |
| `duplicate_type` | CharField | Type of duplicate (type_1, type_2, etc.) |
| `duplicate_risk_score` | FloatField | Risk score for duplicate detection |
| `duplicate_analysis_details` | JSONField | Detailed duplicate analysis results |
| `is_backdated` | Boolean | Flag for backdated transactions |
| `backdated_days` | IntegerField | Days between document and posting date |
| `backdated_risk_score` | FloatField | Risk score for backdated detection |
| `backdated_analysis_details` | JSONField | Detailed backdated analysis results |
| `overall_risk_score` | FloatField | Overall risk score combining all analyses |
| `anomaly_types` | JSONField | List of anomaly types detected |
| `anomaly_analysis_summary` | JSONField | Comprehensive anomaly summary |

### **Enhanced Analysis Results**

The system now provides:
- **Detailed GL Account Analysis**: Debit/credit/balance calculations per account
- **User Activity Analysis**: User-level transaction and account usage
- **Trial Balance Verification**: Complete trial balance with balance checks
- **Statistical Insights**: Account and user behavior patterns
- **Real-time Anomaly Tracking**: Automatic updates to transaction records
- **Comprehensive Risk Assessment**: Multi-level risk scoring and classification

This enhanced system provides a complete audit trail and comprehensive analysis capabilities for financial data analysis and risk assessment. 