# Duplicate Analysis with Business Rules Implementation

## Overview

The duplicate analysis system has been updated to use specific business rules instead of generic ML-based detection. The system now implements a hierarchical classification approach that checks for duplicates from the most specific criteria (Type 6) down to the least specific (Type 1).

## Business Rules Classification

### Duplicate Types (Ordered by Specificity)

1. **Type 6 - Most Specific**: Account Number + Effective Date + Posted Date + User + Source + Amount
   - Risk Score: 95
   - Fields: `['gl_account', 'document_date', 'posting_date', 'user_name', 'source', 'amount_local_currency']`

2. **Type 5**: Account Number + Effective Date + Amount
   - Risk Score: 90
   - Fields: `['gl_account', 'document_date', 'amount_local_currency']`

3. **Type 4**: Account Number + Posted Date + Amount
   - Risk Score: 85
   - Fields: `['gl_account', 'posting_date', 'amount_local_currency']`

4. **Type 3**: Account Number + User + Amount
   - Risk Score: 80
   - Fields: `['gl_account', 'user_name', 'amount_local_currency']`

5. **Type 2**: Account Number + Source + Amount
   - Risk Score: 75
   - Fields: `['gl_account', 'source', 'amount_local_currency']`

6. **Type 1 - Least Specific**: Account Number + Amount
   - Risk Score: 70
   - Fields: `['gl_account', 'amount_local_currency']`

## Implementation Details

### Priority Logic

The system implements a **hierarchical classification approach**:

1. **Check Most Specific First**: The system starts with Type 6 and works down to Type 1
2. **Single Classification**: Once a transaction pair is classified under a specific type, it is excluded from consideration for other types
3. **No Cross-Classification**: A transaction cannot appear in multiple duplicate types

### Key Features

#### 1. Priority-Based Detection
```python
# Duplicate types ordered from most specific to least specific
duplicate_types = {
    'type_6': { 'fields': ['gl_account', 'document_date', 'posting_date', 'user_name', 'source', 'amount_local_currency'], 'risk_score': 95 },
    'type_5': { 'fields': ['gl_account', 'document_date', 'amount_local_currency'], 'risk_score': 90 },
    # ... continues down to type_1
}
```

#### 2. Transaction Pair Tracking
```python
# Track which transaction pairs have already been classified
classified_pairs = set()

for i, t1 in enumerate(transactions):
    for j, t2 in enumerate(transactions[i+1:], i+1):
        pair_id = tuple(sorted([str(t1.id), str(t2.id)]))
        
        # Skip if this pair has already been classified
        if pair_id in classified_pairs:
            continue
        
        # Check each duplicate type from most specific to least specific
        for dup_type, config in duplicate_types.items():
            if self._check_duplicate_criteria(t1, t2, config['fields']):
                classified_pairs.add(pair_id)  # Mark as classified
                break
```

#### 3. Risk-Based Scoring
Each duplicate type has an associated risk score:
- **High Risk (85-95)**: Types 4, 5, 6
- **Medium Risk (75-84)**: Types 2, 3
- **Low Risk (70-74)**: Type 1

### Field Matching Logic

The system includes intelligent field matching:

```python
def _check_duplicate_criteria(self, t1, t2, fields: List[str]) -> bool:
    for field in fields:
        if field == 'amount_local_currency':
            # Allow small rounding differences
            if abs(t1.amount_local_currency - t2.amount_local_currency) > 0.01:
                return False
        elif field == 'posting_date':
            if t1.posting_date != t2.posting_date:
                return False
        elif field == 'document_date':
            if t1.document_date != t2.document_date:
                return False
        # ... other field comparisons
    return True
```

## Analysis Results Structure

### Output Format
```json
{
    "duplicates_found": 4,
    "duplicate_pairs": [...],
    "duplicates_by_type": {
        "type_6": { "count": 1, "risk_score": 95, "duplicates": [...] },
        "type_5": { "count": 1, "risk_score": 90, "duplicates": [...] },
        // ...
    },
    "duplicate_type_summary": {
        "type_1_count": 0,
        "type_2_count": 1,
        "type_3_count": 0,
        "type_4_count": 1,
        "type_5_count": 1,
        "type_6_count": 1
    },
    "audit_recommendations": {
        "high_risk_duplicates": 3,
        "medium_risk_duplicates": 1,
        "low_risk_duplicates": 0
    },
    "compliance_assessment": {
        "total_duplicates": 4,
        "duplicate_percentage": 50.0,
        "duplicate_types_detected": 4
    }
}
```

### Individual Duplicate Record
```json
{
    "transaction1": {
        "id": "uuid",
        "document_number": "DOC001",
        "amount": 1000.0,
        "account": "1000",
        "date": "2024-01-15",
        "user": "User1",
        "source": "",
        "effective_date": "2024-01-15"
    },
    "transaction2": { ... },
    "duplicate_type": "type_6",
    "duplicate_type_name": "Account Number + Effective Date + Posted Date + User + Source + Amount",
    "similarity_score": 0.79,
    "risk_score": 95,
    "matching_fields": ["gl_account", "document_date", "posting_date", "user_name", "source", "amount_local_currency"]
}
```

## Integration with Overall Analysis

The duplicate analysis results are integrated into the overall risk assessment:

```python
# In overall analysis, duplicate risk is scaled based on duplicate type
duplicate_entry = next((d for d in duplicate_results if d.get('transaction1', {}).get('id') == str(t.id)), None)
if duplicate_entry:
    risk_score += duplicate_entry.get('risk_score', 25.0) / 4.0  # Scale down the risk score
```

## Benefits of This Approach

1. **Business Rule Compliance**: Follows specific audit requirements for duplicate classification
2. **Hierarchical Classification**: Ensures most specific matches are identified first
3. **No Duplicate Classification**: Prevents transactions from appearing in multiple types
4. **Risk-Based Scoring**: Different duplicate types have appropriate risk scores
5. **Comprehensive Reporting**: Detailed breakdown by duplicate type and risk level
6. **Audit Trail**: Clear documentation of which fields matched for each duplicate

## Testing Results

The implementation has been tested with various scenarios:

- ✅ **Priority Logic**: Type 6 checked first, then Type 5, Type 4, etc.
- ✅ **Single Classification**: No transaction appears in multiple duplicate types
- ✅ **Correct Risk Scoring**: Higher risk scores for more specific duplicate types
- ✅ **Field Matching**: Accurate detection of matching criteria
- ✅ **Integration**: Proper integration with overall analysis system

## Usage

The duplicate analysis is automatically run as part of the comprehensive analysis:

```python
orchestrator = MLAnalysisOrchestrator()
results = orchestrator.run_duplicate_analysis(transactions)
```

Or as part of the full analysis suite:

```python
comprehensive_results = orchestrator.run_comprehensive_analysis(transactions)
duplicate_results = comprehensive_results['duplicate_analysis']
``` 