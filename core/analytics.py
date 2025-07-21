import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
from django.db.models import Q, Count, Sum, Avg, Min, Max
from django.utils import timezone
import logging
from calendar import monthrange
import holidays

from .models import SAPGLPosting, AnalysisSession, TransactionAnalysis, SystemMetrics

logger = logging.getLogger(__name__)

class SAPGLAnalyzer:
    """Main analyzer for SAP GL posting data with specific anomaly tests"""
    
    def __init__(self):
        self.analysis_config = {
            'duplicate_threshold': 2,  # Minimum count for duplicate detection
            'closing_days_before': 3,  # Days before month end for closing entries
            'closing_days_after': 2,   # Days after month end for closing entries
            'unusual_days': ['Saturday', 'Sunday'],  # Days considered unusual
            'holiday_dates': [],  # Predefined holiday dates
            'high_risk_users': [],  # Users of interest
            'high_risk_accounts': []  # Accounts of interest
        }
    
    def _serialize_transaction_data(self, df_group):
        """Helper function to properly serialize transaction data for JSON"""
        serialized_records = []
        for _, row in df_group.iterrows():
            gl_account = row['gl_account']
            if gl_account == 'UNKNOWN':
                gl_account = 'MISSING'  # Show 'MISSING' instead of 'UNKNOWN' in results
            
            record = {
                'id': str(row['id']),
                'gl_account': gl_account,
                'amount': float(row['amount']),
                'user_name': row['user_name'],
                'posting_date': row['posting_date'].isoformat() if pd.notna(row['posting_date']) else None,
                'document_date': row['document_date'].isoformat() if pd.notna(row['document_date']) else None,
                'document_number': row['document_number'],
                'document_type': row['document_type'],
                'source': row['source']
            }
            serialized_records.append(record)
        return serialized_records
    
    def detect_duplicate_entries(self, transactions):
        """Detect duplicate entries based on different criteria"""
        if not transactions:
            return []
        
        duplicates = []
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame([{
            'id': str(t.id),
            'gl_account': t.gl_account or 'UNKNOWN',  # Use 'UNKNOWN' for empty GL accounts
            'amount': float(t.amount_local_currency),
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'document_date': t.document_date,
            'document_number': t.document_number,
            'document_type': t.document_type,
            'source': t.document_type  # Using document_type as source
        } for t in transactions])
        
        # Type 1: Account Number + Amount
        type1_duplicates = df.groupby(['gl_account', 'amount']).filter(lambda x: len(x) >= self.analysis_config['duplicate_threshold'])
        for _, group in type1_duplicates.groupby(['gl_account', 'amount']):
            if len(group) >= self.analysis_config['duplicate_threshold']:
                gl_account_display = group.iloc[0]['gl_account']
                if gl_account_display == 'UNKNOWN':
                    gl_account_display = 'MISSING'  # Show 'MISSING' instead of 'UNKNOWN' in results
                
                duplicates.append({
                    'type': 'Type 1 Duplicate',
                    'criteria': 'Account Number + Amount',
                    'gl_account': gl_account_display,
                    'amount': group.iloc[0]['amount'],
                    'count': len(group),
                    'transactions': self._serialize_transaction_data(group),
                    'risk_score': min(len(group) * 10, 100)
                })
        
        # Type 2: Account Number + Source + Amount
        type2_duplicates = df.groupby(['gl_account', 'source', 'amount']).filter(lambda x: len(x) >= self.analysis_config['duplicate_threshold'])
        for _, group in type2_duplicates.groupby(['gl_account', 'source', 'amount']):
            if len(group) >= self.analysis_config['duplicate_threshold']:
                gl_account_display = group.iloc[0]['gl_account']
                if gl_account_display == 'UNKNOWN':
                    gl_account_display = 'MISSING'
                
                duplicates.append({
                    'type': 'Type 2 Duplicate',
                    'criteria': 'Account Number + Source + Amount',
                    'gl_account': gl_account_display,
                    'source': group.iloc[0]['source'],
                    'amount': group.iloc[0]['amount'],
                    'count': len(group),
                    'transactions': self._serialize_transaction_data(group),
                    'risk_score': min(len(group) * 12, 100)
                })
        
        # Type 3: Account Number + User + Amount
        type3_duplicates = df.groupby(['gl_account', 'user_name', 'amount']).filter(lambda x: len(x) >= self.analysis_config['duplicate_threshold'])
        for _, group in type3_duplicates.groupby(['gl_account', 'user_name', 'amount']):
            if len(group) >= self.analysis_config['duplicate_threshold']:
                gl_account_display = group.iloc[0]['gl_account']
                if gl_account_display == 'UNKNOWN':
                    gl_account_display = 'MISSING'
                
                duplicates.append({
                    'type': 'Type 3 Duplicate',
                    'criteria': 'Account Number + User + Amount',
                    'gl_account': gl_account_display,
                    'user_name': group.iloc[0]['user_name'],
                    'amount': group.iloc[0]['amount'],
                    'count': len(group),
                    'transactions': self._serialize_transaction_data(group),
                    'risk_score': min(len(group) * 15, 100)
                })
        
        # Type 4: Account Number + Posted Date + Amount
        type4_duplicates = df.groupby(['gl_account', 'posting_date', 'amount']).filter(lambda x: len(x) >= self.analysis_config['duplicate_threshold'])
        for _, group in type4_duplicates.groupby(['gl_account', 'posting_date', 'amount']):
            if len(group) >= self.analysis_config['duplicate_threshold']:
                gl_account_display = group.iloc[0]['gl_account']
                if gl_account_display == 'UNKNOWN':
                    gl_account_display = 'MISSING'
                
                duplicates.append({
                    'type': 'Type 4 Duplicate',
                    'criteria': 'Account Number + Posted Date + Amount',
                    'gl_account': gl_account_display,
                    'posting_date': group.iloc[0]['posting_date'].isoformat() if pd.notna(group.iloc[0]['posting_date']) else None,
                    'amount': group.iloc[0]['amount'],
                    'count': len(group),
                    'transactions': self._serialize_transaction_data(group),
                    'risk_score': min(len(group) * 18, 100)
                })
        
        # Type 5: Account Number + Effective Date + Amount
        type5_duplicates = df.groupby(['gl_account', 'document_date', 'amount']).filter(lambda x: len(x) >= self.analysis_config['duplicate_threshold'])
        for _, group in type5_duplicates.groupby(['gl_account', 'document_date', 'amount']):
            if len(group) >= self.analysis_config['duplicate_threshold']:
                gl_account_display = group.iloc[0]['gl_account']
                if gl_account_display == 'UNKNOWN':
                    gl_account_display = 'MISSING'
                
                duplicates.append({
                    'type': 'Type 5 Duplicate',
                    'criteria': 'Account Number + Effective Date + Amount',
                    'gl_account': gl_account_display,
                    'document_date': group.iloc[0]['document_date'].isoformat() if pd.notna(group.iloc[0]['document_date']) else None,
                    'amount': group.iloc[0]['amount'],
                    'count': len(group),
                    'transactions': self._serialize_transaction_data(group),
                    'risk_score': min(len(group) * 20, 100)
                })
        
        # Type 6: Account Number + Effective Date + Posted Date + User + Source + Amount
        type6_duplicates = df.groupby(['gl_account', 'document_date', 'posting_date', 'user_name', 'source', 'amount']).filter(lambda x: len(x) >= self.analysis_config['duplicate_threshold'])
        for _, group in type6_duplicates.groupby(['gl_account', 'document_date', 'posting_date', 'user_name', 'source', 'amount']):
            if len(group) >= self.analysis_config['duplicate_threshold']:
                gl_account_display = group.iloc[0]['gl_account']
                if gl_account_display == 'UNKNOWN':
                    gl_account_display = 'MISSING'
                
                duplicates.append({
                    'type': 'Type 6 Duplicate',
                    'criteria': 'Account Number + Effective Date + Posted Date + User + Source + Amount',
                    'gl_account': gl_account_display,
                    'document_date': group.iloc[0]['document_date'].isoformat() if pd.notna(group.iloc[0]['document_date']) else None,
                    'posting_date': group.iloc[0]['posting_date'].isoformat() if pd.notna(group.iloc[0]['posting_date']) else None,
                    'user_name': group.iloc[0]['user_name'],
                    'source': group.iloc[0]['source'],
                    'amount': group.iloc[0]['amount'],
                    'count': len(group),
                    'transactions': self._serialize_transaction_data(group),
                    'risk_score': min(len(group) * 25, 100)
                        })
        
        return duplicates
    
    def detect_user_anomalies(self, transactions):
        """Detect user-related anomalies"""
        if not transactions:
            return []
        
        user_anomalies = []
        
        # Analyze user activity patterns
        user_stats = {}
        for t in transactions:
            if t.user_name not in user_stats:
                user_stats[t.user_name] = {
                    'count': 0,
                    'total_amount': 0,
                    'accounts': set(),
                    'document_types': set(),
                    'dates': set(),
                    'high_value_count': 0
                }
            
            user_stats[t.user_name]['count'] += 1
            user_stats[t.user_name]['total_amount'] += float(t.amount_local_currency)
            user_stats[t.user_name]['accounts'].add(t.gl_account)
            user_stats[t.user_name]['document_types'].add(t.document_type)
            user_stats[t.user_name]['dates'].add(t.posting_date)
            
            if t.is_high_value:
                user_stats[t.user_name]['high_value_count'] += 1
        
        # Check for users of interest
        for user, stats in user_stats.items():
            risk_score = 0
            risk_factors = []
            
            # High activity user
            if stats['count'] > 100:
                risk_score += 20
                risk_factors.append('High transaction volume')
            
            # High value transactions
            if stats['high_value_count'] > 10:
                risk_score += 25
                risk_factors.append('Multiple high-value transactions')
            
            # Multiple accounts
            if len(stats['accounts']) > 20:
                risk_score += 15
                risk_factors.append('Multiple account usage')
            
            # Multiple document types
            if len(stats['document_types']) > 10:
                risk_score += 10
                risk_factors.append('Multiple document types')
            
            # User of interest
            if user in self.analysis_config['high_risk_users']:
                risk_score += 30
                risk_factors.append('User of interest')
            
            if risk_score > 0:
                user_anomalies.append({
                    'type': 'User Anomaly',
                    'user_name': user,
                    'risk_score': min(risk_score, 100),
                    'risk_factors': risk_factors,
                    'statistics': {
                        'transaction_count': stats['count'],
                        'total_amount': stats['total_amount'],
                        'unique_accounts': len(stats['accounts']),
                        'unique_document_types': len(stats['document_types']),
                        'high_value_count': stats['high_value_count']
                    }
                })
        
        return user_anomalies
    
    def detect_backdated_entries(self, transactions):
        """Detect backdated entries (posting date after document date)"""
        if not transactions:
            return []
        
        backdated = []
        
        for t in transactions:
            if t.posting_date and t.document_date:
                if t.posting_date > t.document_date:
                    days_diff = (t.posting_date - t.document_date).days
                    risk_score = min(days_diff * 2, 100)
                    
                    backdated.append({
                        'type': 'Backdated Entry',
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'posting_date': t.posting_date.isoformat(),
                        'document_date': t.document_date.isoformat(),
                        'days_difference': days_diff,
                        'amount': float(t.amount_local_currency),
                        'user_name': t.user_name,
                        'gl_account': t.gl_account,
                        'risk_score': risk_score,
                        'risk_factors': [f'Posted {days_diff} days after document date']
                    })
        
        return backdated
    
    def detect_closing_entries(self, transactions):
        """Detect month-end closing entries"""
        if not transactions:
            return []
        
        closing_entries = []
        
        for t in transactions:
            if t.posting_date:
                # Get last day of the month
                last_day = monthrange(t.posting_date.year, t.posting_date.month)[1]
                month_end = datetime(t.posting_date.year, t.posting_date.month, last_day).date()
            
                # Check if posting is within closing period
                days_before = self.analysis_config['closing_days_before']
                days_after = self.analysis_config['closing_days_after']
                
                closing_start = month_end - timedelta(days=days_before)
                closing_end = month_end + timedelta(days=days_after)
                
                if closing_start <= t.posting_date <= closing_end:
                    risk_score = 50  # Base score for closing entries
                    
                    # Higher risk for high-value transactions during closing
                    if t.is_high_value:
                        risk_score += 30
                    
                    closing_entries.append({
                        'type': 'Closing Entry',
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'posting_date': t.posting_date.isoformat(),
                        'month_end': month_end.isoformat(),
                        'days_from_month_end': (t.posting_date - month_end).days,
                        'amount': float(t.amount_local_currency),
                        'user_name': t.user_name,
                        'gl_account': t.gl_account,
                        'is_high_value': t.is_high_value,
                        'risk_score': risk_score,
                        'risk_factors': ['Month-end closing period', 'High value' if t.is_high_value else 'Normal value']
                    })
            
        return closing_entries
    
    def detect_unusual_days(self, transactions):
        """Detect transactions posted on unusual days (weekends)"""
        if not transactions:
            return []
    
        unusual_day_entries = []
        
        for t in transactions:
            if t.posting_date:
                day_name = t.posting_date.strftime('%A')
                
                if day_name in self.analysis_config['unusual_days']:
                    risk_score = 40  # Base score for weekend posting
                    
                    # Higher risk for high-value transactions on weekends
                    if t.is_high_value:
                        risk_score += 25
                    
                    unusual_day_entries.append({
                        'type': 'Unusual Day Entry',
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'posting_date': t.posting_date.isoformat(),
                        'day_name': day_name,
                        'amount': float(t.amount_local_currency),
                        'user_name': t.user_name,
                        'gl_account': t.gl_account,
                        'is_high_value': t.is_high_value,
                        'risk_score': risk_score,
                        'risk_factors': [f'Posted on {day_name}', 'High value' if t.is_high_value else 'Normal value']
                    })
        
        return unusual_day_entries
    
    def detect_holiday_entries(self, transactions):
        """Detect transactions posted on holidays"""
        if not transactions:
            return []
        
        holiday_entries = []
        
        # Create holiday calendar (you can customize this)
        sa_holidays = holidays.SA()
        
        for t in transactions:
            if t.posting_date:
                # Check if it's a predefined holiday
                is_predefined_holiday = t.posting_date in self.analysis_config['holiday_dates']
                is_sa_holiday = t.posting_date in sa_holidays
                
                if is_predefined_holiday or is_sa_holiday:
                    holiday_name = sa_holidays.get(t.posting_date, 'Predefined Holiday') if is_sa_holiday else 'Predefined Holiday'
                    risk_score = 60  # Base score for holiday posting
                    
                    # Higher risk for high-value transactions on holidays
                    if t.is_high_value:
                        risk_score += 30
                    
                    holiday_entries.append({
                        'type': 'Holiday Entry',
                        'transaction_id': str(t.id),
                        'document_number': t.document_number,
                        'posting_date': t.posting_date.isoformat(),
                        'holiday_name': holiday_name,
                        'amount': float(t.amount_local_currency),
                        'user_name': t.user_name,
                        'gl_account': t.gl_account,
                        'is_high_value': t.is_high_value,
                        'risk_score': risk_score,
                        'risk_factors': [f'Posted on {holiday_name}', 'High value' if t.is_high_value else 'Normal value']
                    })
        
        return holiday_entries
    
    def analyze_transactions(self, session):
        """Main analysis method with specific anomaly tests"""
        try:
            # Update session status
            session.status = 'RUNNING'
            session.started_at = timezone.now()
            session.save()
            
            # Build query based on session parameters
            query = SAPGLPosting.objects.all()  # type: ignore
            
            if session.date_from:
                query = query.filter(posting_date__gte=session.date_from)
            if session.date_to:
                query = query.filter(posting_date__lte=session.date_to)
            if session.min_amount:
                query = query.filter(amount_local_currency__gte=session.min_amount)
            if session.max_amount:
                query = query.filter(amount_local_currency__lte=session.max_amount)
            if session.document_types:
                query = query.filter(document_type__in=session.document_types)
            if session.gl_accounts:
                query = query.filter(gl_account__in=session.gl_accounts)
            if session.profit_centers:
                query = query.filter(profit_center__in=session.profit_centers)
            if session.users:
                query = query.filter(user_name__in=session.users)
            
            # Get transactions
            transactions = list(query)
            
            if not transactions:
                session.status = 'COMPLETED'
                session.completed_at = timezone.now()
                session.save()
                return {'error': 'No transactions found matching criteria'}
            
            # Run all anomaly tests
            duplicate_anomalies = self.detect_duplicate_entries(transactions)
            user_anomalies = self.detect_user_anomalies(transactions)
            backdated_anomalies = self.detect_backdated_entries(transactions)
            closing_anomalies = self.detect_closing_entries(transactions)
            unusual_day_anomalies = self.detect_unusual_days(transactions)
            holiday_anomalies = self.detect_holiday_entries(transactions)
            
            all_anomalies = {
                'duplicates': duplicate_anomalies,
                'user_anomalies': user_anomalies,
                'backdated': backdated_anomalies,
                'closing_entries': closing_anomalies,
                'unusual_days': unusual_day_anomalies,
                'holidays': holiday_anomalies
            }
            
            # Clear existing analyses for this session to avoid duplicates
            TransactionAnalysis.objects.filter(session=session).delete()  # type: ignore
            
            # Create analysis records for each transaction
            for transaction in transactions:
                # Remove any existing analysis for this transaction and session
                TransactionAnalysis.objects.filter(transaction=transaction, session=session).delete()  # type: ignore
                
                # Get transaction anomalies
                transaction_anomalies = self.get_transaction_anomalies(transaction, all_anomalies)
                
                # Calculate risk score based on anomalies
                risk_score = self.calculate_risk_score(transaction, all_anomalies)
                risk_level = self.determine_risk_level(risk_score)
                
                # Determine which anomaly flags to set
                amount_anomaly = (
                    any(anomaly['type'] in ['Duplicate Entry'] for anomaly in transaction_anomalies) or
                    transaction.is_high_value or
                    float(transaction.amount_local_currency) > 1000000  # High value threshold
                )
                timing_anomaly = any(anomaly['type'] in ['Backdated Entry', 'Closing Entry', 'Unusual Day', 'Holiday Entry'] for anomaly in transaction_anomalies)
                user_anomaly = any(anomaly['type'] == 'User Anomaly' for anomaly in transaction_anomalies)
                account_anomaly = any(anomaly['type'] in ['Duplicate Entry'] for anomaly in transaction_anomalies)  # Could be expanded
                pattern_anomaly = len(transaction_anomalies) > 1  # Multiple anomalies indicate pattern
                
                # Create analysis record
                TransactionAnalysis.objects.create(  # type: ignore
                    transaction=transaction,
                    session=session,
                    risk_score=risk_score,
                    risk_level=risk_level,
                    amount_anomaly=amount_anomaly,
                    timing_anomaly=timing_anomaly,
                    user_anomaly=user_anomaly,
                    account_anomaly=account_anomaly,
                    pattern_anomaly=pattern_anomaly,
                    analysis_details={
                        'anomalies': transaction_anomalies,
                        'risk_factors': self.get_risk_factors(transaction, all_anomalies)
                    }
                )
            
            # Update session results
            flagged_count = TransactionAnalysis.objects.filter(session=session, risk_level__in=['HIGH', 'CRITICAL']).count()  # type: ignore
            high_value_count = sum(1 for t in transactions if t.is_high_value)
            
            session.total_transactions = len(transactions)
            session.total_amount = sum(t.amount_local_currency for t in transactions)
            session.flagged_transactions = flagged_count
            session.high_value_transactions = high_value_count
            session.status = 'COMPLETED'
            session.completed_at = timezone.now()
            session.save()
            
            return {
                'success': True,
                'total_transactions': len(transactions),
                'flagged_transactions': flagged_count,
                'high_value_transactions': high_value_count,
                'anomalies_detected': sum(len(anomalies) for anomalies in all_anomalies.values()),
                'session_id': str(session.id)
            }
            
        except Exception as e:
            logger.error(f"Error in analysis: {e}")
            session.status = 'FAILED'
            session.completed_at = timezone.now()
            session.save()
            return {'error': str(e)}
    
    def get_transaction_anomalies(self, transaction, all_anomalies):
        """Get anomalies for a specific transaction"""
        transaction_anomalies = []
        
        # Check duplicates
        for duplicate in all_anomalies['duplicates']:
            for dup_transaction in duplicate['transactions']:
                if dup_transaction['id'] == str(transaction.id):
                    transaction_anomalies.append(duplicate)
        
        # Check backdated
        for backdated in all_anomalies['backdated']:
            if backdated['transaction_id'] == str(transaction.id):
                transaction_anomalies.append(backdated)
        
        # Check closing entries
        for closing in all_anomalies['closing_entries']:
            if closing['transaction_id'] == str(transaction.id):
                transaction_anomalies.append(closing)
        
        # Check unusual days
        for unusual in all_anomalies['unusual_days']:
            if unusual['transaction_id'] == str(transaction.id):
                transaction_anomalies.append(unusual)
        
        # Check holidays
        for holiday in all_anomalies['holidays']:
            if holiday['transaction_id'] == str(transaction.id):
                transaction_anomalies.append(holiday)
        
        return transaction_anomalies
    
    def get_risk_factors(self, transaction, all_anomalies):
        """Get risk factors for a transaction"""
        factors = []
        
        if transaction.is_high_value:
            factors.append('High value transaction')
        
        # Check for specific anomalies
        transaction_anomalies = self.get_transaction_anomalies(transaction, all_anomalies)
        for anomaly in transaction_anomalies:
            factors.append(f"{anomaly['type']} detected")
        
        return factors
    
    def calculate_risk_score(self, transaction, all_anomalies):
        """Calculate risk score for a transaction"""
        base_score = 0.0
        
        # High value transaction
        if transaction.is_high_value:
            base_score += 20.0
        
        # Check for anomalies
        transaction_anomalies = self.get_transaction_anomalies(transaction, all_anomalies)
        for anomaly in transaction_anomalies:
            base_score += anomaly.get('risk_score', 10.0)
        
        # Normalize to 0-100 range
        risk_score = min(max(base_score, 0), 100)
        
        return risk_score
    
    def determine_risk_level(self, risk_score):
        """Determine risk level based on score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def get_analysis_summary(self, session):
        """Get comprehensive summary statistics for an analysis session"""
        if not session:
            return {}
        
        analyses = TransactionAnalysis.objects.filter(session=session)
        
        # Get all transactions for this session
        transactions = SAPGLPosting.objects.filter(
            analysis__session=session
        ).distinct()
        
        # Run anomaly tests to get current data
        duplicate_anomalies = self.detect_duplicate_entries(transactions)
        user_anomalies = self.detect_user_anomalies(transactions)
        backdated_anomalies = self.detect_backdated_entries(transactions)
        closing_anomalies = self.detect_closing_entries(transactions)
        unusual_day_anomalies = self.detect_unusual_days(transactions)
        holiday_anomalies = self.detect_holiday_entries(transactions)
        
        # Risk distribution
        risk_distribution = analyses.values('risk_level').annotate(count=Count('id'))
        
        # Anomaly summary
        anomaly_summary = {
            'duplicate_entries': len(duplicate_anomalies),
            'user_anomalies': len(user_anomalies),
            'backdated_entries': len(backdated_anomalies),
            'closing_entries': len(closing_anomalies),
            'unusual_days': len(unusual_day_anomalies),
            'holiday_entries': len(holiday_anomalies)
        }
        
        # Charts data
        charts_data = self.generate_charts_data(
            transactions, duplicate_anomalies, user_anomalies, 
            backdated_anomalies, closing_anomalies, unusual_day_anomalies, holiday_anomalies
        )
        
        return {
            'session_id': session.id,
            'session_name': session.session_name,
            'status': session.status,
            'total_transactions': session.total_transactions,
            'total_amount': float(session.total_amount),
            'flagged_transactions': session.flagged_transactions,
            'high_value_transactions': session.high_value_transactions,
            'flag_rate': (session.flagged_transactions / session.total_transactions * 100) if session.total_transactions > 0 else 0,
            'risk_distribution': list(risk_distribution),
            'anomaly_summary': anomaly_summary,
            
            # Detailed anomaly data
            'duplicate_entries': duplicate_anomalies,
            'user_anomalies': user_anomalies,
            'backdated_entries': backdated_anomalies,
            'closing_entries': closing_anomalies,
            'unusual_days': unusual_day_anomalies,
            'holiday_entries': holiday_anomalies,
            
            # Charts data
            'charts_data': charts_data,
            
            # Timing information
            'created_at': session.created_at,
            'started_at': session.started_at,
            'completed_at': session.completed_at,
            'duration': (session.completed_at - session.started_at).total_seconds() if session.started_at and session.completed_at else None
        } 
    
    def generate_charts_data(self, transactions, duplicate_anomalies, user_anomalies, 
                           backdated_anomalies, closing_anomalies, unusual_day_anomalies, holiday_anomalies):
        """Generate charts data for visualization"""
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame([{
            'id': str(t.id),
            'gl_account': t.gl_account,
            'amount': float(t.amount_local_currency),
            'user_name': t.user_name,
            'posting_date': t.posting_date,
            'document_date': t.document_date,
            'document_type': t.document_type,
            'is_high_value': t.is_high_value
        } for t in transactions])
        # Ensure posting_date is datetime for .dt accessor
        df['posting_date'] = pd.to_datetime(df['posting_date'], errors='coerce')
        
        charts_data = {
            # Duplicate entries by type
            'duplicate_types': {
                'labels': ['Type 1', 'Type 2', 'Type 3', 'Type 4', 'Type 5', 'Type 6'],
                'data': [
                    len([d for d in duplicate_anomalies if d['type'] == 'Type 1 Duplicate']),
                    len([d for d in duplicate_anomalies if d['type'] == 'Type 2 Duplicate']),
                    len([d for d in duplicate_anomalies if d['type'] == 'Type 3 Duplicate']),
                    len([d for d in duplicate_anomalies if d['type'] == 'Type 4 Duplicate']),
                    len([d for d in duplicate_anomalies if d['type'] == 'Type 5 Duplicate']),
                    len([d for d in duplicate_anomalies if d['type'] == 'Type 6 Duplicate'])
                ]
            },
            
            # Anomalies by category
            'anomalies_by_category': {
                'labels': ['Duplicates', 'User Anomalies', 'Backdated', 'Closing', 'Unusual Days', 'Holidays'],
                'data': [
                    len(duplicate_anomalies),
                    len(user_anomalies),
                    len(backdated_anomalies),
                    len(closing_anomalies),
                    len(unusual_day_anomalies),
                    len(holiday_anomalies)
                ]
            },
            
            # Top users by anomaly count
            'top_users_anomalies': {
                'labels': [user['user_name'] for user in user_anomalies[:10]],
                'data': [user['risk_score'] for user in user_anomalies[:10]]
            },
            
            # Monthly trend of anomalies
            'monthly_anomalies': self.get_monthly_anomaly_trend(df, backdated_anomalies, closing_anomalies, unusual_day_anomalies, holiday_anomalies),
            
            # Risk distribution
            'risk_distribution': {
                'labels': ['Low', 'Medium', 'High', 'Critical'],
                'data': [0, 0, 0, 0]  # Will be populated from risk_distribution
            },
            
            # Amount distribution for anomalies
            'anomaly_amount_distribution': self.get_anomaly_amount_distribution(duplicate_anomalies, backdated_anomalies, closing_anomalies)
        }
        
        return charts_data
    
    def get_monthly_anomaly_trend(self, df, backdated_anomalies, closing_anomalies, unusual_day_anomalies, holiday_anomalies):
        """Get monthly trend of anomalies"""
        if df.empty:
            return {'labels': [], 'data': []}
        
        # Group by month
        df['month'] = df['posting_date'].dt.to_period('M')
        monthly_counts = df.groupby('month').size()
        
        # Count anomalies by month
        monthly_anomalies = {}
        for anomaly_list in [backdated_anomalies, closing_anomalies, unusual_day_anomalies, holiday_anomalies]:
            for anomaly in anomaly_list:
                month = pd.to_datetime(anomaly['posting_date']).to_period('M')
                monthly_anomalies[month] = monthly_anomalies.get(month, 0) + 1
        
        # Create trend data
        months = sorted(monthly_counts.index)
        labels = [str(month) for month in months]
        data = [monthly_anomalies.get(month, 0) for month in months]
        
        return {'labels': labels, 'data': data}
    
    def get_anomaly_amount_distribution(self, duplicate_anomalies, backdated_anomalies, closing_anomalies):
        """Get amount distribution for anomalies"""
        amounts = []
        
        # Collect amounts from different anomaly types
        for anomaly_list in [duplicate_anomalies, backdated_anomalies, closing_anomalies]:
            for anomaly in anomaly_list:
                if 'amount' in anomaly:
                    amounts.append(anomaly['amount'])
        
        if not amounts:
            return {'labels': [], 'data': []}
        
        # Create amount ranges
        amount_ranges = [
            (0, 10000, '0-10K'),
            (10000, 100000, '10K-100K'),
            (100000, 1000000, '100K-1M'),
            (1000000, 10000000, '1M-10M'),
            (10000000, float('inf'), '10M+')
        ]
        
        range_counts = [0] * len(amount_ranges)
        for amount in amounts:
            for i, (min_amt, max_amt, _) in enumerate(amount_ranges):
                if min_amt <= amount < max_amt:
                    range_counts[i] += 1
                    break
        
        labels = [label for _, _, label in amount_ranges]
        return {'labels': labels, 'data': range_counts} 