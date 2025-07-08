import csv
import uuid
import pandas as pd
from datetime import datetime
from django.core.management.base import BaseCommand
from core.models import Expense, ExpenseAnalysis, AnalysisSession
from core.analytics import ExpenseFraudAnalyzer
from django.db import transaction

class Command(BaseCommand):
    help = 'Analyze expenses from a CSV file and store analytics results.'

    def add_arguments(self, parser):
        parser.add_argument('csv_path', type=str, help='Path to the CSV file to analyze')
        parser.add_argument('--session', type=str, help='Optional session ID for this analysis run')

    def handle(self, *args, **options):
        csv_path = options['csv_path']
        session_id = options.get('session') or str(uuid.uuid4())
        file_name = csv_path.split('/')[-1]
        total_expenses = 0
        flagged_expenses = 0
        model_config = {
            'models': ['IsolationForest', 'XGBoost', 'LOF', 'DBSCAN', 'Prophet'],
            'version': 'scaffold',
        }

        # Read CSV into pandas DataFrame
        df = pd.read_csv(csv_path)
        
        # Initialize the fraud analyzer
        analyzer = ExpenseFraudAnalyzer()
        
        # Train models on the data
        analyzer.train_models(df)
        
        with transaction.atomic():
            session = AnalysisSession.objects.create(
                session_id=session_id,
                file_name=file_name,
                total_expenses=len(df),
                flagged_expenses=0,  # Will update after analysis
                analysis_status='RUNNING',
                model_config=model_config,
            )

            flagged_expenses = 0
            
            for index, row in df.iterrows():
                # Create or update expense record
                expense, created = Expense.objects.update_or_create(
                    expense_id=row['expense_id'],
                    defaults={
                        'analysis_session': session,  # Link to the analysis session
                        'date': row['date'],
                        'category': row['category'],
                        'subcategory': row['subcategory'],
                        'description': row['description'],
                        'employee': row['employee'],
                        'department': row['department'],
                        'amount': row['amount'],
                        'currency': row['currency'],
                        'payment_method': row['payment_method'],
                        'vendor_supplier': row['vendor_supplier'],
                        'receipt_number': row['receipt_number'],
                        'status': row['status'],
                        'approved_by': row['approved_by'],
                        'notes': row.get('notes', ''),
                    }
                )
                
                # Run fraud analysis on this expense
                expense_df = df.iloc[[index]]  # Single row DataFrame
                results = analyzer.predict_fraud(expense_df)
                
                # Count flagged expenses
                if results['fraud_score'] > 50:  # Threshold for flagging
                    flagged_expenses += 1
                
                # Store analysis results
                ExpenseAnalysis.objects.update_or_create(
                    expense=expense,
                    defaults={
                        'fraud_score': results['fraud_score'],
                        'isolation_forest_score': results['isolation_forest_score'],
                        'xgboost_score': results['xgboost_score'],
                        'lof_score': results['lof_score'],
                        'random_forest_score': results['random_forest_score'],
                        'dbscan_cluster': results['dbscan_cluster'],
                        'prophet_anomaly': False,  # Not implemented in this version
                        'risk_level': results['risk_level'],
                        'analysis_details': {
                            'model_scores': {
                                'isolation_forest': results['isolation_forest_score'],
                                'xgboost': results['xgboost_score'],
                                'lof': results['lof_score'],
                                'random_forest': results['random_forest_score'],
                            },
                            'anomalies': results['anomalies'],
                            'feature_importance': 'Available in detailed analysis'
                        },
                        'amount_anomaly': results['anomalies']['amount_anomaly'],
                        'timing_anomaly': results['anomalies']['timing_anomaly'],
                        'vendor_anomaly': results['anomalies']['vendor_anomaly'],
                        'employee_anomaly': results['anomalies']['employee_anomaly'],
                        'duplicate_suspicion': results['anomalies']['duplicate_suspicion'],
                    }
                )
            
            # Update session with final counts
            session.flagged_expenses = flagged_expenses
            session.analysis_status = 'COMPLETED'
            session.save()
        
        self.stdout.write(
            self.style.SUCCESS(
                f'Analysis complete! Session ID: {session_id}\n'
                f'Total expenses: {len(df)}\n'
                f'Flagged expenses: {flagged_expenses}\n'
                f'Flag rate: {(flagged_expenses/len(df)*100):.1f}%'
            )
        ) 