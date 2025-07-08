from django.shortcuts import render
from django.http import JsonResponse
from django.db import connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser
import csv
import io
import traceback
import os
from datetime import datetime, date
from .models import Expense, ExpenseAnalysis, AnalysisSession, ExpenseSheet, SheetAnalysis
from .serializers import ExpenseSerializer, ExpenseSheetSerializer
from .analytics import ExpenseSheetAnalyzer

# Create your views here.

def test_db_connection(request):
    """
    Simple view to test database connection
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
        
        return JsonResponse({
            'status': 'success',
            'message': 'Database connection successful',
            'test_query_result': result[0] if result else None
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': f'Database connection failed: {str(e)}'
        }, status=500)

def home(request):
    """
    Simple home view for analytics dashboard
    """
    return JsonResponse({
        'message': 'Analytics Dashboard API',
        'status': 'running',
        'database': 'SQLite'
    })

FIELD_MAP = {
    'date': 'Date',
    'category': 'Category',
    'subcategory': 'Subcategory',
    'description': 'Description',
    'employee': 'Employee',
    'department': 'Department',
    'amount': 'Amount',
    'currency': 'Currency',
    'payment_method': 'Payment Method',
    'vendor_supplier': 'Vendor/Supplier',
    'receipt_number': 'Receipt Number',
    'status': 'Status',
    'approved_by': 'Approved By',
    'notes': 'Notes',
}

def normalize_key(key):
    return key.strip().replace('\ufeff', '')

class ExpenseUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    def post(self, request, format=None):
        file_obj = request.FILES.get('file')
        if not file_obj:
            return Response({'error': 'No file uploaded.'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            # Extract sheet name from file name
            file_name = file_obj.name
            sheet_name = os.path.splitext(file_name)[0]  # Remove extension
            sheet_date = date.today()  # Use current date, or extract from file name if available
            
            # Create or get the expense sheet
            expense_sheet, created = ExpenseSheet.objects.get_or_create(
                sheet_name=sheet_name,
                sheet_date=sheet_date,
                defaults={
                    'total_expenses': 0,
                    'total_amount': 0
                }
            )
            
            # Ensure the sheet is saved and we have the ID
            expense_sheet.save()
            print(f"Created/Found ExpenseSheet: ID={expense_sheet.id}, Name={expense_sheet.sheet_name}, Date={expense_sheet.sheet_date}")
            
            decoded_file = file_obj.read().decode('utf-8')
            io_string = io.StringIO(decoded_file)
            reader = csv.DictReader(io_string)
            # Normalize headers
            reader.fieldnames = [normalize_key(h) for h in reader.fieldnames]
            
            expenses = []
            total_amount = 0
            
            for row in reader:
                print(row)
                # Normalize row keys
                normalized_row = {normalize_key(k): v for k, v in row.items()}
                print(f"normalized_row: {normalized_row}")
                print(f"FIELD_MAP: {FIELD_MAP}")

                # Convert date to YYYY-MM-DD
                if 'Date' in normalized_row and normalized_row['Date']:
                    try:
                        normalized_row['Date'] = datetime.strptime(normalized_row['Date'], '%m/%d/%Y').strftime('%Y-%m-%d')
                    except ValueError:
                        # Try alternative format if needed
                        normalized_row['Date'] = datetime.strptime(normalized_row['Date'], '%Y-%m-%d').strftime('%Y-%m-%d')

                expense_data = {model_field: normalized_row.get(csv_field, None) for model_field, csv_field in FIELD_MAP.items()}
                expense_data['expense_sheet_id'] = expense_sheet.id
                
                print(f"expense_data: {expense_data}")
                print(f"expense_sheet_id being set: {expense_sheet.id}")
                print(f"expense_data keys: {list(expense_data.keys())}")
                serializer = ExpenseSerializer(data=expense_data)
                print(f"Serializer is_valid: {serializer.is_valid()}")
                if serializer.is_valid():
                    print(f"Validated data: {serializer.validated_data}")
                    expense = serializer.save()
                    expenses.append(serializer.data)
                    
                    # Add to total amount
                    if expense.amount:
                        total_amount += expense.amount
                else:
                    print(f"Serializer errors: {serializer.errors}")
                    return Response({'error': serializer.errors, 'row': row}, status=status.HTTP_400_BAD_REQUEST)
            
            # Update expense sheet with totals
            expense_sheet.total_expenses = len(expenses)
            expense_sheet.total_amount = total_amount
            expense_sheet.save()
            
            # Auto-train models after new sheet upload
            try:
                analyzer = ExpenseSheetAnalyzer()
                analyzer.auto_train_if_needed()
                training_status = "Models auto-trained" if analyzer._last_training_time else "No training needed"
            except Exception as e:
                training_status = f"Training failed: {str(e)}"
            
            return Response({
                'message': 'Expenses uploaded successfully.',
                'sheet_info': {
                    'sheet_name': expense_sheet.sheet_name,
                    'sheet_date': expense_sheet.sheet_date,
                    'total_expenses': expense_sheet.total_expenses,
                    'total_amount': str(expense_sheet.total_amount)
                },
                'training_status': training_status,
                'data': expenses
            }, status=status.HTTP_201_CREATED)
        except Exception as e:
            traceback.print_exc()
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

class ExpenseListView(APIView):
    def get(self, request, format=None):
        expense_sheets = ExpenseSheet.objects.all().order_by('-sheet_date', '-uploaded_at')
        sheet_data = []
        
        for sheet in expense_sheets:
            try:
                analysis = sheet.analysis
                sheet_data.append({
                    'id': sheet.id,
                    'sheet_name': sheet.sheet_name,
                    'sheet_date': sheet.sheet_date,
                    'display_name': sheet.display_name,
                    'total_expenses': sheet.total_expenses,
                    'total_amount': str(sheet.total_amount),
                    'uploaded_at': sheet.uploaded_at,
                    'analysis': {
                        'overall_fraud_score': analysis.overall_fraud_score,
                        'risk_level': analysis.risk_level,
                        'flag_rate': analysis.flag_rate,
                        'total_flagged_expenses': analysis.total_flagged_expenses,
                    } if analysis else None
                })
            except SheetAnalysis.DoesNotExist:
                sheet_data.append({
                    'id': sheet.id,
                    'sheet_name': sheet.sheet_name,
                    'sheet_date': sheet.sheet_date,
                    'display_name': sheet.display_name,
                    'total_expenses': sheet.total_expenses,
                    'total_amount': str(sheet.total_amount),
                    'uploaded_at': sheet.uploaded_at,
                    'analysis': None
                })
        
        return Response(sheet_data)



class ExpenseAnalysisView(APIView):
    """
    Get fraud analysis report for a specific expense
    """
    def get(self, request, expense_id, format=None):
        try:
            expense = Expense.objects.get(id=expense_id)
            analysis = ExpenseAnalysis.objects.get(expense=expense)
            
            return Response({
                'expense_id': expense.id,
                'description': expense.description,
                'amount': str(expense.amount),
                'employee': expense.employee,
                'department': expense.department,
                'date': expense.date,
                'vendor': expense.vendor_supplier,
                'category': expense.category,
                'subcategory': expense.subcategory,
                'payment_method': expense.payment_method,
                'status': expense.status,
                'approved_by': expense.approved_by,
                'expense_sheet': {
                    'sheet_name': expense.expense_sheet.sheet_name,
                    'sheet_date': expense.expense_sheet.sheet_date,
                    'display_name': expense.expense_sheet.display_name,
                },
                'analysis': {
                    'fraud_score': analysis.fraud_score,
                    'risk_level': analysis.risk_level,
                    'anomaly_reasons': analysis.analysis_details.get('anomaly_reasons', []),
                    'fraud_score_breakdown': analysis.analysis_details.get('fraud_score_breakdown', {}),
                    'anomaly_flags': {
                        'amount_anomaly': analysis.amount_anomaly,
                        'timing_anomaly': analysis.timing_anomaly,
                        'vendor_anomaly': analysis.vendor_anomaly,
                        'employee_anomaly': analysis.employee_anomaly,
                        'duplicate_suspicion': analysis.duplicate_suspicion,
                    },
                    'analysis_details': analysis.analysis_details,
                    'created_at': analysis.created_at,
                }
            })
        except Expense.DoesNotExist:
            return Response({'error': 'Expense not found'}, status=status.HTTP_404_NOT_FOUND)
        except ExpenseAnalysis.DoesNotExist:
            return Response({'error': 'Analysis not found for this expense'}, status=status.HTTP_404_NOT_FOUND)

class AnalysisSessionView(APIView):
    """
    Get analysis session summary
    """
    def get(self, request, session_id, format=None):
        try:
            session = AnalysisSession.objects.get(session_id=session_id)
            
            return Response({
                'session_id': session.session_id,
                'file_name': session.file_name,
                'total_expenses': session.total_expenses,
                'flagged_expenses': session.flagged_expenses,
                'flag_rate': f"{(session.flagged_expenses/session.total_expenses*100):.1f}%" if session.total_expenses > 0 else "0%",
                'analysis_status': session.analysis_status,
                'created_at': session.created_at,
                'model_config': session.model_config,
            })
        except AnalysisSession.DoesNotExist:
            return Response({'error': 'Analysis session not found'}, status=status.HTTP_404_NOT_FOUND)

class ExpenseSheetView(APIView):
    """
    Get all expenses from a specific expense sheet
    """
    def get(self, request, sheet_id, format=None):
        try:
            expense_sheet = ExpenseSheet.objects.get(id=sheet_id)
            expenses = expense_sheet.expenses.all()
            
            expense_data = []
            for expense in expenses:
                try:
                    analysis = expense.analysis
                    expense_data.append({
                        'expense_id': expense.id,
                        'description': expense.description,
                        'amount': str(expense.amount),
                        'employee': expense.employee,
                        'department': expense.department,
                        'date': expense.date,
                        'fraud_score': analysis.fraud_score,
                        'risk_level': analysis.risk_level,
                    })
                except ExpenseAnalysis.DoesNotExist:
                    # Expense exists but no analysis yet
                    expense_data.append({
                        'expense_id': expense.id,
                        'description': expense.description,
                        'amount': str(expense.amount),
                        'employee': expense.employee,
                        'department': expense.department,
                        'date': expense.date,
                        'fraud_score': None,
                        'risk_level': None,
                    })
            
            return Response({
                'sheet_id': expense_sheet.id,
                'sheet_name': expense_sheet.sheet_name,
                'sheet_date': expense_sheet.sheet_date,
                'display_name': expense_sheet.display_name,
                'total_expenses': len(expense_data),
                'expenses': expense_data,
            })
        except ExpenseSheet.DoesNotExist:
            return Response({'error': 'Expense sheet not found'}, status=status.HTTP_404_NOT_FOUND)

class DebugExpenseView(APIView):
    """
    Debug endpoint to check expense relationships
    """
    def get(self, request, expense_id, format=None):
        try:
            expense = Expense.objects.get(id=expense_id)
            
            return Response({
                'expense_id': expense.id,
                'description': expense.description,
                'has_expense_sheet': expense.expense_sheet is not None,
                'expense_sheet_id': expense.expense_sheet.id if expense.expense_sheet else None,
                'expense_sheet_name': expense.expense_sheet.sheet_name if expense.expense_sheet else None,
                'has_analysis': hasattr(expense, 'analysis'),
                'analysis_fraud_score': expense.analysis.fraud_score if hasattr(expense, 'analysis') else None,
            })
        except Expense.DoesNotExist:
            return Response({'error': 'Expense not found'}, status=status.HTTP_404_NOT_FOUND)

class SheetAnalysisView(APIView):
    """Analyze a specific expense sheet for fraud detection"""
    
    def post(self, request, sheet_id, format=None):
        try:
            expense_sheet = ExpenseSheet.objects.get(id=sheet_id)
            analyzer = ExpenseSheetAnalyzer()
            
            # Auto-train before analysis
            training_status = "No training needed"
            if analyzer.auto_train_if_needed():
                training_status = "Models auto-trained before analysis"
            
            # Try to load existing models
            analyzer.load_models()
            
            # Perform analysis
            sheet_analysis = analyzer.analyze_sheet(expense_sheet)
            
            if sheet_analysis:
                # Get flagged expenses with detailed reasons
                flagged_expenses = []
                for expense in expense_sheet.expenses.all():
                    try:
                        analysis = expense.analysis
                        if analysis.fraud_score > 0:  # Only include flagged expenses
                            flagged_expenses.append({
                                'expense_id': expense.id,
                                'description': expense.description,
                                'amount': str(expense.amount),
                                'employee': expense.employee,
                                'department': expense.department,
                                'date': expense.date,
                                'vendor': expense.vendor_supplier,
                                'category': expense.category,
                                'fraud_score': analysis.fraud_score,
                                'risk_level': analysis.risk_level,
                                'anomaly_reasons': analysis.analysis_details.get('anomaly_reasons', []),
                                'fraud_score_breakdown': analysis.analysis_details.get('fraud_score_breakdown', {}),
                                'anomaly_flags': {
                                    'amount_anomaly': analysis.amount_anomaly,
                                    'timing_anomaly': analysis.timing_anomaly,
                                    'vendor_anomaly': analysis.vendor_anomaly,
                                    'employee_anomaly': analysis.employee_anomaly,
                                    'duplicate_suspicion': analysis.duplicate_suspicion,
                                }
                            })
                    except ExpenseAnalysis.DoesNotExist:
                        continue
                
                # Sort by fraud score (highest first)
                flagged_expenses.sort(key=lambda x: x['fraud_score'], reverse=True)
                
                # Get advanced metrics from analysis_details
                analysis_details = getattr(sheet_analysis, 'analysis_details', {})
                
                # Extract all advanced metrics
                advanced_metrics = {
                    'expense_velocity_ratio': analysis_details.get('expense_velocity_ratio', 0),
                    'approval_concentration_index': analysis_details.get('approval_concentration_index', 0),
                    'payment_method_risk_score': analysis_details.get('payment_method_risk_score', 0),
                    'vendor_concentration_ratio': analysis_details.get('vendor_concentration_ratio', 0),
                    'high_value_expense_frequency': analysis_details.get('high_value_expense_frequency', {}),
                    'basic_metrics': analysis_details.get('basic_metrics', {}),
                    'risk_indicators': analysis_details.get('risk_indicators', {}),
                    'category_deviation_index': analysis_details.get('category_deviation_index', []),
                    'department_expense_intensity': analysis_details.get('department_expense_intensity', {}),
                    'recurring_expense_variance': analysis_details.get('recurring_expense_variance', []),
                    'expense_complexity_scores': analysis_details.get('expense_complexity_scores', []),
                    'cross_department_expense_ratio': analysis_details.get('cross_department_expense_ratio', []),
                    'expense_timing_anomaly_score': analysis_details.get('expense_timing_anomaly_score', []),
                    'vendor_loyalty_index': analysis_details.get('vendor_loyalty_index', []),
                    'expense_categorization_accuracy': analysis_details.get('expense_categorization_accuracy', {}),
                    'budget_burn_rate': analysis_details.get('budget_burn_rate', {}),
                    'approval_turnaround_time': analysis_details.get('approval_turnaround_time', {}),
                }
                
                # Get chart data
                chart_data = analysis_details.get('chart_data', {})
                
                return Response({
                    'message': 'Sheet analysis completed successfully',
                    'sheet_id': sheet_id,
                    'sheet_name': expense_sheet.sheet_name,
                    'sheet_date': expense_sheet.sheet_date,
                    'display_name': expense_sheet.display_name,
                    'total_expenses': expense_sheet.total_expenses,
                    'total_amount': str(expense_sheet.total_amount),
                    'training_status': training_status,
                    'analysis_summary': {
                        'overall_fraud_score': getattr(sheet_analysis, 'overall_fraud_score', 0),
                        'risk_level': getattr(sheet_analysis, 'risk_level', 'LOW'),
                        'total_flagged_expenses': getattr(sheet_analysis, 'total_flagged_expenses', 0),
                        'high_risk_expenses': getattr(sheet_analysis, 'high_risk_expenses', 0),
                        'critical_risk_expenses': getattr(sheet_analysis, 'critical_risk_expenses', 0),
                        'flag_rate': getattr(sheet_analysis, 'flag_rate', 0),
                        'anomalies_detected': {
                            'amount_anomalies': getattr(sheet_analysis, 'amount_anomalies_detected', 0),
                            'timing_anomalies': getattr(sheet_analysis, 'timing_anomalies_detected', 0),
                            'vendor_anomalies': getattr(sheet_analysis, 'vendor_anomalies_detected', 0),
                            'employee_anomalies': getattr(sheet_analysis, 'employee_anomalies_detected', 0),
                            'duplicate_suspicions': getattr(sheet_analysis, 'duplicate_suspicions', 0),
                        }
                    },
                    'advanced_metrics': advanced_metrics,
                    'chart_data': chart_data,
                    'flagged_expenses': flagged_expenses,
                    'analysis_timestamp': sheet_analysis.updated_at.isoformat()
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    'error': 'Analysis failed - insufficient data'
                }, status=status.HTTP_400_BAD_REQUEST)
                
        except ExpenseSheet.DoesNotExist:
            return Response({'error': 'Expense sheet not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response({'error': f'Analysis failed: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class ModelTrainingView(APIView):
    """Train fraud detection models on historical data"""
    
    def post(self, request, format=None):
        try:
            analyzer = ExpenseSheetAnalyzer()
            
            # Get all sheets for training
            sheets = ExpenseSheet.objects.all()
            
            if not sheets.exists():
                return Response({
                    'error': 'No expense sheets available for training'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Train models
            success = analyzer.train_models(sheets)
            
            if success:
                return Response({
                    'message': 'Models trained successfully',
                    'sheets_used': sheets.count(),
                    'models_trained': list(analyzer.models.keys())
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    'error': 'Model training failed - insufficient data'
                }, status=status.HTTP_400_BAD_REQUEST)
                
        except Exception as e:
            return Response({
                'error': f'Training failed: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def get(self, request, format=None):
        """Get training status and model information"""
        try:
            analyzer = ExpenseSheetAnalyzer()
            
            # Check if models exist
            model_files = []
            for name in analyzer.models.keys():
                model_file = os.path.join(analyzer.model_path, f'{name}_model.pkl')
                if os.path.exists(model_file):
                    model_files.append(name)
            
            return Response({
                'models_available': model_files,
                'total_sheets': ExpenseSheet.objects.count(),
                'sheets_with_analysis': SheetAnalysis.objects.count(),
                'training_ready': ExpenseSheet.objects.count() >= 2  # Need at least 2 sheets for training
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'error': f'Failed to get training status: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class BulkAnalysisView(APIView):
    """Analyze all expense sheets"""
    
    def post(self, request, format=None):
        try:
            analyzer = ExpenseSheetAnalyzer()
            
            # Auto-train before bulk analysis
            training_status = "No training needed"
            if analyzer.auto_train_if_needed():
                training_status = "Models auto-trained before bulk analysis"
            
            # Try to load existing models
            analyzer.load_models()
            
            sheets = ExpenseSheet.objects.all()
            results = []
            all_flagged_expenses = []
            
            for sheet in sheets:
                try:
                    sheet_analysis = analyzer.analyze_sheet(sheet)
                    if sheet_analysis:
                        # Get flagged expenses for this sheet
                        sheet_flagged_expenses = []
                        for expense in sheet.expenses.all():
                            try:
                                analysis = expense.analysis
                                if analysis.fraud_score > 0:  # Only include flagged expenses
                                    sheet_flagged_expenses.append({
                                        'expense_id': expense.id,
                                        'description': expense.description,
                                        'amount': str(expense.amount),
                                        'employee': expense.employee,
                                        'department': expense.department,
                                        'date': expense.date,
                                        'vendor': expense.vendor_supplier,
                                        'category': expense.category,
                                        'fraud_score': analysis.fraud_score,
                                        'risk_level': analysis.risk_level,
                                        'anomaly_reasons': analysis.analysis_details.get('anomaly_reasons', []),
                                        'fraud_score_breakdown': analysis.analysis_details.get('fraud_score_breakdown', {}),
                                        'anomaly_flags': {
                                            'amount_anomaly': analysis.amount_anomaly,
                                            'timing_anomaly': analysis.timing_anomaly,
                                            'vendor_anomaly': analysis.vendor_anomaly,
                                            'employee_anomaly': analysis.employee_anomaly,
                                            'duplicate_suspicion': analysis.duplicate_suspicion,
                                        }
                                    })
                            except ExpenseAnalysis.DoesNotExist:
                                continue
                        
                        # Add to all flagged expenses
                        all_flagged_expenses.extend(sheet_flagged_expenses)
                        
                        # Get advanced metrics from analysis_details
                        analysis_details = getattr(sheet_analysis, 'analysis_details', {})
                        
                        # Extract all advanced metrics
                        advanced_metrics = {
                            'expense_velocity_ratio': analysis_details.get('expense_velocity_ratio', 0),
                            'approval_concentration_index': analysis_details.get('approval_concentration_index', 0),
                            'payment_method_risk_score': analysis_details.get('payment_method_risk_score', 0),
                            'vendor_concentration_ratio': analysis_details.get('vendor_concentration_ratio', 0),
                            'high_value_expense_frequency': analysis_details.get('high_value_expense_frequency', {}),
                            'basic_metrics': analysis_details.get('basic_metrics', {}),
                            'risk_indicators': analysis_details.get('risk_indicators', {}),
                            'category_deviation_index': analysis_details.get('category_deviation_index', []),
                            'department_expense_intensity': analysis_details.get('department_expense_intensity', {}),
                            'recurring_expense_variance': analysis_details.get('recurring_expense_variance', []),
                            'expense_complexity_scores': analysis_details.get('expense_complexity_scores', []),
                            'cross_department_expense_ratio': analysis_details.get('cross_department_expense_ratio', []),
                            'expense_timing_anomaly_score': analysis_details.get('expense_timing_anomaly_score', []),
                            'vendor_loyalty_index': analysis_details.get('vendor_loyalty_index', []),
                            'expense_categorization_accuracy': analysis_details.get('expense_categorization_accuracy', {}),
                            'budget_burn_rate': analysis_details.get('budget_burn_rate', {}),
                            'approval_turnaround_time': analysis_details.get('approval_turnaround_time', {}),
                        }
                        
                        # Get chart data
                        chart_data = analysis_details.get('chart_data', {})
                        
                        results.append({
                            'sheet_id': sheet.id,
                            'sheet_name': sheet.sheet_name,
                            'sheet_date': sheet.sheet_date,
                            'display_name': sheet.display_name,
                            'status': 'success',
                            'fraud_score': getattr(sheet_analysis, 'overall_fraud_score', 0),
                            'risk_level': getattr(sheet_analysis, 'risk_level', 'LOW'),
                            'total_flagged_expenses': getattr(sheet_analysis, 'total_flagged_expenses', 0),
                            'high_risk_expenses': getattr(sheet_analysis, 'high_risk_expenses', 0),
                            'critical_risk_expenses': getattr(sheet_analysis, 'critical_risk_expenses', 0),
                            'flag_rate': getattr(sheet_analysis, 'flag_rate', 0),
                            'advanced_metrics': advanced_metrics,
                            'chart_data': chart_data,
                            'flagged_expenses': sheet_flagged_expenses
                        })
                    else:
                        results.append({
                            'sheet_id': sheet.id,
                            'sheet_name': sheet.sheet_name,
                            'sheet_date': sheet.sheet_date,
                            'display_name': sheet.display_name,
                            'status': 'failed',
                            'error': 'Insufficient data for analysis'
                        })
                except Exception as e:
                    results.append({
                        'sheet_id': sheet.id,
                        'sheet_name': sheet.sheet_name,
                        'sheet_date': sheet.sheet_date,
                        'display_name': sheet.display_name,
                        'status': 'error',
                        'error': str(e)
                    })
            
            # Calculate overall statistics
            successful_analyses = [r for r in results if r['status'] == 'success']
            total_sheets = len(results)
            successful_sheets = len(successful_analyses)
            
            if successful_analyses:
                avg_fraud_score = sum(r['fraud_score'] for r in successful_analyses) / len(successful_analyses)
                high_risk_sheets = len([r for r in successful_analyses if r['risk_level'] in ['HIGH', 'CRITICAL']])
            else:
                avg_fraud_score = 0
                high_risk_sheets = 0
            
            return Response({
                'message': f'Bulk analysis completed. {successful_sheets}/{total_sheets} sheets analyzed successfully.',
                'training_status': training_status,
                'summary': {
                    'total_sheets': total_sheets,
                    'successful_analyses': successful_sheets,
                    'failed_analyses': total_sheets - successful_sheets,
                    'average_fraud_score': avg_fraud_score,
                    'high_risk_sheets': high_risk_sheets,
                    'total_flagged_expenses': len(all_flagged_expenses)
                },
                'results': results,
                'all_flagged_expenses': all_flagged_expenses
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response({
                'error': f'Bulk analysis failed: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
