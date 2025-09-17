"""
Management command to train AI models on GL transaction data
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from core.gl_prediction_tasks import (
    train_gl_anomaly_detection_model,
    train_gl_volume_prediction_model,
    predict_gl_anomalies
)
from core.models import Engagement, Client, SAPGLPosting
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Train AI models on GL transaction data for prediction and anomaly detection'

    def add_arguments(self, parser):
        parser.add_argument(
            '--client-name',
            type=str,
            help='Train models for specific client',
        )
        parser.add_argument(
            '--engagement-id',
            type=str,
            help='Train models for specific engagement',
        )
        parser.add_argument(
            '--model-type',
            choices=['anomaly', 'volume', 'both'],
            default='both',
            help='Type of model to train',
        )
        parser.add_argument(
            '--test-predictions',
            action='store_true',
            help='Test predictions after training',
        )

    def handle(self, *args, **options):
        client_name = options.get('client_name')
        engagement_id = options.get('engagement_id')
        model_type = options.get('model_type')
        test_predictions = options.get('test_predictions')

        self.stdout.write(self.style.SUCCESS('🚀 Starting GL AI Model Training'))
        
        # Check available data
        self.stdout.write('📊 Checking available data...')
        
        total_transactions = SAPGLPosting.objects.count()
        total_engagements = Engagement.objects.count()
        total_clients = Client.objects.count()
        
        self.stdout.write(f'  📈 Total GL Transactions: {total_transactions:,}')
        self.stdout.write(f'  📁 Total Engagements: {total_engagements}')
        self.stdout.write(f'  👥 Total Clients: {total_clients}')
        
        if total_transactions == 0:
            self.stdout.write(self.style.ERROR('❌ No GL transaction data found!'))
            return
        
        if client_name:
            try:
                client = Client.objects.get(client_name__icontains=client_name)
                self.stdout.write(f'🎯 Training for client: {client.client_name}')
                client_transactions = SAPGLPosting.objects.filter(
                    data_file__engagement__client=client
                ).count()
                self.stdout.write(f'  📊 Client transactions: {client_transactions:,}')
            except Client.DoesNotExist:
                self.stdout.write(self.style.ERROR(f'❌ Client "{client_name}" not found!'))
                return
        
        # Train models
        training_results = []
        
        if model_type in ['anomaly', 'both']:
            self.stdout.write('🔍 Training Anomaly Detection Model...')
            try:
                result = train_gl_anomaly_detection_model(
                    engagement_id=engagement_id,
                    client_name=client_name
                )
                training_results.append(('Anomaly Detection', result))
                
                if result.get('success'):
                    self.stdout.write(self.style.SUCCESS(
                        f'✅ Anomaly model trained successfully! '
                        f'Model ID: {result["model_id"]}, '
                        f'Training time: {result["training_duration"]:.1f}s'
                    ))
                else:
                    self.stdout.write(self.style.ERROR(
                        f'❌ Anomaly model training failed: {result.get("error")}'
                    ))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Anomaly model training error: {e}'))
        
        if model_type in ['volume', 'both']:
            self.stdout.write('📈 Training Volume Prediction Model...')
            try:
                result = train_gl_volume_prediction_model(
                    engagement_id=engagement_id,
                    client_name=client_name
                )
                training_results.append(('Volume Prediction', result))
                
                if result.get('success'):
                    self.stdout.write(self.style.SUCCESS(
                        f'✅ Volume model trained successfully! '
                        f'Model ID: {result["model_id"]}, '
                        f'R²: {result.get("test_r2", 0):.3f}, '
                        f'Training time: {result["training_duration"]:.1f}s'
                    ))
                else:
                    self.stdout.write(self.style.ERROR(
                        f'❌ Volume model training failed: {result.get("error")}'
                    ))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Volume model training error: {e}'))
        
        # Test predictions
        if test_predictions and any(r[1].get('success') for r in training_results):
            self.stdout.write('🧪 Testing predictions...')
            
            # Get a sample engagement for testing
            sample_engagement = Engagement.objects.filter(
                data_files__sapglposting__isnull=False
            ).distinct().first()
            
            if sample_engagement:
                self.stdout.write(f'🎯 Testing on engagement: {sample_engagement.engagement_name}')
                
                try:
                    prediction_result = predict_gl_anomalies(str(sample_engagement.id))
                    
                    if prediction_result.get('success'):
                        self.stdout.write(self.style.SUCCESS(
                            f'✅ Prediction successful! '
                            f'Anomalous: {prediction_result["is_anomalous"]}, '
                            f'Risk: {prediction_result["risk_level"]}, '
                            f'Confidence: {prediction_result["confidence"]:.3f}'
                        ))
                    else:
                        self.stdout.write(self.style.ERROR(
                            f'❌ Prediction failed: {prediction_result.get("error")}'
                        ))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'❌ Prediction error: {e}'))
            else:
                self.stdout.write(self.style.WARNING('⚠️ No engagement with GL data found for testing'))
        
        # Summary
        self.stdout.write('\n📋 Training Summary:')
        for model_name, result in training_results:
            if result.get('success'):
                self.stdout.write(self.style.SUCCESS(f'  ✅ {model_name}: SUCCESS'))
            else:
                self.stdout.write(self.style.ERROR(f'  ❌ {model_name}: FAILED'))
        
        self.stdout.write(self.style.SUCCESS('\n🎉 GL AI Model Training Complete!'))
