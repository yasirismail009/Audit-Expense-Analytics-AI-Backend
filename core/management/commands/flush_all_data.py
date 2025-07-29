from django.core.management.base import BaseCommand
from django.db import connection
from django.apps import apps
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Flush all data from all database tables'

    def add_arguments(self, parser):
        parser.add_argument(
            '--confirm',
            action='store_true',
            help='Confirm that you want to delete all data',
        )
        parser.add_argument(
            '--tables',
            nargs='+',
            help='Specific tables to flush (default: all tables)',
        )

    def handle(self, *args, **options):
        if not options['confirm']:
            self.stdout.write(
                self.style.WARNING(
                    'This will delete ALL data from the database!\n'
                    'Use --confirm to proceed.'
                )
            )
            return

        # Get all models
        all_models = apps.get_models()
        
        if options['tables']:
            # Filter models by specified table names
            models_to_flush = [
                model for model in all_models 
                if model._meta.db_table in options['tables']
            ]
        else:
            models_to_flush = all_models

        self.stdout.write(
            self.style.WARNING(f'Flushing data from {len(models_to_flush)} tables...')
        )

        # Disable foreign key checks for faster deletion (PostgreSQL)
        with connection.cursor() as cursor:
            cursor.execute("SET session_replication_role = replica;")

        try:
            # Delete data from each model
            for model in models_to_flush:
                table_name = model._meta.db_table
                
                try:
                    count = model.objects.count()
                    
                    if count > 0:
                        model.objects.all().delete()
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'Deleted {count} records from {table_name}'
                            )
                        )
                    else:
                        self.stdout.write(
                            f'Table {table_name} is already empty'
                        )
                except Exception as e:
                    # Table might not exist yet
                    self.stdout.write(
                        f'Table {table_name} does not exist or is not accessible: {str(e)}'
                    )

        finally:
            # Re-enable foreign key checks
            with connection.cursor() as cursor:
                cursor.execute("SET session_replication_role = DEFAULT;")

        self.stdout.write(
            self.style.SUCCESS('Database flush completed successfully!')
        ) 