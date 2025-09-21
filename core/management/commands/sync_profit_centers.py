"""
Management command to sync profit center data between GL and COA

This command ensures that all profit center codes from GL postings and 
cost center codes from Chart of Accounts are properly synchronized in the 
ProfitCenter master table.

Usage:
    python manage.py sync_profit_centers
"""

from django.core.management.base import BaseCommand
from core.models import sync_profit_center_data


class Command(BaseCommand):
    help = 'Sync profit center data between GL postings and Chart of Accounts'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS('Starting profit center synchronization...')
        )
        
        if options['dry_run']:
            self.stdout.write(
                self.style.WARNING('DRY RUN MODE - No changes will be made')
            )
            # TODO: Implement dry run logic
            return
        
        try:
            result = sync_profit_center_data()
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'Profit center synchronization completed successfully!'
                )
            )
            self.stdout.write(f'Created: {result["created_count"]} profit centers')
            self.stdout.write(f'Updated: {result["updated_count"]} profit centers')
            self.stdout.write(f'Total centers processed: {result["total_centers"]}')
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error during synchronization: {str(e)}')
            )
            raise
