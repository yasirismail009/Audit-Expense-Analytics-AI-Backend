from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connection
from django.conf import settings
import os
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Complete database reset - drops all tables and recreates them'

    def add_arguments(self, parser):
        parser.add_argument(
            '--confirm',
            action='store_true',
            help='Confirm that you want to reset the entire database',
        )
        parser.add_argument(
            '--keep-migrations',
            action='store_true',
            help='Keep migration files (only reset database)',
        )
        parser.add_argument(
            '--backup',
            action='store_true',
            help='Create backup before reset (if supported)',
        )

    def handle(self, *args, **options):
        if not options['confirm']:
            self.stdout.write(
                self.style.ERROR(
                    '⚠️  WARNING: This will completely reset your database!\n'
                    'All data will be permanently deleted and tables will be recreated.\n'
                    'Use --confirm to proceed.'
                )
            )
            return

        self.stdout.write(
            self.style.WARNING('🔄 Starting complete database reset...')
        )

        try:
            # Step 1: Backup (if requested)
            if options['backup']:
                self.stdout.write('📦 Creating backup...')
                self._create_backup()

            # Step 2: Drop all tables
            self.stdout.write('🗑️  Dropping all tables...')
            self._drop_all_tables()

            # Step 3: Remove migration files (unless keep-migrations is specified)
            if not options['keep_migrations']:
                self.stdout.write('📁 Removing migration files...')
                self._remove_migration_files()

            # Step 4: Run migrations
            self.stdout.write('🔄 Running migrations...')
            call_command('migrate', verbosity=0)

            # Step 5: Create superuser (optional)
            self.stdout.write('👤 Creating superuser...')
            self._create_superuser()

            self.stdout.write(
                self.style.SUCCESS('✅ Database reset completed successfully!')
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Database reset failed: {str(e)}')
            )
            raise

    def _create_backup(self):
        """Create database backup if supported"""
        try:
            # This is a placeholder - implement based on your database
            self.stdout.write('   Backup creation not implemented for this database type')
        except Exception as e:
            self.stdout.write(f'   Warning: Backup failed - {str(e)}')

    def _drop_all_tables(self):
        """Drop all tables in the database"""
        with connection.cursor() as cursor:
            # Get all table names
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
            """)
            tables = [row[0] for row in cursor.fetchall()]

            # Disable foreign key checks (PostgreSQL)
            cursor.execute("SET session_replication_role = replica;")

            # Drop each table
            for table in tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS \"{table}\" CASCADE;")
                    self.stdout.write(f"   Dropped table: {table}")
                except Exception as e:
                    self.stdout.write(f"   Warning: Could not drop {table} - {str(e)}")

            # Re-enable foreign key checks
            cursor.execute("SET session_replication_role = DEFAULT;")

    def _remove_migration_files(self):
        """Remove all migration files except __init__.py"""
        import glob
        
        # Find all migration directories
        for app_config in settings.INSTALLED_APPS:
            if isinstance(app_config, str):
                app_name = app_config.split('.')[-1]
            else:
                app_name = app_config.name.split('.')[-1]
            
            migrations_dir = os.path.join(settings.BASE_DIR, app_name, 'migrations')
            
            if os.path.exists(migrations_dir):
                # Remove all .py files except __init__.py
                for py_file in glob.glob(os.path.join(migrations_dir, '*.py')):
                    if not py_file.endswith('__init__.py'):
                        try:
                            os.remove(py_file)
                            self.stdout.write(f"   Removed: {py_file}")
                        except Exception as e:
                            self.stdout.write(f"   Warning: Could not remove {py_file} - {str(e)}")

    def _create_superuser(self):
        """Create a default superuser"""
        try:
            from django.contrib.auth import get_user_model
            User = get_user_model()
            
            # Check if superuser already exists
            if User.objects.filter(is_superuser=True).exists():
                self.stdout.write('   Superuser already exists')
                return

            # Create default superuser
            username = 'admin'
            email = 'admin@example.com'
            password = 'admin123'

            if not User.objects.filter(username=username).exists():
                User.objects.create_superuser(username, email, password)
                self.stdout.write(f'   Created superuser: {username} / {password}')
            else:
                self.stdout.write('   Default superuser already exists')
                
        except Exception as e:
            self.stdout.write(f'   Warning: Could not create superuser - {str(e)}') 