from django.core.management.base import BaseCommand
from core.parallel_processor import start_processor, stop_processor, get_processor_stats
import time

class Command(BaseCommand):
    help = 'Control the parallel processing system'

    def add_arguments(self, parser):
        parser.add_argument(
            'action',
            choices=['start', 'stop', 'status', 'run'],
            help='Action to perform: start, stop, status, or run (start and monitor)'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=4,
            help='Number of worker processes (default: 4)'
        )
        parser.add_argument(
            '--monitor',
            action='store_true',
            help='Monitor the processor (for run action)'
        )

    def handle(self, *args, **options):
        action = options['action']
        num_workers = options['workers']
        
        if action == 'start':
            self.start_processor(num_workers)
        elif action == 'stop':
            self.stop_processor()
        elif action == 'status':
            self.show_status()
        elif action == 'run':
            self.run_processor(num_workers, options['monitor'])
    
    def start_processor(self, num_workers):
        """Start the parallel processor"""
        try:
            processor = start_processor(num_workers=num_workers)
            self.stdout.write(
                self.style.SUCCESS(f'✅ Parallel processor started with {num_workers} workers')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Failed to start processor: {e}')
            )
    
    def stop_processor(self):
        """Stop the parallel processor"""
        try:
            stop_processor()
            self.stdout.write(
                self.style.SUCCESS('✅ Parallel processor stopped')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Failed to stop processor: {e}')
            )
    
    def show_status(self):
        """Show processor status"""
        stats = get_processor_stats()
        
        self.stdout.write("📊 Parallel Processor Status:")
        self.stdout.write("=" * 40)
        
        if stats['is_running']:
            self.stdout.write(f"🟢 Status: Running")
            self.stdout.write(f"👷 Workers: {stats['num_workers']}")
            self.stdout.write(f"✅ Jobs Processed: {stats['jobs_processed']}")
            self.stdout.write(f"❌ Jobs Failed: {stats['jobs_failed']}")
            self.stdout.write(f"⏱️  Uptime: {stats['uptime']}")
            if stats['last_job_time']:
                self.stdout.write(f"🕐 Last Job: {stats['last_job_time']}")
        else:
            self.stdout.write("🔴 Status: Stopped")
    
    def run_processor(self, num_workers, monitor):
        """Start processor and optionally monitor it"""
        try:
            # Start the processor
            processor = start_processor(num_workers=num_workers)
            
            self.stdout.write(
                self.style.SUCCESS(f'✅ Parallel processor started with {num_workers} workers')
            )
            
            if monitor:
                self.stdout.write("📊 Monitoring processor (Press Ctrl+C to stop)...")
                
                try:
                    while True:
                        time.sleep(5)  # Update every 5 seconds
                        stats = get_processor_stats()
                        
                        if stats['is_running']:
                            self.stdout.write(
                                f"📈 Stats: {stats['jobs_processed']} processed, "
                                f"{stats['jobs_failed']} failed, "
                                f"Uptime: {stats['uptime']}"
                            )
                        else:
                            self.stdout.write("❌ Processor stopped unexpectedly")
                            break
                            
                except KeyboardInterrupt:
                    self.stdout.write("\n🛑 Stopping processor...")
                    stop_processor()
                    self.stdout.write(
                        self.style.SUCCESS('✅ Processor stopped')
                    )
            else:
                self.stdout.write("💡 Use --monitor to see real-time stats")
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Failed to run processor: {e}')
            ) 