from django.core.management.base import BaseCommand
import pandas as pd
import os

class Command(BaseCommand):
    help = 'Debug Chart of Accounts file structure'

    def handle(self, *args, **options):
        file_path = "temp_uploads/Chart of Accounts Data File.xlsx"
        
        if not os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f"File not found: {file_path}"))
            return
        
        try:
            # Read the Excel file
            df = pd.read_excel(file_path, engine='openpyxl')
            
            self.stdout.write(f"🔍 File: {file_path}")
            self.stdout.write(f"📊 Total rows: {len(df)}")
            self.stdout.write(f"📋 Total columns: {len(df.columns)}")
            self.stdout.write("")
            
            # Show all column names
            self.stdout.write("📋 All column names:")
            for i, col in enumerate(df.columns):
                self.stdout.write(f"   {i+1:2d}. '{col}'")
            self.stdout.write("")
            
            # Check row 8 (index 7) if it exists
            if len(df) > 7:
                self.stdout.write("🔍 Row 8 (index 7) content:")
                row8 = df.iloc[7]
                self.stdout.write(f"   Row 8 values: {list(row8.values)}")
                self.stdout.write("")
                
                self.stdout.write("🔍 Column names and their values in row 8:")
                for i, (col, val) in enumerate(zip(df.columns, row8.values)):
                    self.stdout.write(f"   {i+1:2d}. '{col}' = '{val}'")
                self.stdout.write("")
                
                # Check if row 8 has meaningful data
                has_data = any(pd.notna(val) and str(val).strip() != '' for val in row8.values)
                self.stdout.write(f"📊 Row 8 has meaningful data: {has_data}")
                
                # Check for chart of accounts indicators
                chart_indicators = ['account', 'type', 'sub type', 'sub sub type', 'g/l acct long text', 'ref to fs', 'fin q1']
                row8_text = ' '.join([str(val).lower() for val in row8.values if pd.notna(val)])
                
                self.stdout.write(f"🔍 Looking for chart indicators in row 8:")
                for indicator in chart_indicators:
                    if indicator in row8_text:
                        self.stdout.write(f"   ✅ Found: '{indicator}'")
                    else:
                        self.stdout.write(f"   ❌ Missing: '{indicator}'")
            else:
                self.stdout.write("❌ File has less than 8 rows")
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {e}"))
