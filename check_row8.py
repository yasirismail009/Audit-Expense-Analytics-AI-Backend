#!/usr/bin/env python
"""
Check row 8 of the Chart of Accounts file to see the column structure
"""

import pandas as pd
import sys

def check_row8():
    file_path = "temp_uploads/Chart of Accounts Data File.xlsx"
    
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, engine='openpyxl')
        
        print(f"🔍 File: {file_path}")
        print(f"📊 Total rows: {len(df)}")
        print(f"📋 Total columns: {len(df.columns)}")
        print()
        
        # Check if row 8 exists (index 7)
        if len(df) > 7:
            print("🔍 Row 8 (index 7) content:")
            row8 = df.iloc[7]
            print(f"   Row 8 values: {list(row8.values)}")
            print()
            
            print("🔍 Column names and their values in row 8:")
            for i, (col, val) in enumerate(zip(df.columns, row8.values)):
                print(f"   {i+1:2d}. '{col}' = '{val}'")
            print()
            
            # Check if row 8 has meaningful data
            has_data = any(pd.notna(val) and str(val).strip() != '' for val in row8.values)
            print(f"📊 Row 8 has meaningful data: {has_data}")
            
            # Check for chart of accounts indicators
            chart_indicators = ['account', 'type', 'sub type', 'sub sub type', 'g/l acct long text', 'ref to fs', 'fin q1']
            row8_text = ' '.join([str(val).lower() for val in row8.values if pd.notna(val)])
            
            print(f"🔍 Looking for chart indicators in row 8:")
            for indicator in chart_indicators:
                if indicator in row8_text:
                    print(f"   ✅ Found: '{indicator}'")
                else:
                    print(f"   ❌ Missing: '{indicator}'")
            
        else:
            print("❌ File has less than 8 rows")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_row8()
