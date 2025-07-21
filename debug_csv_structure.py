#!/usr/bin/env python3
"""
Debug script to analyze CSV file structure and identify GL account issues
"""

import os
import sys
import csv
import io

def analyze_csv_structure(csv_file_path):
    """Analyze CSV file structure to understand GL account field issues"""
    
    print("=== CSV Structure Analysis ===\n")
    
    if not os.path.exists(csv_file_path):
        print(f"✗ CSV file not found: {csv_file_path}")
        return
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            # Read first few lines to understand structure
            lines = file.readlines()
            
            if not lines:
                print("✗ CSV file is empty")
                return
            
            print(f"1. CSV file: {csv_file_path}")
            print(f"   Total lines: {len(lines)}")
            print()
            
            # Parse header
            header_line = lines[0].strip()
            print("2. Header analysis:")
            print(f"   Raw header: {header_line}")
            
            # Parse header as CSV
            header_reader = csv.reader([header_line])
            headers = next(header_reader)
            print(f"   Parsed headers ({len(headers)} columns):")
            for i, header in enumerate(headers):
                print(f"     {i+1}. '{header}'")
            print()
            
            # Look for GL account related columns
            gl_account_columns = []
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if any(keyword in header_lower for keyword in ['gl', 'account', 'g/l', 'general ledger']):
                    gl_account_columns.append((i, header))
            
            print("3. GL Account related columns:")
            if gl_account_columns:
                for col_idx, col_name in gl_account_columns:
                    print(f"   Column {col_idx+1}: '{col_name}'")
            else:
                print("   ⚠️ No obvious GL account columns found!")
            print()
            
            # Analyze sample data
            print("4. Sample data analysis:")
            
            # Read first 10 data rows
            sample_rows = []
            for i, line in enumerate(lines[1:11], start=2):  # Skip header
                if line.strip():
                    sample_rows.append((i, line.strip()))
            
            print(f"   Analyzing {len(sample_rows)} sample rows:")
            
            for row_num, row_line in sample_rows:
                # Parse row as CSV
                row_reader = csv.reader([row_line])
                row_data = next(row_reader)
                
                print(f"   Row {row_num}:")
                print(f"     Raw: {row_line}")
                print(f"     Parsed ({len(row_data)} columns): {row_data}")
                
                # Check GL account columns
                if gl_account_columns:
                    for col_idx, col_name in gl_account_columns:
                        if col_idx < len(row_data):
                            value = row_data[col_idx]
                            print(f"     GL Account '{col_name}': '{value}' (length: {len(value)})")
                        else:
                            print(f"     GL Account '{col_name}': Column index out of range")
                else:
                    # If no GL account columns found, show all columns
                    print(f"     All columns: {row_data}")
                print()
            
            # Check for empty GL accounts
            print("5. Empty GL account analysis:")
            
            empty_count = 0
            total_count = 0
            
            # Read all data rows
            for i, line in enumerate(lines[1:], start=2):
                if line.strip():
                    total_count += 1
                    row_reader = csv.reader([line.strip()])
                    row_data = next(row_reader)
                    
                    # Check GL account columns
                    if gl_account_columns:
                        for col_idx, col_name in gl_account_columns:
                            if col_idx < len(row_data):
                                value = row_data[col_idx].strip()
                                if not value:
                                    empty_count += 1
                                    break
                    else:
                        # If no GL account columns, check if any column is empty
                        if any(not col.strip() for col in row_data):
                            empty_count += 1
            
            print(f"   Total data rows: {total_count}")
            print(f"   Rows with empty GL accounts: {empty_count}")
            print(f"   Percentage empty: {(empty_count/total_count*100):.1f}%" if total_count > 0 else "N/A")
            
            # Show sample empty rows
            if empty_count > 0:
                print("   Sample rows with empty GL accounts:")
                empty_sample_count = 0
                
                for i, line in enumerate(lines[1:], start=2):
                    if line.strip() and empty_sample_count < 5:
                        row_reader = csv.reader([line.strip()])
                        row_data = next(row_reader)
                        
                        # Check if this row has empty GL account
                        has_empty_gl = False
                        if gl_account_columns:
                            for col_idx, col_name in gl_account_columns:
                                if col_idx < len(row_data) and not row_data[col_idx].strip():
                                    has_empty_gl = True
                                    break
                        else:
                            has_empty_gl = any(not col.strip() for col in row_data)
                        
                        if has_empty_gl:
                            empty_sample_count += 1
                            print(f"     Row {i}: {row_data}")
            
            print()
            
            # Recommendations
            print("6. Recommendations:")
            if not gl_account_columns:
                print("   ⚠️ No GL account columns found. Check column names in CSV.")
                print("   Common GL account column names:")
                print("     - 'G/L Account'")
                print("     - 'GL Account'")
                print("     - 'Account'")
                print("     - 'General Ledger Account'")
            elif empty_count > 0:
                print(f"   ⚠️ {empty_count} rows have empty GL accounts.")
                print("   This will cause issues in duplicate detection and analysis.")
                print("   Consider:")
                print("     - Cleaning the source data")
                print("     - Using a default GL account for empty values")
                print("     - Filtering out transactions without GL accounts")
            else:
                print("   ✓ GL account data looks good!")
    
    except Exception as e:
        print(f"✗ Error analyzing CSV file: {e}")

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python debug_csv_structure.py <csv_file_path>")
        print("Example: python debug_csv_structure.py sample_data.csv")
        return
    
    csv_file_path = sys.argv[1]
    analyze_csv_structure(csv_file_path)

if __name__ == "__main__":
    main() 