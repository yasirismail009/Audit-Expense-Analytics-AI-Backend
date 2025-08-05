#!/usr/bin/env python
"""
Script to examine the actual data stored in UserAnalysisResult table in Docker PostgreSQL
"""

import os
import sys
import django
import json
import psycopg2
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')
django.setup()

def connect_to_docker_db():
    """Connect to the Docker PostgreSQL database"""
    try:
        # Database connection parameters from docker-compose
        connection = psycopg2.connect(
            host="localhost",  # Docker host
            port="5433",       # Mapped port from docker-compose
            database="analytics_db",
            user="analytics_user",
            password="analytics_password"
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def check_user_analysis_table_structure():
    """Check the structure of UserAnalysisResult table"""
    
    print("=" * 80)
    print("USER ANALYSIS TABLE STRUCTURE")
    print("=" * 80)
    
    connection = connect_to_docker_db()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'user_analysis_results'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        print(f"Table 'user_analysis_results' exists: {table_exists}")
        print()
        
        if not table_exists:
            print("Table does not exist. Creating it...")
            return
        
        # Get table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'user_analysis_results'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("TABLE STRUCTURE:")
        print("-" * 20)
        for column in columns:
            print(f"  {column[0]}: {column[1]} (nullable: {column[2]}, default: {column[3]})")
        print()
        
        # Get row count
        cursor.execute("SELECT COUNT(*) FROM user_analysis_results;")
        row_count = cursor.fetchone()[0]
        print(f"Total rows in user_analysis_results: {row_count}")
        print()
        
        if row_count == 0:
            print("No data found in user_analysis_results table.")
            return
        
        # Get sample data
        cursor.execute("""
            SELECT id, data_file_id, analysis_date, analysis_type, analysis_version, 
                   status, processing_duration, created_at, updated_at
            FROM user_analysis_results
            ORDER BY analysis_date DESC
            LIMIT 5;
        """)
        
        rows = cursor.fetchall()
        print("SAMPLE DATA (Basic Fields):")
        print("-" * 30)
        for i, row in enumerate(rows):
            print(f"Row {i+1}:")
            print(f"  ID: {row[0]}")
            print(f"  Data File ID: {row[1]}")
            print(f"  Analysis Date: {row[2]}")
            print(f"  Analysis Type: {row[3]}")
            print(f"  Analysis Version: {row[4]}")
            print(f"  Status: {row[5]}")
            print(f"  Processing Duration: {row[6]}")
            print(f"  Created: {row[7]}")
            print(f"  Updated: {row[8]}")
            print()
        
        # Get JSON field data
        cursor.execute("""
            SELECT id, analysis_info, user_transaction_summary, user_anomalies, 
                   user_risk_assessment, chart_data, export_data
            FROM user_analysis_results
            ORDER BY analysis_date DESC
            LIMIT 3;
        """)
        
        json_rows = cursor.fetchall()
        print("JSON FIELD DATA:")
        print("-" * 20)
        for i, row in enumerate(json_rows):
            print(f"Record {i+1} (ID: {row[0]}):")
            
            # Analysis Info
            if row[1]:
                print(f"  analysis_info: {json.dumps(row[1], indent=4)}")
            else:
                print(f"  analysis_info: null")
            
            # User Transaction Summary
            if row[2]:
                print(f"  user_transaction_summary: {len(row[2])} items")
                if len(row[2]) > 0:
                    print(f"    Sample: {json.dumps(row[2][0], indent=6)}")
            else:
                print(f"  user_transaction_summary: null")
            
            # User Anomalies
            if row[3]:
                print(f"  user_anomalies: {len(row[3])} items")
                if len(row[3]) > 0:
                    print(f"    Sample: {json.dumps(row[3][0], indent=6)}")
            else:
                print(f"  user_anomalies: null")
            
            # User Risk Assessment
            if row[4]:
                if isinstance(row[4], list):
                    print(f"  user_risk_assessment: {len(row[4])} items")
                    if len(row[4]) > 0:
                        print(f"    Sample: {json.dumps(row[4][0], indent=6)}")
                else:
                    print(f"  user_risk_assessment: {type(row[4])} - {json.dumps(row[4], indent=4)}")
            else:
                print(f"  user_risk_assessment: null")
            
            # Chart Data
            if row[5]:
                print(f"  chart_data: {json.dumps(row[5], indent=4)}")
            else:
                print(f"  chart_data: null")
            
            # Export Data
            if row[6]:
                print(f"  export_data: {len(row[6])} items")
                if len(row[6]) > 0:
                    print(f"    Sample: {json.dumps(row[6][0], indent=6)}")
            else:
                print(f"  export_data: null")
            
            print()
        
        cursor.close()
        
    except Exception as e:
        print(f"Error examining table: {e}")
        import traceback
        traceback.print_exc()
    finally:
        connection.close()

def check_related_tables():
    """Check related tables"""
    
    print("=" * 80)
    print("RELATED TABLES")
    print("=" * 80)
    
    connection = connect_to_docker_db()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()
        
        # Check data_files table
        cursor.execute("SELECT COUNT(*) FROM data_files;")
        data_files_count = cursor.fetchone()[0]
        print(f"Data Files: {data_files_count} records")
        
        if data_files_count > 0:
            cursor.execute("""
                SELECT id, file_name, status, total_records, processed_records, uploaded_at
                FROM data_files
                ORDER BY uploaded_at DESC
                LIMIT 3;
            """)
            data_files = cursor.fetchall()
            for i, row in enumerate(data_files):
                print(f"  File {i+1}: {row[1]} (ID: {row[0]}, Status: {row[2]}, Records: {row[3]}/{row[4]})")
        print()
        
        # Check file_processing_jobs table
        cursor.execute("SELECT COUNT(*) FROM file_processing_jobs;")
        jobs_count = cursor.fetchone()[0]
        print(f"Processing Jobs: {jobs_count} records")
        
        if jobs_count > 0:
            cursor.execute("""
                SELECT id, data_file_id, status, run_anomalies, requested_anomalies, created_at
                FROM file_processing_jobs
                ORDER BY created_at DESC
                LIMIT 3;
            """)
            jobs = cursor.fetchall()
            for i, row in enumerate(jobs):
                print(f"  Job {i+1}: ID {row[0]}, File {row[1]}, Status: {row[2]}, Anomalies: {row[3]}")
        print()
        
        # Check sap_gl_postings table
        cursor.execute("SELECT COUNT(*) FROM sap_gl_postings;")
        postings_count = cursor.fetchone()[0]
        print(f"SAP GL Postings: {postings_count} records")
        
        if postings_count > 0:
            cursor.execute("""
                SELECT COUNT(DISTINCT user_name) as unique_users,
                       COUNT(DISTINCT gl_account) as unique_accounts,
                       COUNT(DISTINCT data_file_id) as unique_files
                FROM sap_gl_postings;
            """)
            stats = cursor.fetchone()
            print(f"  Unique Users: {stats[0]}")
            print(f"  Unique Accounts: {stats[1]}")
            print(f"  Unique Files: {stats[2]}")
        print()
        
        cursor.close()
        
    except Exception as e:
        print(f"Error examining related tables: {e}")
        import traceback
        traceback.print_exc()
    finally:
        connection.close()

def check_database_connection():
    """Test database connection"""
    
    print("=" * 80)
    print("DATABASE CONNECTION TEST")
    print("=" * 80)
    
    connection = connect_to_docker_db()
    if not connection:
        print("❌ Failed to connect to database")
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Connected to PostgreSQL: {version[0]}")
        
        cursor.execute("SELECT current_database(), current_user;")
        db_info = cursor.fetchone()
        print(f"   Database: {db_info[0]}")
        print(f"   User: {db_info[1]}")
        
        cursor.close()
        return True
        
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False
    finally:
        connection.close()

if __name__ == "__main__":
    try:
        # Test connection first
        if check_database_connection():
            check_user_analysis_table_structure()
            check_related_tables()
        else:
            print("Cannot proceed without database connection.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 