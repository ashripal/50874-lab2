import argparse
import os
import sys

# Import the logic from our modules
import ingest
import analysis
import report
import database

def main():
    parser = argparse.ArgumentParser(description='Avalon Tax Calculator 2026')
    
    # Spec-compliant arguments [cite: 170-171]
    parser.add_argument('-inputFile', required=True, help='Path to NDJSON input file')
    parser.add_argument('-outputFile', required=True, help='Path for NDJSON output file')
    
    args = parser.parse_args()
    
    # Step 0: Ensure DB is clean/ready
    # (Optional: Delete DB file to start fresh, or let ingest handle upserts)
    if os.path.exists(database.DB_NAME):
        os.remove(database.DB_NAME)
    database.init_db()

    # Step 1: Ingest
    print("--- Step 1: Ingestion ---")
    ingest.ingest_data(args.inputFile)
    
    # Step 2: Analyze
    print("\n--- Step 2: Analysis ---")
    # We open a connection to pass to the analysis functions
    conn = database.get_connection()
    analysis.calculate_capital_gains(conn)
    analysis.calculate_ewma(conn)
    analysis.calculate_federal_tax(conn)
    analysis.calculate_state_tax(conn)
    conn.close()
    
    # Step 3: Report
    print("\n--- Step 3: Reporting ---")
    report.generate_report(args.inputFile, args.outputFile)
    
    print(f"\nSuccess! Results written to {args.outputFile}")

if __name__ == "__main__":
    main()