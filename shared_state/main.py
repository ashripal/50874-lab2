import argparse
import os
import time

# Import the threaded implementations
import database
import ingest   # Contains ingest_data_threaded
import analysis # Contains run_analysis_threaded
import report   # Contains generate_report

def main():
    parser = argparse.ArgumentParser(description='Avalon Tax Calculator 2026 (Threaded)')
    parser.add_argument('-inputFile', required=True, help='Path to NDJSON input file')
    parser.add_argument('-outputFile', required=True, help='Path for NDJSON output file')
    
    args = parser.parse_args()
    
    # Start fresh
    if os.path.exists(database.DB_NAME):
        try:
            os.remove(database.DB_NAME)
        except PermissionError:
            print("Error: Database file is locked. Close any other connections.")
            return

    # Initialize Threaded DB
    database.init_db()

    start_time = time.time()

    # --- Step 1: Threaded Ingestion ---
    # (Producer-Consumer with Mutex on DB Writes)
    print("\n--- Step 1: Threaded Ingestion ---")
    ingest.ingest_data_threaded(args.inputFile)
    
    # --- Step 2: Threaded Analysis ---
    # (Parallel Computation, Mutex on DB Reads/Writes)
    print("\n--- Step 2: Threaded Analysis ---")
    analysis.run_analysis_threaded()
    
    # --- Step 3: Reporting ---
    # (Mutex on Final Read)
    print("\n--- Step 3: Reporting ---")
    report.generate_report(args.inputFile, args.outputFile)
    
    duration = time.time() - start_time
    print(f"\nSuccess! Processing complete in {duration:.2f} seconds.")
    print(f"Results written to {args.outputFile}")

if __name__ == "__main__":
    main()