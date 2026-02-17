import asyncio
import argparse
import os
import aiosqlite
from ingestion import ingest_file
from analysis_engine import run_analysis_engine
from output_writer import write_output

# Define DB Schema script inline for convenience (or import it)
SCHEMA_SCRIPT = """
-- (Paste the full SQL schema from the previous step here if not using a separate file)
-- For this implementation, we assume schema.sql exists or we run it here.
"""

async def initialize_db(db_path="taxes.db"):
    """
    Ensures the database is clean and has the correct schema before starting.
    """
    if os.path.exists(db_path):
        os.remove(db_path) # Start fresh for every run to avoid duplicate key errors
        
    print("Initializing database...")
    async with aiosqlite.connect(db_path) as db:
        # Load schema from file or string
        # Here we assume schema.sql is in the same directory
        with open('schema.sql', 'r') as f:
            await db.executescript(f.read())

async def main():
    # 1. CLI Argument Parsing 
    parser = argparse.ArgumentParser(description="Avalon Tax Calculator 2026")
    parser.add_argument('-inputFile', required=True, help="Path to input NDJSON file")
    parser.add_argument('-outputFile', required=True, help="Path to output NDJSON file")
    
    args = parser.parse_args()
    
    # 2. Pipeline Execution
    try:
        # Step A: Initialize DB
        await initialize_db()
        
        # Step B: Ingestion
        # Streams file to DB, calculating EWMA and hydrating tables
        await ingest_file(args.inputFile)
        
        # Step C: Analysis
        # Processes taxes, FIFO gains, and Brackets
        await run_analysis_engine()
        
        # Step D: Output
        # Writes final results to disk
        await write_output(args.outputFile)
        
        print("\nSUCCESS: Tax computation complete.")
        
    except Exception as e:
        print(f"\nFAILURE: {e}")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())