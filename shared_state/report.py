import json
import argparse
import sys
from database import get_connection, DB_LOCK

def generate_report(input_file, output_file):
    print(f"Generating report to {output_file}...")
    
    conn = get_connection()
    c = conn.cursor()
    
    # CRITICAL SECTION: Reading Shared State
    # We acquire the lock to ensure we read a consistent snapshot of the DB
    with DB_LOCK:
        c.execute("SELECT taxpayer_id, total_federal_tax, total_state_tax FROM final_liability")
        rows = c.fetchall()
        
    conn.close()
    
    # Convert to dictionary for O(1) lookup
    results = {}
    for row in rows:
        results[row['taxpayer_id']] = {
            'federal_tax': row['total_federal_tax'],
            'state_tax': row['total_state_tax']
        }
    
    # Stream input file to preserve order (Read-only, no lock needed)
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                if not line.strip():
                    continue
                
                try:
                    record = json.loads(line)
                    tp_id = record.get('taxpayer_id')
                    
                    if tp_id in results:
                        output_obj = {
                            "taxpayer_id": tp_id,
                            "federal_tax": results[tp_id]['federal_tax'],
                            "state_tax": results[tp_id]['state_tax']
                        }
                        outfile.write(json.dumps(output_obj) + '\n')
                        
                except json.JSONDecodeError:
                    continue
                    
        print("Report generation complete.")
        
    except FileNotFoundError:
        print(f"Error: Input file {input_file} not found.")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-inputFile', required=True)
    parser.add_argument('-outputFile', required=True)
    args = parser.parse_args()
    
    generate_report(args.inputFile, args.outputFile)