import json
import sqlite3
import argparse
import sys
from database import get_connection

def generate_report(input_file, output_file):
    print(f"Generating report to {output_file}...")
    
    # 1. Load all calculated taxes into memory for fast lookup
    # We use a dictionary: { taxpayer_id: { federal: X, state: Y } }
    conn = get_connection()
    c = conn.cursor()
    
    # Fetch data from the 'final_liability' table [cite: 121, 164-167]
    # Note: Values were already rounded to whole dollars in analysis.py [cite: 125]
    c.execute("SELECT taxpayer_id, total_federal_tax, total_state_tax FROM final_liability")
    results = {}
    for row in c.fetchall():
        results[row['taxpayer_id']] = {
            'federal_tax': row['total_federal_tax'],
            'state_tax': row['total_state_tax']
        }
    
    conn.close()
    
    # 2. Stream the input file to preserve order 
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line_num, line in enumerate(infile, 1):
                if not line.strip():
                    continue
                
                try:
                    # We only need the ID from the input line
                    record = json.loads(line)
                    tp_id = record.get('taxpayer_id')
                    
                    if tp_id in results:
                        # Construct Output Object [cite: 164-167]
                        output_obj = {
                            "taxpayer_id": tp_id,
                            "federal_tax": results[tp_id]['federal_tax'],
                            "state_tax": results[tp_id]['state_tax']
                        }
                        
                        # Write as NDJSON
                        outfile.write(json.dumps(output_obj) + '\n')
                    else:
                        print(f"Warning: No calculation found for taxpayer {tp_id} (Line {line_num})")
                        
                except json.JSONDecodeError:
                    print(f"Skipping malformed JSON on line {line_num}")
                    
        print("Report generation complete.")
        
    except FileNotFoundError:
        print(f"Error: Could not find input file {input_file}")
        sys.exit(1)
    except IOError as e:
        print(f"Error writing to output file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate tax report.')
    parser.add_argument('-inputFile', required=True, help='Path to NDJSON input file')
    parser.add_argument('-outputFile', required=True, help='Path to NDJSON output file')
    
    args = parser.parse_args()
    generate_report(args.inputFile, args.outputFile)