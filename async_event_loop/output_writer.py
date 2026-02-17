import asyncio
import aiosqlite
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DB_PATH = "taxes.db"

async def write_output(output_file_path):
    """
    Reads finalized tax computations from the DB and writes them to an NDJSON file.
    """
    print(f"Writing results to {output_file_path}...")
    
    async with aiosqlite.connect(DB_PATH) as db:
        # We enforce the order of output to match input if taxpayer_ids are sortable, 
        # or we just dump them. The spec says "The order of output taxes due must match 
        # the order of households in the input file"[cite: 169].
        # Since we ingested sequentially, sorting by rowid in the original table usually preserves order,
        # but to be strictly safe, we might need to join against the original taxpayers table ordering.
        # However, for this implementation, we will select based on the computation table.
        
        # JOIN to ensure we only get completed records and (optionally) preserve ingestion order if rowid aligns
        query = """
        SELECT t.taxpayer_id, c.total_federal_tax, c.total_state_tax
        FROM taxpayers t
        JOIN tax_computations c ON t.taxpayer_id = c.taxpayer_id
        ORDER BY t.rowid ASC
        """
        
        try:
            with open(output_file_path, 'w') as f:
                async with db.execute(query) as cursor:
                    async for row in cursor:
                        taxpayer_id, fed_tax, state_tax = row
                        
                        # Construct Output Object [cite: 165-168]
                        output_obj = {
                            "taxpayer_id": taxpayer_id,
                            "federal_tax": int(fed_tax), # Ensure whole numbers [cite: 127]
                            "state_tax": int(state_tax)
                        }
                        
                        # Write NDJSON line
                        f.write(json.dumps(output_obj) + "\n")
                        
            print("Output writing complete.")
            
        except Exception as e:
            logging.error(f"Failed to write output file: {e}")
            raise

if __name__ == "__main__":
    # Test run
    asyncio.run(write_output("taxes.ndjson"))