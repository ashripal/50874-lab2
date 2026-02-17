import asyncio
import aiosqlite
import logging
from decimal import Decimal, ROUND_HALF_UP

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DB_PATH = "taxes.db"

# ---------------------------------------------------------
# UTILITY: Math & Brackets
# ---------------------------------------------------------

def round_dollar(d: Decimal) -> Decimal:
    """Rounds to the nearest whole dollar."""
    return d.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

def calculate_bracket_tax(income: Decimal, brackets: list) -> Decimal:
    """
    Generic function to calculate progressive tax.
    brackets: list of (limit, rate). Last limit should be Infinity.
    """
    tax = Decimal("0")
    previous_limit = Decimal("0")
    
    for limit, rate in brackets:
        if income > previous_limit:
            # The amount of income falling into this bracket
            taxable_in_bracket = min(income, limit) - previous_limit
            tax += taxable_in_bracket * rate
            previous_limit = limit
        else:
            break
    return tax

# ---------------------------------------------------------
# STEP 1: Capital Gains (FIFO)
# ---------------------------------------------------------

async def process_capital_gains(db, taxpayer_id):
    """
    [cite_start]Implements FIFO matching for capital gains [cite: 33-35].
    Returns: Net Capital Gain (Decimal).
    """
    # 1. Fetch all transactions ordered by date
    async with db.execute("""
        SELECT transaction_id, transaction_type, asset_id, quantity, unit_price, remaining_quantity 
        FROM asset_transactions 
        WHERE taxpayer_id = ? 
        ORDER BY transaction_date ASC, transaction_id ASC
    """, (taxpayer_id,)) as cursor:
        transactions = await cursor.fetchall()

    portfolio = {} # { asset_id: [list of BUY transactions] }
    realized_gains_total = Decimal("0")
    
    # 2. Iterate through history
    for txn in transactions:
        t_id, t_type, asset, qty, price, remaining = txn
        
        # FIX: Handle None for 'remaining' explicitly (SELLs have None)
        qty = Decimal(str(qty))
        price = Decimal(str(price))
        remaining = Decimal(str(remaining)) if remaining is not None else Decimal("0")
        
        if asset not in portfolio:
            portfolio[asset] = []

        if t_type == 'BUY':
            portfolio[asset].append({
                'id': t_id,
                'price': price,
                'remaining': qty # Use the full qty initially (which equals 'remaining' for new buys)
            })
            
        elif t_type == 'SELL':
            # FIFO Matching Logic
            shares_to_sell = qty
            sell_price = price
            
            while shares_to_sell > 0:
                # Find the earliest BUY with remaining shares
                valid_buys = [b for b in portfolio[asset] if b['remaining'] > 0]
                
                if not valid_buys:
                    # Should not happen in valid data, but handle gracefully
                    logging.warning(f"Overselling asset {asset} for {taxpayer_id}")
                    break
                    
                earliest_buy = valid_buys[0] # FIFO: Take the first one
                
                match_qty = min(shares_to_sell, earliest_buy['remaining'])
                
                # [cite_start]Calculate Gain: q * (p_sell - p_buy) [cite: 26-30]
                gain = match_qty * (sell_price - earliest_buy['price'])
                realized_gains_total += gain
                
                # Update State
                shares_to_sell -= match_qty
                earliest_buy['remaining'] -= match_qty
                
                # Record the match in DB for auditing
                await db.execute("""
                    INSERT INTO realized_gains (taxpayer_id, sell_transaction_id, buy_transaction_id, matched_quantity, gain_amount)
                    VALUES (?, ?, ?, ?, ?)
                """, (taxpayer_id, t_id, earliest_buy['id'], str(match_qty), str(gain)))
                
                # Update the BUY record in DB to persist state
                await db.execute("UPDATE asset_transactions SET remaining_quantity = ? WHERE transaction_id = ?", 
                                 (str(earliest_buy['remaining']), earliest_buy['id']))

    return realized_gains_total

# ---------------------------------------------------------
# STEP 2: Federal Tax Logic
# ---------------------------------------------------------

async def calculate_federal_tax(db, taxpayer_id, w2_income, net_capital_gain, ewma_income, num_children):
    """
    [cite_start]Computes Federal Tax Liability [cite: 11-98].
    """
    # 1. Gross Income
    gross_income = w2_income + net_capital_gain
    
    # 2. Deductions
    std_deduction = Decimal("10000")
    
    # Itemized: Charitable
    async with db.execute("SELECT amount FROM charitable_donations WHERE taxpayer_id = ?", (taxpayer_id,)) as cursor:
        rows = await cursor.fetchall()
        charitable_total = sum(Decimal(str(r[0])) for r in rows) # [cite: 57-58]
        
    # [cite_start]Itemized: Child Tax Deduction [cite: 63-70]
    child_rate = min(Decimal(num_children) * Decimal("0.01"), Decimal("0.10"))
    
    # Base for child deduction is Taxable Income *before* child deduction.
    # We interpret this as (Gross - Charitable) because Charitable is the only other itemized part.
    # Note: If Gross - Charitable < 0, base is 0.
    base_for_child = max(Decimal("0"), gross_income - charitable_total)
    child_deduction = base_for_child * child_rate
    
    itemized_total = charitable_total + child_deduction
    
    # [cite_start]Selection [cite: 75]
    if itemized_total > std_deduction:
        deduction_amount = itemized_total
        deduction_type = 'ITEMIZED'
    else:
        deduction_amount = std_deduction
        deduction_type = 'STANDARD'
        
    # [cite_start]3. Taxable Income [cite: 74]
    taxable_income = max(Decimal("0"), gross_income - deduction_amount)
    
    # [cite_start]4. Progressive Brackets [cite: 77]
    fed_brackets = [
        (Decimal("100000"), Decimal("0.05")),
        (Decimal("200000"), Decimal("0.10")),
        (Decimal("300000"), Decimal("0.15")),
        (Decimal("Infinity"), Decimal("0.20"))
    ]
    bracket_tax = calculate_bracket_tax(taxable_income, fed_brackets)
    
    # [cite_start]5. High-Income Surcharge [cite: 91-94]
    surcharge = Decimal("0")
    if ewma_income > Decimal("1000000"):
        surcharge = gross_income * Decimal("0.02")
        
    total_federal = bracket_tax + surcharge
    
    return {
        "gross": gross_income,
        "taxable": taxable_income,
        "deduction_type": deduction_type,
        "surcharge": surcharge,
        "total": total_federal
    }

# ---------------------------------------------------------
# STEP 3: State Tax Logic
# ---------------------------------------------------------

def calculate_state_tax(state, w2_income, net_capital_gain, fed_data, fed_deduction_amount):
    """
    [cite_start]Computes State Tax Liability [cite: 105-121].
    """
    state_tax = Decimal("0")
    state_surcharge = Decimal("0")
    
    if state == 'California':
        # [cite_start]CA Tax = 0.04 * Wage + 0.06 * max(0, NetCapGain) [cite: 108]
        ca_gains_tax = Decimal("0.06") * max(Decimal("0"), net_capital_gain)
        ca_wage_tax = Decimal("0.04") * w2_income
        base_tax = ca_wage_tax + ca_gains_tax
        
        # [cite_start]CA Surcharge: 5% of state tax if Federal Surcharge applies [cite: 113]
        if fed_data['surcharge'] > 0:
            state_surcharge = base_tax * Decimal("0.05")
            
        state_tax = base_tax + state_surcharge
        
    elif state == 'Texas':
        # [cite_start]TX uses Federal Taxable Income [cite: 114]
        # [cite_start]Brackets: 0-90k (3%), 90-200k (5%), >200k (7%) [cite: 116]
        tx_brackets = [
            (Decimal("90000"), Decimal("0.03")),
            (Decimal("200000"), Decimal("0.05")),
            (Decimal("Infinity"), Decimal("0.07"))
        ]
        state_tax = calculate_bracket_tax(fed_data['taxable'], tx_brackets)
        
        # [cite_start]Deduction: If Fed Deductions > 15,000, reduce state tax by 1% [cite: 120]
        if fed_deduction_amount > Decimal("15000"):
            state_tax = state_tax * Decimal("0.99")
            
    return state_tax, state_surcharge

# ---------------------------------------------------------
# MAIN PROCESSOR LOOP
# ---------------------------------------------------------

async def process_taxpayer(db, taxpayer_id, state, w2_income, ewma_income, num_children):
    try:
        w2 = Decimal(str(w2_income))
        ewma = Decimal(str(ewma_income))
        
        # 1. Calculate Capital Gains
        net_capital_gain = await process_capital_gains(db, taxpayer_id)
        
        # 2. Calculate Federal Tax
        fed_res = await calculate_federal_tax(db, taxpayer_id, w2, net_capital_gain, ewma, num_children)
        
        # Recover deduction amount for State logic
        deduction_amt = fed_res['gross'] - fed_res['taxable']
        
        # 3. Calculate State Tax
        state_tax_raw, state_surcharge = calculate_state_tax(state, w2, net_capital_gain, fed_res, deduction_amt)
        
        # [cite_start]4. Rounding & Persisting [cite: 127]
        total_fed_rounded = round_dollar(fed_res['total'])
        total_state_rounded = round_dollar(state_tax_raw)
        
        # Save to tax_computations
        await db.execute("""
            INSERT OR REPLACE INTO tax_computations 
            (taxpayer_id, federal_gross_income, federal_taxable_income, federal_deduction_type, 
             federal_surcharge, total_federal_tax, state_surcharge, total_state_tax)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            taxpayer_id, 
            str(fed_res['gross']), 
            str(fed_res['taxable']), 
            fed_res['deduction_type'],
            str(fed_res['surcharge']),
            str(total_fed_rounded),
            str(state_surcharge),
            str(total_state_rounded)
        ))
        
        # Mark as completed
        await db.execute("UPDATE taxpayers SET processing_status = 'COMPLETED' WHERE taxpayer_id = ?", (taxpayer_id,))
        logging.info(f"Processed {taxpayer_id}: Fed={total_fed_rounded}, State={total_state_rounded}")
        
    except Exception as e:
        # Log exact error types for easier debugging
        logging.error(f"Failed to process {taxpayer_id}: {type(e)} {e}")
        await db.execute("UPDATE taxpayers SET processing_status = 'ERROR' WHERE taxpayer_id = ?", (taxpayer_id,))

async def run_analysis_engine():
    """Main loop fetching pending records."""
    print("Starting Analysis Engine...")
    async with aiosqlite.connect(DB_PATH) as db:
        while True:
            # Fetch a batch of pending users
            async with db.execute("SELECT taxpayer_id, state, w2_income, ewma_income, num_children FROM taxpayers WHERE processing_status = 'PENDING' LIMIT 100") as cursor:
                batch = await cursor.fetchall()
                
            if not batch:
                print("No pending records found. Analysis complete.")
                break
                
            for row in batch:
                # row: (id, state, w2, ewma, children)
                await process_taxpayer(db, row[0], row[1], row[2], row[3], row[4])
            
            await db.commit()

if __name__ == "__main__":
    asyncio.run(run_analysis_engine())