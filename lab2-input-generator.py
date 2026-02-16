#!/usr/bin/env python3
"""
Generate NDJSON test inputs for the Avalon tax-engine specification.

Outputs (always produced):
  - smoke-test.ndjson
  - full-test.ndjson

Optional args:
  --nSmoke N   (default 100)
  --nFull  N   (default 10000)
"""

from __future__ import annotations

import argparse
import json
import random
import string
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Tuple, Sequence


# ============================================================
# Parameters (edit here to match Appendix A + test design goals)
# ============================================================

# --- Enumerations ---
STATES = ["California", "Texas"]
ASSETS = [
    "Ananyas Wool Whimsy",
    "Mikos Coal Collaborative",
    "Estebans Timberfell",
]

# --- Spec thresholds (used for stratification) ---
BRACKET_1 = 100_000
BRACKET_2 = 200_000
BRACKET_3 = 300_000
TEX_DED_TRIGGER = 15_000
EWMA_THRESHOLD = 1_000_000
EWMA_ALPHA = 0.6  # not used by generator, but useful as a reminder

FED_STD_DEDUCTION = 10_000  # also just a reminder; generator doesn't compute tax

# --- Smoke-test constraints (per your request) ---
SMOKE_MAX_W2 = 170_000
SMOKE_MAX_CHILDREN = 1
SMOKE_MAX_TRADES_TOTAL = 1  # at most one sale event (and one purchase lot)
SMOKE_AVOID_SURCHARGE = True

# --- Full-test ranges / knobs ---
FULL_CHILDREN_RANGE = (0, 10)
FULL_NUM_DONATIONS_RANGE = (0, 18)
FULL_DONATION_AMOUNT_RANGE = (0.0, 60_000.0)  # per donation
FULL_PRIOR_YEAR_INCOME_RANGE = (0.0, 3_000_000.0)

# Investment generation (full)
FULL_NUM_BUYS_RANGE = (0, 25)
FULL_NUM_SELLS_RANGE = (0, 25)
FULL_QUANTITY_RANGE = (0.001, 2500.0)     # shares
FULL_UNIT_PRICE_RANGE = (0.01, 5000.0)    # dollars per share

# --- Same-day clustering knobs (full test) ---
FULL_SAME_DAY_CLUSTER_PROB = 0.10   # probability we force a clustered day scenario
FULL_SAME_DAY_DAYS_RANGE = (1, 3)   # number of distinct days used when clustered
FULL_SAME_DAY_MIN_BUYS = 3
FULL_SAME_DAY_MIN_SELLS = 3
FULL_SAME_DAY_MAX_BUYS = 12
FULL_SAME_DAY_MAX_SELLS = 12

# Date generation
DATE_START = date(2024, 1, 1)
DATE_END = date(2026, 12, 31)

# Numeric formatting: transactions to 3 decimal places
TX_DECIMALS = 3

# --------------------------
# Stratified bucket settings
# --------------------------

# W2 income buckets to exercise bracket boundaries.
# Choose a bucket uniformly, then sample uniformly within.
FULL_W2_BUCKETS: Sequence[Tuple[float, float]] = [
    (0, 25_000),
    (25_000, 75_000),
    (75_000, BRACKET_1 - 1),
    (BRACKET_1 - 500, BRACKET_1 + 500),        # boundary focus
    (BRACKET_1 + 1, 150_000),
    (150_000, BRACKET_2 - 1),
    (BRACKET_2 - 500, BRACKET_2 + 500),        # boundary focus
    (BRACKET_2 + 1, 250_000),
    (250_000, BRACKET_3 - 1),
    (BRACKET_3 - 500, BRACKET_3 + 500),        # boundary focus
    (BRACKET_3 + 1, 500_000),
    (500_000, 1_000_000),
    (1_000_000, 2_500_000),
]

# Prior-year income buckets to ensure EWMA threshold is frequently exercised.
FULL_PRIOR_YEAR_BUCKETS: Sequence[Tuple[float, float]] = [
    (0, 100_000),
    (100_000, 400_000),
    (400_000, 800_000),
    (800_000, EWMA_THRESHOLD - 1),
    (EWMA_THRESHOLD - 2_000, EWMA_THRESHOLD + 2_000),  # tight around threshold
    (EWMA_THRESHOLD + 1, 1_300_000),
    (1_300_000, 2_000_000),
    (2_000_000, 3_000_000),
]

# Donation *total* buckets to ensure “deductions > 15k” is hit often.
# We generate a target total from a bucket, then split into a random list.
FULL_DONATION_TOTAL_BUCKETS: Sequence[Tuple[float, float]] = [
    (0, 0),                        # many zeros
    (1, 500),
    (500, 5_000),
    (5_000, TEX_DED_TRIGGER - 500),
    (TEX_DED_TRIGGER - 500, TEX_DED_TRIGGER + 500),  # around trigger
    (TEX_DED_TRIGGER + 500, 50_000),
    (50_000, 150_000),
]

# For the “0 total” bucket above, we want it to occur but not dominate.
# Implement via a simple “bucket list with repeats” trick.
FULL_DONATION_BUCKET_INDEX_POOL: List[int] = (
    [0] * 3 +   # zero bucket appears 3x
    [1, 2, 3, 4, 5, 6]  # others appear once
)


# =========================
# Utility / helper functions
# =========================

def _rand_dates_iso(k: int) -> List[str]:
    return [_rand_date_iso() for _ in range(k)]

def _choose_cluster_date(cluster_dates: List[str]) -> str:
    return random.choice(cluster_dates)

def _rand_id(prefix: str, n: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return prefix + "".join(random.choice(alphabet) for _ in range(n))

def _rand_date_iso() -> str:
    delta_days = (DATE_END - DATE_START).days
    d = DATE_START + timedelta(days=random.randint(0, delta_days))
    return d.isoformat()

def _round_tx(x: float) -> float:
    return round(x, TX_DECIMALS)

def _sample_from_buckets(buckets: Sequence[Tuple[float, float]], decimals: int = 2) -> float:
    lo, hi = random.choice(buckets)
    if lo == hi:
        return round(lo, decimals)
    return round(random.uniform(lo, hi), decimals)

def _choose_state() -> str:
    return random.choice(STATES)

def _choose_asset() -> str:
    return random.choice(ASSETS)

def _nonneg_int(lo: int, hi: int) -> int:
    return random.randint(lo, hi)

@dataclass
class Lot:
    date_iso: str
    qty_remaining: float
    unit_price: float

def _fifo_sell(lots: List[Lot], sell_qty: float) -> None:
    """
    Consume sell_qty from lots FIFO by date order.
    This is only to ensure generated inputs do not oversell.
    """
    lots.sort(key=lambda l: l.date_iso)
    remaining = sell_qty
    for lot in lots:
        if remaining <= 0:
            break
        take = min(lot.qty_remaining, remaining)
        lot.qty_remaining = _round_tx(lot.qty_remaining - take)
        remaining = _round_tx(remaining - take)
    if remaining > 0:
        raise ValueError("Generator bug: oversell detected.")

def _generate_prior_five_years_income(smoke: bool) -> List[float]:
    # Ordered oldest -> most recent.
    if smoke and SMOKE_AVOID_SURCHARGE:
        # Keep low to avoid triggering surcharge, but still varied.
        return [round(random.uniform(0, 250_000), 2) for _ in range(5)]
    # Full: stratified per-year buckets around the EWMA threshold.
    return [_sample_from_buckets(FULL_PRIOR_YEAR_BUCKETS, decimals=2) for _ in range(5)]

def _split_total_into_parts(total: float, max_parts: int) -> List[float]:
    """
    Split 'total' into 0..max_parts positive amounts summing to total (within cents).
    """
    if total <= 0:
        return []
    k = random.randint(1, max(1, max_parts))
    # Generate k-1 cut points in (0,total), sort, take differences.
    cuts = sorted([random.random() for _ in range(k - 1)])
    parts = []
    prev = 0.0
    for c in cuts + [1.0]:
        frac = c - prev
        parts.append(frac)
        prev = c
    amounts = [round(total * p, 2) for p in parts]
    # Fix rounding drift to ensure exact total to cents.
    drift = round(total - sum(amounts), 2)
    amounts[-1] = round(amounts[-1] + drift, 2)
    # Guard against tiny negative due to rounding correction
    amounts = [a for a in amounts if a > 0]
    return amounts

def _generate_charitable_donations(smoke: bool) -> List[float]:
    if smoke:
        k = random.randint(0, 2)
        return [round(random.uniform(0, 2000), 2) for _ in range(k)]
    # Full: choose a donation-total bucket, then split into a list
    bucket_idx = random.choice(FULL_DONATION_BUCKET_INDEX_POOL)
    lo, hi = FULL_DONATION_TOTAL_BUCKETS[bucket_idx]
    total = round(lo if lo == hi else random.uniform(lo, hi), 2)
    max_parts = _nonneg_int(*FULL_NUM_DONATIONS_RANGE)
    return _split_total_into_parts(total, max_parts=max_parts)

def _generate_investments(smoke: bool) -> Tuple[List[dict], List[dict]]:
    purchases: List[dict] = []
    sales: List[dict] = []

    if smoke:
        do_trade = (random.random() < 0.35)
        if not do_trade:
            return purchases, sales

        asset = _choose_asset()
        buy_qty = _round_tx(random.uniform(0.1, 20.0))
        buy_price = _round_tx(random.uniform(1.0, 500.0))
        purchases.append({
            "asset_id": asset,
            "date": _rand_date_iso(),
            "quantity": buy_qty,
            "unit_price": buy_price,
        })

        sell_qty = _round_tx(random.uniform(0.1, float(buy_qty)))
        sell_price = _round_tx(random.uniform(1.0, 500.0))
        sales.append({
            "asset_id": asset,
            "date": _rand_date_iso(),
            "quantity": sell_qty,
            "unit_price": sell_price,
        })
        return purchases, sales

    # Full: many buys/sells, across assets, avoid oversell by construction.
    #       optionally force multiple buys/sells to share the same day(s)
    clustered = (random.random() < FULL_SAME_DAY_CLUSTER_PROB)

    if clustered:
        num_buys = random.randint(FULL_SAME_DAY_MIN_BUYS, FULL_SAME_DAY_MAX_BUYS)
        num_sells = random.randint(FULL_SAME_DAY_MIN_SELLS, FULL_SAME_DAY_MAX_SELLS)
        cluster_days = random.randint(*FULL_SAME_DAY_DAYS_RANGE)
        cluster_dates = _rand_dates_iso(cluster_days)
    else:
        num_buys = _nonneg_int(*FULL_NUM_BUYS_RANGE)
        num_sells = _nonneg_int(*FULL_NUM_SELLS_RANGE)
        cluster_dates = []  # unused

    lots_by_asset: Dict[str, List[Lot]] = {a: [] for a in ASSETS}

    # Generate purchases
    for _ in range(num_buys):
        asset = _choose_asset()
        qty = _round_tx(random.uniform(*FULL_QUANTITY_RANGE))
        price = _round_tx(random.uniform(*FULL_UNIT_PRICE_RANGE))
        d = _choose_cluster_date(cluster_dates) if clustered else _rand_date_iso()

        purchases.append({
            "asset_id": asset,
            "date": d,
            "quantity": qty,
            "unit_price": price,
        })
        lots_by_asset[asset].append(Lot(date_iso=d, qty_remaining=qty, unit_price=price))

    # Compute max sellable per asset
    sellable_by_asset = {a: _round_tx(sum(l.qty_remaining for l in lots)) for a, lots in lots_by_asset.items()}
    assets_with_inventory = [a for a, q in sellable_by_asset.items() if q > 0]

    # Generate sales
    for _ in range(num_sells):
        if not assets_with_inventory:
            break
        asset = random.choice(assets_with_inventory)
        max_qty = float(sellable_by_asset[asset])

        qty = _round_tx(random.uniform(0.001, max_qty))
        price = _round_tx(random.uniform(*FULL_UNIT_PRICE_RANGE))
        d = _choose_cluster_date(cluster_dates) if clustered else _rand_date_iso()

        sales.append({
            "asset_id": asset,
            "date": d,
            "quantity": qty,
            "unit_price": price,
        })

        # Consume inventory so we never oversell
        _fifo_sell(lots_by_asset[asset], qty)
        sellable_by_asset[asset] = _round_tx(sum(l.qty_remaining for l in lots_by_asset[asset]))
        assets_with_inventory = [a for a, q in sellable_by_asset.items() if q > 0]
    num_buys = _nonneg_int(*FULL_NUM_BUYS_RANGE)
    num_sells = _nonneg_int(*FULL_NUM_SELLS_RANGE)

    lots_by_asset: Dict[str, List[Lot]] = {a: [] for a in ASSETS}

    for _ in range(num_buys):
        asset = _choose_asset()
        qty = _round_tx(random.uniform(*FULL_QUANTITY_RANGE))
        price = _round_tx(random.uniform(*FULL_UNIT_PRICE_RANGE))
        d = _rand_date_iso()
        purchases.append({
            "asset_id": asset,
            "date": d,
            "quantity": qty,
            "unit_price": price,
        })
        lots_by_asset[asset].append(Lot(date_iso=d, qty_remaining=qty, unit_price=price))

    sellable_by_asset = {a: _round_tx(sum(l.qty_remaining for l in lots)) for a, lots in lots_by_asset.items()}
    assets_with_inventory = [a for a, q in sellable_by_asset.items() if q > 0]

    for _ in range(num_sells):
        if not assets_with_inventory:
            break
        asset = random.choice(assets_with_inventory)
        max_qty = float(sellable_by_asset[asset])
        qty = _round_tx(random.uniform(0.001, max_qty))
        price = _round_tx(random.uniform(*FULL_UNIT_PRICE_RANGE))
        d = _rand_date_iso()
        sales.append({
            "asset_id": asset,
            "date": d,
            "quantity": qty,
            "unit_price": price,
        })
        _fifo_sell(lots_by_asset[asset], qty)
        sellable_by_asset[asset] = _round_tx(sum(l.qty_remaining for l in lots_by_asset[asset]))
        assets_with_inventory = [a for a, q in sellable_by_asset.items() if q > 0]

    return purchases, sales

def _generate_household(smoke: bool, idx: int) -> dict:
    taxpayer_id = f"H{idx:06d}-{_rand_id('', 6)}"
    state = _choose_state()

    if smoke:
        w2_income = round(random.uniform(0, SMOKE_MAX_W2), 2)
        num_children = random.randint(0, SMOKE_MAX_CHILDREN)
    else:
        w2_income = _sample_from_buckets(FULL_W2_BUCKETS, decimals=2)
        num_children = random.randint(*FULL_CHILDREN_RANGE)

    prior = _generate_prior_five_years_income(smoke=smoke)
    donations = _generate_charitable_donations(smoke=smoke)
    purchases, sales = _generate_investments(smoke=smoke)

    return {
        "taxpayer_id": taxpayer_id,
        "state": state,
        "w2_income": w2_income,
        "num_children": num_children,
        "prior_five_years_income": prior,   # [Y_{t-5},...,Y_{t-1}]
        "purchases": purchases,
        "sales": sales,
        "charitable_donations": donations,
    }

def _write_ndjson(path: str, objs: List[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for obj in objs:
            f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate NDJSON test inputs for the Avalon tax-engine lab.")
    ap.add_argument("--nSmoke", type=int, default=100, help="Number of smoke-test households (default 100).")
    ap.add_argument("--nFull", type=int, default=10000, help="Number of full-test households (default 10000).")
    args = ap.parse_args()

    smoke = [_generate_household(smoke=True, idx=i + 1) for i in range(args.nSmoke)]
    _write_ndjson("lab2-smoke-test-cases.ndjson", smoke)

    full = [_generate_household(smoke=False, idx=i + 1) for i in range(args.nFull)]
    _write_ndjson("lab2-full-test-cases.ndjson", full)

    print(f"Wrote smoke-test.ndjson ({args.nSmoke} lines)")
    print(f"Wrote full-test.ndjson  ({args.nFull} lines)")

if __name__ == "__main__":
    main()
