# How Institutional Adoption Changes BTC Market Structure

## Project Description

CEX + On-Chain data used to be extremely powerful for those who knew how to use it in cycles gone by.  
As Institutional adoption grew and risk appetites evolved, the notional exposure exploded with it, and naturally the moves in spot price were influenced by players becoming more opaque over time, through OTC Derivatives, Swaps and Notes written on hazy balance sheets.

This repo is focused on one question:

> Has BTC price discovery become less transparent for retail traders as institutional wrappers (ETFs/ETPs, DATs, OTC-linked products) gained share?

## What this toolkit does

1. Pulls base public data automatically:
- CoinGecko BTC daily price
- Farside IBIT daily net flows
- Optional Dune query pulls (if you set `DUNE_API_KEY`)

2. Accepts manual exports where anti-bot controls are common:
- DefiLlama ETP share (% of market cap)
- DefiLlama DAT share (% of BTC supply)

3. Builds a regime comparison (pre/post US spot ETF launch) with charts and summary stats.

Default split date: `2024-01-11`.

## Setup

```bash
cd "/Users/andrewdoherty/Desktop/Coding/Crypto Trading/How-Institutional-Adoption-Changes-BTC-Market-Structure"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
cp .env.example .env
```

## Step 1: Fetch data

Base pull (CoinGecko + Farside):

```bash
PYTHONPATH=src python ./scripts/fetch_transparency_data.py --out-dir data/market_structure
```

Optional Dune pull (requires API key):

```bash
export DUNE_API_KEY=your_key_here
PYTHONPATH=src python ./scripts/fetch_transparency_data.py \
  --out-dir data/market_structure \
  --dune-query etf_total_flow_usd=4352717 \
  --dune-query etf_holdings_btc=4872748 \
  --dune-query cex_spot_volume_usd=4430641
```

## Step 2: Add manual DefiLlama exports

Populate these files in `data/market_structure/`:
- `defillama_etp_share_pct.csv` with columns: `date,etp_share_pct`
- `defillama_dat_share_pct.csv` with columns: `date,dat_share_pct`

If you want to encode screenshot claims manually:
- `manual_image_claims.csv` with columns: `date,metric,value,source,notes`

## Step 3: Run analysis + visuals

```bash
PYTHONPATH=src python ./scripts/analyze_transparency_shift.py \
  --data-dir data/market_structure \
  --out-root reports/transparency_shift \
  --split-date 2024-01-11
```

Outputs are written to:
- `reports/transparency_shift/<timestamp>/dataset_merged.csv`
- `reports/transparency_shift/<timestamp>/regime_stats.json`
- `reports/transparency_shift/<timestamp>/summary.md`
- `reports/transparency_shift/<timestamp>/price_vs_wrapper_share.png`
- `reports/transparency_shift/<timestamp>/flow_vs_nextday_scatter.png` (if flow data exists)
- `reports/transparency_shift/<timestamp>/rolling_flow_return_corr.png` (if flow data exists)
- `reports/transparency_shift/<timestamp>/regime_comparison_bars.png` (if regime stats exist)

## Notebook inspection

Open and run:
- `notebooks/transparency_shift_analysis.ipynb`

## Source notes

- CoinGecko and Farside are fetched automatically.
- Dune requires an API key for programmatic access.
- DefiLlama pages may block CLI scraping; use manual CSV export into `data/market_structure`.

## Research framing

See:
- `docs/transparency_research_plan.md`
