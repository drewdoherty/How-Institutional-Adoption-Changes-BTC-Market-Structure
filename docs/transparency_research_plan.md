# Transparency Shift Research Plan

## Hypothesis
As institutional adoption grows (ETFs/ETPs, DATs, OTC and structured wrappers), BTC spot price discovery becomes less transparent to retail traders than in periods dominated by CEX + on-chain observability.

## Regime Split
- Pre institutional-wrapper regime: before `2024-01-11` (US spot BTC ETF launch)
- Post institutional-wrapper regime: `2024-01-11` onward

## Core Questions
1. Did ETF flow variables become more explanatory for next-day BTC returns than in pre-ETF regimes?
2. Did return/volatility behavior shift as wrapper share of BTC supply/market cap increased?
3. Is public CEX flow/volume now a weaker standalone signal unless combined with ETF data?

## Data Stack (free / public)
- CoinGecko: BTC daily price (`btc_price_usd.csv`)
- Farside (via script): IBIT daily net flows (`ibit_flow_usd.csv`)
- Dune (API key or manual export): ETF aggregate flow, ETF holdings, CEX spot volume
- DefiLlama (manual export): ETP share of market cap, DAT share of BTC supply
- Manual image claims (optional): key points pulled from screenshots/posts

## Output Artifacts
- `reports/transparency_shift/<timestamp>/dataset_merged.csv`
- `reports/transparency_shift/<timestamp>/regime_stats.json`
- `reports/transparency_shift/<timestamp>/summary.md`
- `reports/transparency_shift/<timestamp>/price_vs_wrapper_share.png`
- `reports/transparency_shift/<timestamp>/flow_vs_nextday_scatter.png` (if flow present)
- `reports/transparency_shift/<timestamp>/rolling_flow_return_corr.png` (if flow present)
- `reports/transparency_shift/<timestamp>/regime_comparison_bars.png` (if regime stats present)

## Caveats
- Dune query schema can vary by query; verify date/value column names in raw exports.
- DefiLlama anti-bot can block CLI scraping; manual CSV export is expected in this workflow.
- OTC and bespoke derivative positioning remains partially unobservable, so this is a proxy framework, not a full market microstructure model.
