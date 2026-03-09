#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from hv_btc_vault.ibit_fetcher import fetch_ibit_flows_usd

HEADERS = {"User-Agent": "btc-market-structure-research/0.1"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch/prepare market-structure data (CoinGecko, Farside, optional Dune API, manual DefiLlama CSVs)"
    )
    p.add_argument("--out-dir", default="data/market_structure")
    p.add_argument("--btc-days", type=int, default=3650)
    p.add_argument("--dune-api-key-env", default="DUNE_API_KEY")
    p.add_argument(
        "--dune-query",
        action="append",
        default=[],
        help="Dune query mapping: alias=query_id (repeatable). Example: --dune-query etf_total_flow_usd=4352717",
    )
    return p.parse_args()


def _coingecko_rows(days: int) -> list[dict[str, object]]:
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "daily"}
    headers = dict(HEADERS)
    api_key = os.getenv("COINGECKO_API_KEY", "").strip()
    if api_key:
        headers["x-cg-demo-api-key"] = api_key
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    rows: list[dict[str, object]] = []
    for ts_ms, px in payload.get("prices", []):
        dt = pd.to_datetime(ts_ms, unit="ms", utc=True).date().isoformat()
        rows.append({"date": dt, "btc_close_usd": float(px)})
    return rows


def _cryptocompare_rows(days: int) -> list[dict[str, object]]:
    limit = min(max(days, 30), 2000)
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {"fsym": "BTC", "tsym": "USD", "limit": limit}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    data = payload.get("Data", {}).get("Data", [])
    rows: list[dict[str, object]] = []
    for x in data:
        ts = int(x.get("time", 0))
        close = x.get("close")
        if not ts or close is None:
            continue
        dt = pd.to_datetime(ts, unit="s", utc=True).date().isoformat()
        rows.append({"date": dt, "btc_close_usd": float(close)})
    return rows


def _yahoo_rows(days: int) -> list[dict[str, object]]:
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=days + 2)
    url = "https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD"
    params = {"period1": int(start.timestamp()), "period2": int(end.timestamp()), "interval": "1d"}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("chart", {}).get("result", [])
    if not result:
        return []
    timestamps = result[0].get("timestamp", [])
    closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])

    rows: list[dict[str, object]] = []
    for ts, px in zip(timestamps, closes):
        if px is None:
            continue
        dt = pd.to_datetime(ts, unit="s", utc=True).date().isoformat()
        rows.append({"date": dt, "btc_close_usd": float(px)})
    return rows


def _fetch_btc_prices(days: int) -> pd.DataFrame:
    errors: list[str] = []
    rows: list[dict[str, object]] = []
    for name, fn in [
        ("coingecko", _coingecko_rows),
        ("cryptocompare", _cryptocompare_rows),
        ("yahoo", _yahoo_rows),
    ]:
        try:
            rows = fn(days)
            if rows:
                break
            errors.append(f"{name}: empty")
        except Exception as exc:
            errors.append(f"{name}: {exc}")

    if not rows:
        raise RuntimeError("Failed to fetch BTC prices from all sources: " + " | ".join(errors))

    df = pd.DataFrame(rows).drop_duplicates("date", keep="last").sort_values("date")
    return df


def _parse_date_col(df: pd.DataFrame) -> str | None:
    candidates = ["date", "day", "dt", "timestamp", "time", "block_date"]
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    return None


def _parse_value_col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in aliases:
        if c in lower:
            return lower[c]

    # Fallback: first numeric non-date column.
    date_col = _parse_date_col(df)
    for c in df.columns:
        if c == date_col:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            return c
    return None


def _normalize_dune(alias: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "etf_total_flow_usd": ["net_flow_usd", "netflow_usd", "flow_usd", "net_flow", "value"],
        "etf_holdings_btc": ["holdings_btc", "btc_holdings", "balance_btc", "btc", "value"],
        "cex_spot_volume_usd": ["volume_usd", "spot_volume_usd", "volume", "value"],
    }
    value_aliases = mapping.get(alias, ["value"])

    date_col = _parse_date_col(raw_df)
    value_col = _parse_value_col(raw_df, value_aliases)
    if date_col is None or value_col is None:
        raise RuntimeError(f"Could not detect date/value columns for alias={alias}. Columns={list(raw_df.columns)}")

    out = raw_df[[date_col, value_col]].copy()
    out.columns = ["date", alias]
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    out[alias] = pd.to_numeric(out[alias], errors="coerce")
    out = out.dropna(subset=["date", alias]).drop_duplicates("date", keep="last").sort_values("date")
    return out


def _fetch_dune_csv(query_id: str, api_key: str) -> pd.DataFrame:
    url = f"https://api.dune.com/api/v1/query/{query_id}/results/csv"
    resp = requests.get(url, headers={"X-Dune-API-Key": api_key, **HEADERS}, params={"limit": 100000}, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def _write_template_if_missing(path: Path, header: str) -> None:
    if path.exists():
        return
    path.write_text(header + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, object] = {"out_dir": str(out_dir), "written": {}, "warnings": []}

    btc_df = _fetch_btc_prices(days=args.btc_days)
    btc_path = out_dir / "btc_price_usd.csv"
    btc_df.to_csv(btc_path, index=False)
    summary["written"] = {**summary["written"], "btc_price_usd": str(btc_path)}

    ibit_df = fetch_ibit_flows_usd().rename(columns={"net_flow_usd": "ibit_net_flow_usd"})
    ibit_path = out_dir / "ibit_flow_usd.csv"
    ibit_df.to_csv(ibit_path, index=False)
    summary["written"] = {**summary["written"], "ibit_flow_usd": str(ibit_path)}

    dune_api_key = os.getenv(args.dune_api_key_env, "").strip()
    if args.dune_query and not dune_api_key:
        summary["warnings"].append(
            f"Dune query ids provided but {args.dune_api_key_env} is missing; skipping Dune API pulls."
        )

    for item in args.dune_query:
        if "=" not in item:
            summary["warnings"].append(f"Invalid --dune-query format: {item} (expected alias=query_id)")
            continue
        alias, query_id = [x.strip() for x in item.split("=", 1)]
        if not alias or not query_id:
            summary["warnings"].append(f"Invalid --dune-query format: {item}")
            continue
        if not dune_api_key:
            continue

        try:
            raw = _fetch_dune_csv(query_id=query_id, api_key=dune_api_key)
            raw_path = out_dir / f"dune_{alias}_raw.csv"
            raw.to_csv(raw_path, index=False)

            norm = _normalize_dune(alias=alias, raw_df=raw)
            norm_path = out_dir / f"{alias}.csv"
            norm.to_csv(norm_path, index=False)
            summary["written"] = {
                **summary["written"],
                f"dune_{alias}_raw": str(raw_path),
                alias: str(norm_path),
            }
        except Exception as exc:
            summary["warnings"].append(f"Dune fetch failed for alias={alias}, query_id={query_id}: {exc}")

    # DefiLlama data commonly requires browser export due anti-bot protections.
    _write_template_if_missing(out_dir / "defillama_etp_share_pct.csv", "date,etp_share_pct")
    _write_template_if_missing(out_dir / "defillama_dat_share_pct.csv", "date,dat_share_pct")
    _write_template_if_missing(
        out_dir / "manual_image_claims.csv",
        "date,metric,value,source,notes",
    )

    print(pd.Series(summary).to_json(indent=2))


if __name__ == "__main__":
    main()
