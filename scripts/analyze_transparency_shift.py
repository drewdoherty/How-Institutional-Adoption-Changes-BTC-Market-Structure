#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from hv_btc_vault.transparency_metrics import compute_inst_share_pct, compute_regime_stats, rolling_corr


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze BTC market-structure transparency shift")
    p.add_argument("--data-dir", default="data/market_structure")
    p.add_argument("--out-root", default="reports/transparency_shift")
    p.add_argument("--split-date", default="2024-01-11", help="US spot BTC ETF launch date by default")
    p.add_argument("--rolling-window", type=int, default=60)
    return p.parse_args()


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_optional_series(
    path: Path,
    value_col_candidates: list[str],
) -> pd.DataFrame | None:
    if not path.exists():
        return None

    df = pd.read_csv(path)
    if df.empty:
        return None

    lower_map = {str(c).strip().lower(): c for c in df.columns}
    date_col = None
    for cand in ["date", "day", "dt", "timestamp", "time", "block_date"]:
        if cand in lower_map:
            date_col = lower_map[cand]
            break
    if date_col is None:
        return None

    value_col = None
    for cand in value_col_candidates:
        if cand.lower() in lower_map:
            value_col = lower_map[cand.lower()]
            break
    if value_col is None:
        for c in df.columns:
            if c == date_col:
                continue
            if pd.api.types.is_numeric_dtype(df[c]):
                value_col = c
                break
    if value_col is None:
        return None

    out = df[[date_col, value_col]].copy()
    out.columns = ["date", value_col_candidates[0]]
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    out[value_col_candidates[0]] = pd.to_numeric(out[value_col_candidates[0]], errors="coerce")
    out = out.dropna(subset=["date", value_col_candidates[0]]).drop_duplicates("date", keep="last")
    return out.sort_values("date")


def load_dataset(data_dir: Path) -> tuple[pd.DataFrame, list[str]]:
    files = [
        ("btc_price_usd.csv", ["btc_close_usd", "close", "price"]),
        ("ibit_flow_usd.csv", ["ibit_net_flow_usd", "net_flow_usd", "flow_usd"]),
        ("etf_total_flow_usd.csv", ["etf_total_flow_usd", "net_flow_usd", "flow_usd"]),
        ("etf_holdings_btc.csv", ["etf_holdings_btc", "holdings_btc", "btc_holdings"]),
        ("cex_spot_volume_usd.csv", ["cex_spot_volume_usd", "volume_usd", "spot_volume_usd"]),
        ("defillama_etp_share_pct.csv", ["etp_share_pct"]),
        ("defillama_dat_share_pct.csv", ["dat_share_pct"]),
    ]

    merged: pd.DataFrame | None = None
    loaded_names: list[str] = []
    for filename, candidates in files:
        frame = _load_optional_series(data_dir / filename, value_col_candidates=candidates)
        if frame is None:
            continue
        loaded_names.append(filename)
        if merged is None:
            merged = frame.copy()
        else:
            merged = merged.merge(frame, on="date", how="outer")

    if merged is None or "btc_close_usd" not in merged.columns:
        raise RuntimeError(
            "btc_price_usd.csv with price column is required. Run scripts/fetch_transparency_data.py first."
        )

    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.dropna(subset=["date"]).drop_duplicates("date", keep="last").sort_values("date")
    return merged.reset_index(drop=True), loaded_names


def _choose_flow_col(df: pd.DataFrame) -> str | None:
    if "etf_total_flow_usd" in df.columns:
        return "etf_total_flow_usd"
    if "ibit_net_flow_usd" in df.columns:
        return "ibit_net_flow_usd"
    return None


def _plot_price_and_share(df: pd.DataFrame, out: Path) -> None:
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(df["date"], df["btc_close_usd"], color="#2ca8ff", label="BTC close (USD)")
    ax1.set_ylabel("BTC close (USD)")
    ax1.grid(alpha=0.2)

    share = compute_inst_share_pct(df)
    if share.notna().any():
        ax2 = ax1.twinx()
        ax2.plot(df["date"], share, color="#ffb020", label="Inst wrapper share (%)")
        ax2.set_ylabel("Institutional wrapper share (%)")
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc="upper left")
    else:
        ax1.legend(loc="upper left")

    ax1.set_title("BTC Price vs Institutional Wrapper Share")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def _plot_flow_scatter(df: pd.DataFrame, flow_col: str, split_date: str, out: Path) -> None:
    data = df[[flow_col, "btc_ret_1d_fwd", "date"]].dropna()
    split_dt = pd.to_datetime(split_date)
    pre = data[data["date"] < split_dt]
    post = data[data["date"] >= split_dt]

    fig, ax = plt.subplots(figsize=(8, 6))
    if not pre.empty:
        ax.scatter(pre[flow_col] / 1_000_000_000.0, pre["btc_ret_1d_fwd"] * 100.0, alpha=0.6, label="Pre")
    if not post.empty:
        ax.scatter(post[flow_col] / 1_000_000_000.0, post["btc_ret_1d_fwd"] * 100.0, alpha=0.6, label="Post")
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.axvline(0.0, color="black", linewidth=0.8)
    ax.set_xlabel(f"{flow_col} (USD bn)")
    ax.set_ylabel("Next-day BTC return (%)")
    ax.set_title("Flow vs Next-Day BTC Return (Pre/Post ETF Regime)")
    ax.legend()
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def _plot_rolling_corr(df: pd.DataFrame, flow_col: str, window: int, out: Path) -> None:
    data = df[["date", flow_col, "btc_ret_1d_fwd"]].copy()
    data["rolling_corr"] = rolling_corr(data, x_col=flow_col, y_col="btc_ret_1d_fwd", window=window)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(data["date"], data["rolling_corr"], color="#6bd49f")
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.set_title(f"{window}D Rolling Correlation: {flow_col} vs Next-Day BTC Return")
    ax.set_ylabel("Correlation")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def _plot_regime_bars(stats: list[dict[str, float | int | str | None]], out: Path) -> None:
    frame = pd.DataFrame(stats)
    if frame.empty:
        return

    x = np.arange(len(frame))
    w = 0.25

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - w, frame["mean_abs_return_pct"], width=w, label="Mean |ret| (%)")
    ax.bar(x, frame["ann_vol_pct"], width=w, label="Ann vol (%)")
    flow_abs_beta = frame["flow_beta_bps_per_1bn"].abs()
    ax.bar(x + w, flow_abs_beta, width=w, label="|Flow beta| (bps per $1bn)")

    ax.set_xticks(x)
    ax.set_xticklabels(frame["label"])
    ax.set_title("Regime Comparison")
    ax.grid(alpha=0.2)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def _write_summary_md(
    out: Path,
    *,
    split_date: str,
    loaded_sources: list[str],
    flow_col: str | None,
    stats: list[dict[str, float | int | str | None]],
) -> None:
    lines = [
        "# BTC Transparency Shift Summary",
        "",
        f"- ETF regime split date: **{split_date}**",
        f"- Loaded sources: {', '.join(loaded_sources) if loaded_sources else '(none)'}",
        f"- Flow signal used: {flow_col or '(none available)'}",
        "",
    ]

    if stats:
        lines += [
            "## Regime Stats",
            "",
            "| Regime | N | Mean Ret % | Mean |Ret| % | Ann Vol % | Flow Corr | Flow Beta (bps per $1bn) | Flow R2 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for r in stats:
            corr_txt = "n/a" if r["flow_return_corr"] is None else f"{r['flow_return_corr']:.4f}"
            beta_txt = "n/a" if r["flow_beta_bps_per_1bn"] is None else f"{r['flow_beta_bps_per_1bn']:.2f}"
            r2_txt = "n/a" if r["flow_r2"] is None else f"{r['flow_r2']:.4f}"
            lines.append(
                "| "
                f"{r['label']} | {r['n_obs']} | "
                f"{r['mean_return_pct']:.4f} | {r['mean_abs_return_pct']:.4f} | {r['ann_vol_pct']:.2f} | "
                f"{corr_txt} | "
                f"{beta_txt} | "
                f"{r2_txt} |"
            )
    else:
        lines += ["No regime statistics computed."]

    out.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_root) / _ts()
    out_dir.mkdir(parents=True, exist_ok=True)

    df, loaded_sources = load_dataset(data_dir)
    df["btc_ret_1d"] = df["btc_close_usd"].pct_change()
    df["btc_ret_1d_fwd"] = df["btc_ret_1d"].shift(-1)
    df["institutional_wrapper_share_pct"] = compute_inst_share_pct(df)

    flow_col = _choose_flow_col(df)
    stats_out: list[dict[str, float | int | str | None]] = []
    if flow_col is not None:
        regime_stats = compute_regime_stats(df, flow_col=flow_col, return_col="btc_ret_1d_fwd", split_date=args.split_date)
        stats_out = [x.to_dict() for x in regime_stats]
    else:
        regime_stats = []

    merged_path = out_dir / "dataset_merged.csv"
    stats_path = out_dir / "regime_stats.json"
    summary_path = out_dir / "summary.md"
    df_out = df.copy()
    df_out["date"] = df_out["date"].dt.date.astype(str)
    df_out.to_csv(merged_path, index=False)
    stats_path.write_text(json.dumps(stats_out, indent=2), encoding="utf-8")

    _write_summary_md(
        summary_path,
        split_date=args.split_date,
        loaded_sources=loaded_sources,
        flow_col=flow_col,
        stats=stats_out,
    )

    _plot_price_and_share(df, out_dir / "price_vs_wrapper_share.png")
    if flow_col is not None:
        _plot_flow_scatter(df, flow_col=flow_col, split_date=args.split_date, out=out_dir / "flow_vs_nextday_scatter.png")
        _plot_rolling_corr(df, flow_col=flow_col, window=args.rolling_window, out=out_dir / "rolling_flow_return_corr.png")
    if regime_stats:
        _plot_regime_bars(stats_out, out_dir / "regime_comparison_bars.png")

    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "flow_col": flow_col,
                "n_rows": int(len(df)),
                "loaded_sources": loaded_sources,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
