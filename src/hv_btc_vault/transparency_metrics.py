from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class RegimeStats:
    label: str
    n_obs: int
    mean_return_pct: float
    mean_abs_return_pct: float
    ann_vol_pct: float
    flow_return_corr: float | None
    flow_beta_bps_per_1bn: float | None
    flow_r2: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "n_obs": int(self.n_obs),
            "mean_return_pct": float(self.mean_return_pct),
            "mean_abs_return_pct": float(self.mean_abs_return_pct),
            "ann_vol_pct": float(self.ann_vol_pct),
            "flow_return_corr": None if self.flow_return_corr is None else float(self.flow_return_corr),
            "flow_beta_bps_per_1bn": None if self.flow_beta_bps_per_1bn is None else float(self.flow_beta_bps_per_1bn),
            "flow_r2": None if self.flow_r2 is None else float(self.flow_r2),
        }


def _linear_fit_stats(x: pd.Series, y: pd.Series) -> tuple[float | None, float | None]:
    xy = pd.concat([x, y], axis=1).dropna()
    if len(xy) < 20:
        return None, None

    x_vals = xy.iloc[:, 0].to_numpy(dtype=float)
    y_vals = xy.iloc[:, 1].to_numpy(dtype=float)

    if np.allclose(np.var(x_vals), 0.0):
        return None, None

    beta, alpha = np.polyfit(x_vals, y_vals, 1)
    y_hat = alpha + beta * x_vals

    sst = float(np.sum((y_vals - np.mean(y_vals)) ** 2))
    if np.isclose(sst, 0.0):
        return float(beta), None

    sse = float(np.sum((y_vals - y_hat) ** 2))
    r2 = 1.0 - sse / sst
    return float(beta), float(r2)


def compute_regime_stats(
    df: pd.DataFrame,
    *,
    flow_col: str,
    return_col: str,
    split_date: str,
) -> list[RegimeStats]:
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"], utc=False)

    split_dt = pd.to_datetime(split_date)
    pre = data[data["date"] < split_dt]
    post = data[data["date"] >= split_dt]

    out: list[RegimeStats] = []
    for label, part in [("pre", pre), ("post", post)]:
        ret_base = part[[return_col]].dropna()
        n_obs = len(ret_base)
        flow_base = part[[flow_col, return_col]].dropna()

        if n_obs == 0:
            out.append(
                RegimeStats(
                    label=label,
                    n_obs=0,
                    mean_return_pct=float("nan"),
                    mean_abs_return_pct=float("nan"),
                    ann_vol_pct=float("nan"),
                    flow_return_corr=None,
                    flow_beta_bps_per_1bn=None,
                    flow_r2=None,
                )
            )
            continue

        mean_ret = float(ret_base[return_col].mean() * 100.0)
        mean_abs_ret = float(ret_base[return_col].abs().mean() * 100.0)
        ann_vol = float(ret_base[return_col].std(ddof=0) * np.sqrt(365.0) * 100.0)

        corr = flow_base[flow_col].corr(flow_base[return_col]) if len(flow_base) > 0 else np.nan
        corr_out = None if pd.isna(corr) else float(corr)

        beta, r2 = _linear_fit_stats(flow_base[flow_col] / 1_000_000_000.0, flow_base[return_col])
        beta_bps_per_1bn = None if beta is None else float(beta * 10_000.0)

        out.append(
            RegimeStats(
                label=label,
                n_obs=n_obs,
                mean_return_pct=mean_ret,
                mean_abs_return_pct=mean_abs_ret,
                ann_vol_pct=ann_vol,
                flow_return_corr=corr_out,
                flow_beta_bps_per_1bn=beta_bps_per_1bn,
                flow_r2=r2,
            )
        )
    return out


def compute_inst_share_pct(df: pd.DataFrame) -> pd.Series:
    cols = [c for c in ["etp_share_pct", "dat_share_pct"] if c in df.columns]
    if not cols:
        return pd.Series(index=df.index, dtype=float)
    return df[cols].sum(axis=1, min_count=1)


def rolling_corr(
    df: pd.DataFrame,
    *,
    x_col: str,
    y_col: str,
    window: int = 60,
) -> pd.Series:
    return df[x_col].rolling(window).corr(df[y_col])
