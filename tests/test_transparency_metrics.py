from __future__ import annotations

import numpy as np
import pandas as pd

from hv_btc_vault.transparency_metrics import compute_inst_share_pct, compute_regime_stats


def test_compute_inst_share_pct() -> None:
    df = pd.DataFrame(
        {
            "etp_share_pct": [7.0, 7.2, np.nan],
            "dat_share_pct": [4.0, np.nan, 5.1],
        }
    )
    out = compute_inst_share_pct(df)
    assert float(out.iloc[0]) == 11.0
    assert float(out.iloc[1]) == 7.2
    assert float(out.iloc[2]) == 5.1


def test_compute_regime_stats_has_pre_and_post() -> None:
    n = 220
    dates = pd.date_range("2023-09-01", periods=n, freq="D")
    flow = np.linspace(-500_000_000, 500_000_000, n)
    # synthetic relation so regression stats are non-null
    ret_fwd = 0.000001 * (flow / 1_000_000_000) + 0.0001

    df = pd.DataFrame(
        {
            "date": dates,
            "flow_usd": flow,
            "ret_fwd": ret_fwd,
        }
    )
    stats = compute_regime_stats(df, flow_col="flow_usd", return_col="ret_fwd", split_date="2024-01-11")
    assert len(stats) == 2
    assert {s.label for s in stats} == {"pre", "post"}
    assert all(s.n_obs > 0 for s in stats)
