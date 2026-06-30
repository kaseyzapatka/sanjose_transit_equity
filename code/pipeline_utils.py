# ==========================================================================
# pipeline_utils.py — small reproducibility helpers shared by the analysis.
#
# Two goals, both flagged in review:
#   1. Fail fast. A spatial filter that returns zero features should raise,
#      not silently write NaN/empty outputs over good ones.
#   2. Write atomically. Outputs are written to a temp file and only moved
#      into place after the write succeeds, so a mid-run failure cannot
#      corrupt an existing good output.
# ==========================================================================

import os
from pathlib import Path

import numpy as np


def require(condition: bool, message: str) -> None:
    """Raise with a clear message if a pipeline invariant is violated."""
    if not condition:
        raise ValueError(f"[pipeline check failed] {message}")


def check_geo(gdf, name: str, min_rows: int = 1) -> None:
    """Validate a GeoDataFrame before it drives results: non-empty, has a CRS,
    and finite total bounds (catches the corrupt-GeoParquet / CRS issues that
    otherwise surface as zero-feature selections)."""
    require(len(gdf) >= min_rows,
            f"{name}: expected >= {min_rows} rows, got {len(gdf)}")
    require(gdf.crs is not None, f"{name}: missing CRS")
    bounds = gdf.total_bounds
    require(np.all(np.isfinite(bounds)),
            f"{name}: non-finite bounds {bounds} (likely invalid geometry or CRS)")


def atomic_save(obj, path, writer) -> Path:
    """Write `obj` to a temp file via `writer(obj, tmp_path)`, then atomically
    replace `path`. Leaves any existing good output untouched on failure."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    writer(obj, tmp)
    os.replace(tmp, path)
    return path


def save_csv(df, path) -> Path:
    return atomic_save(df, path, lambda o, p: o.to_csv(p, index=False))


def save_parquet(gdf, path) -> Path:
    return atomic_save(gdf, path, lambda o, p: o.to_parquet(p))
