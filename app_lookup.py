# app_lookup.py
import os
import json
import pathlib
import pandas as pd
import numpy as np
from shapely import from_wkb
from shapely.geometry import Point
from shapely.strtree import STRtree

# ---------- Config & paths ----------
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "/data"))
GEOMS_ENV = os.getenv("GEOMS_PATH", "").strip()
GEOMS_PATH = pathlib.Path(GEOMS_ENV) if GEOMS_ENV else (DATA_DIR / "tracts_wkb.parquet")
SCORES_PATH = pathlib.Path(os.getenv("SCORES_PATH", "./tract_lookup.json"))

# ---------- Module-level state (lazy-loaded) ----------
df: pd.DataFrame | None = None         # DataFrame with GEOID + wkb
geoms: list | None = None              # list[shapely geometry]
tree: STRtree | None = None            # spatial index
geoid_by_geom_id: dict = {}            # id(geom) -> GEOID
geoid_by_idx: list[str] | None = None  # index -> GEOID (aligned with geoms)
scores: dict = {}                      # GEOID -> score

def _exists_ok() -> tuple[bool, bool]:
    return GEOMS_PATH.exists(), SCORES_PATH.exists()

def _load() -> None:
    """(Re)load geometries, build STRtree, and load scores."""
    global df, geoms, tree, geoid_by_geom_id, geoid_by_idx, scores

    if not GEOMS_PATH.exists():
        raise FileNotFoundError(f"Geometry parquet missing: {GEOMS_PATH}")
    if not SCORES_PATH.exists():
        raise FileNotFoundError(f"Scores JSON missing: {SCORES_PATH}")

    # Expect exactly two columns: GEOID (str-like), wkb (bytes)
    df = pd.read_parquet(GEOMS_PATH)
    if not {"GEOID", "wkb"}.issubset(df.columns):
        raise ValueError("Parquet must contain columns: GEOID, wkb")

    # Build shapely geometries & spatial index
    geoids = df["GEOID"].astype(str).values
    geoms_list = [from_wkb(w) for w in df["wkb"].values]
    idx_tree = STRtree(geoms_list)

    # Maps for both STRtree return modes
    geoid_by_idx = list(geoids)  # index -> GEOID
    geoid_by_geom_id = {id(g): geoid for g, geoid in zip(geoms_list, geoids)}

    # Load scores
    with open(SCORES_PATH, "r") as f:
        scores = json.load(f)

    # Commit to globals last (so we don’t leave half-initialized state)
    geoms = geoms_list
    tree = idx_tree

    print(f"[startup] Loaded polygons: {len(geoms)} | scores: {len(scores)}")
    print(f"[startup] GEOMS_PATH={GEOMS_PATH} | SCORES_PATH={SCORES_PATH}")

def ready() -> bool:
    """True if index and scores are loaded."""
    return tree is not None and geoms is not None and len(scores) > 0

def latlon_to_geoid_score(lat: float, lon: float) -> tuple[str | None, float | int | None]:
    """
    Robust to STRtree returning either geometry objects or integer indices,
    depending on platform/build.
    """
    if not ready():
        raise RuntimeError("Index not loaded yet. Upload file(s) and call /reload.")

    pt = Point(lon, lat)
    candidates = tree.query(pt)  # may be geometries OR int indices

    for cand in candidates:
        try:
            # Case A: query returned indices (int / numpy.int64)
            if isinstance(cand, (int, np.integer)):
                idx = int(cand)
                geom = geoms[idx]
                if geom.covers(pt):  # covers() is boundary-friendly
                    geoid = geoid_by_idx[idx]
                    return geoid, scores.get(geoid)

            # Case B: query returned geometry objects
            else:
                geom = cand
                if geom.covers(pt):
                    geoid = geoid_by_geom_id[id(geom)]
                    return geoid, scores.get(geoid)

        except Exception:
            # Ignore this candidate and continue scanning others
            continue

    return None, None

# Try to autoload at import; if files aren’t there, don’t crash.
try:
    g_ok, s_ok = _exists_ok()
    if g_ok and s_ok:
        _load()
    else:
        print(f"[startup] Waiting for files. geometry={g_ok}, scores={s_ok}. "
              f"GEOMS_PATH={GEOMS_PATH}, SCORES_PATH={SCORES_PATH}")
except Exception as e:
    print("[startup] WARNING: deferred load due to error:", str(e))
