import os
import json
import pathlib
import pandas as pd
from shapely import from_wkb
from shapely.geometry import Point
from shapely.strtree import STRtree

# ---------- Config & paths ----------
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "/data"))
GEOMS_ENV = os.getenv("GEOMS_PATH", "").strip()
GEOMS_PATH = pathlib.Path(GEOMS_ENV) if GEOMS_ENV else (DATA_DIR / "tracts_wkb.parquet")
SCORES_PATH = pathlib.Path(os.getenv("SCORES_PATH", "./tract_lookup.json"))

# ---------- Module-level state (lazy-loaded) ----------
df = None                   # pandas DataFrame with GEOID + wkb
geoms = None                # list[shapely.Polygon]
tree: STRtree | None = None # spatial index
geoid_by_geom_id = {}       # id(geom) -> GEOID
scores = {}                 # GEOID -> score

def _exists_ok() -> tuple[bool, bool]:
    return GEOMS_PATH.exists(), SCORES_PATH.exists()

def _load() -> None:
    """(Re)load geometries, build STRtree, and load scores."""
    global df, geoms, tree, geoid_by_geom_id, scores

    if not GEOMS_PATH.exists():
        raise FileNotFoundError(f"Geometry parquet missing: {GEOMS_PATH}")
    if not SCORES_PATH.exists():
        raise FileNotFoundError(f"Scores JSON missing: {SCORES_PATH}")

    # Read parquet with two cols: GEOID (str), wkb (bytes)
    df = pd.read_parquet(GEOMS_PATH)
    if not {"GEOID", "wkb"}.issubset(df.columns):
        raise ValueError("Parquet must contain columns: GEOID, wkb")

    # Build shapely geoms and STRtree
    gseries = df["wkb"].values
    geoms = [from_wkb(w) for w in gseries]
    tree = STRtree(geoms)
    geoids = df["GEOID"].astype(str).values
    geoid_by_geom_id = {id(g): geoid for g, geoid in zip(geoms, geoids)}

    # Load scores
    with open(SCORES_PATH, "r") as f:
        scores = json.load(f)

    print(f"[startup] Loaded polygons: {len(geoms)} | scores: {len(scores)}")
    print(f"[startup] GEOMS_PATH={GEOMS_PATH} | SCORES_PATH={SCORES_PATH}")

def ready() -> bool:
    """True if index and scores are loaded."""
    return tree is not None and len(scores) > 0

def latlon_to_geoid_score(lat: float, lon: float) -> tuple[str | None, float | int | None]:
    if not ready():
        raise RuntimeError("Index not loaded yet. Upload file(s) and call /reload.")
    pt = Point(lon, lat)
    candidates = tree.query(pt)
    for poly in candidates:
        if poly.covers(pt):  # tolerant on boundaries
            geoid = geoid_by_geom_id[id(poly)]
            return geoid, scores.get(geoid)
    return None, None

# Try to autoload at import time; if files aren’t there, don’t crash.
try:
    if GEOMS_PATH.exists() and SCORES_PATH.exists():
        _load()
    else:
        g_ok, s_ok = _exists_ok()
        print(f"[startup] Waiting for files. geometry={g_ok}, scores={s_ok}. "
              f"GEOMS_PATH={GEOMS_PATH}, SCORES_PATH={SCORES_PATH}")
except Exception as e:
    # Never crash the module import; log and continue.
    print("[startup] WARNING: deferred load due to error:", str(e))
