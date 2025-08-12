from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List, Tuple

from app_lookup import _load, ready, latlon_to_geoid_score, GEOMS_PATH, SCORES_PATH

app = FastAPI(title="Tract Score API", version="1.0.0",
              description="lat,lon → Census tract GEOID + precomputed score")

class ScoreResponse(BaseModel):
    geoid: str
    score: float | int | None = Field(None, description="Precomputed score for this tract (may be null)")

class BulkRequest(BaseModel):
    points: List[Tuple[float, float]] = Field(..., description="List of [lat, lon] pairs")

class BulkItem(BaseModel):
    lat: float
    lon: float
    geoid: Optional[str] = None
    score: float | int | None = None
    ok: bool = False
    error: Optional[str] = None

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/readyz")
def readyz():
    return {"ready": ready(), "geom_path": str(GEOMS_PATH), "scores_path": str(SCORES_PATH)}

@app.post("/reload")
def reload_index():
    try:
        _load()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/score", response_model=ScoreResponse)
def score(
    lat: float = Query(..., description="Latitude (EPSG:4326)"),
    lon: float = Query(..., description="Longitude (EPSG:4326)")
):
    try:
        geoid, s = latlon_to_geoid_score(lat, lon)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    if geoid is None:
        raise HTTPException(status_code=404, detail="Point not inside any tract")
    return {"geoid": geoid, "score": s}

@app.post("/score_bulk", response_model=list[BulkItem])
def score_bulk(req: BulkRequest):
    out: list[BulkItem] = []
    for lat, lon in req.points:
        try:
            geoid, s = latlon_to_geoid_score(lat, lon)
            if geoid is None:
                out.append(BulkItem(lat=lat, lon=lon, ok=False, error="not_in_tract"))
            else:
                out.append(BulkItem(lat=lat, lon=lon, geoid=geoid, score=s, ok=True))
        except RuntimeError as e:
            out.append(BulkItem(lat=lat, lon=lon, ok=False, error=str(e)))
        except Exception as e:
            out.append(BulkItem(lat=lat, lon=lon, ok=False, error=f"error:{e}"))
    return out
