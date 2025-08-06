from fastapi import FastAPI, Query
import geopandas as gpd
import json
from shapely.geometry import Point

app = FastAPI()

# Load GeoPackage (one-time on startup)
tracts_gdf = gpd.read_file("filtered_tracts.gpkg")

# Load JSON scores
with open("tract_lookup.json", "r") as f:
    tract_scores = json.load(f)

@app.get("/get_score")
def get_score(lat: float = Query(...), lon: float = Query(...)):
    point = Point(lon, lat)
    match = tracts_gdf[tracts_gdf.geometry.contains(point)]

    if match.empty:
        return {"error": "No tract found for provided coordinates."}

    tract_id = match.iloc[0]["GEOID"]
    score = tract_scores.get(tract_id)

    if score is None:
        return {"tract": tract_id, "error": "Tract found, but no score available."}

    return {"tract": tract_id, "score": score}
