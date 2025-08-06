from flask import Flask, request, jsonify
import geopandas as gpd
from shapely.geometry import Point

app = Flask(__name__)
tracts = gpd.read_file("filtered_tracts.gpkg")  # Assumes it's in root folder

@app.route("/get_score")
def get_score():
    lat = request.args.get("lat", type=float)
    lon = request.args.get("lon", type=float)

    if lat is None or lon is None:
        return jsonify({"error": "Missing lat or lon"}), 400

    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326")
    point = point.to_crs(tracts.crs)

    match = tracts[tracts.contains(point.iloc[0])]
    if match.empty:
        return jsonify({"tract": None, "score": None})

    row = match.iloc[0]
    return jsonify({"tract": row["GEOID"], "score": row["score"]})
