import requests
import json
import time
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from shapely.ops import unary_union

##############################
#  CONFIG: Shapefile Path
##############################
ADMIN_SHAPEFILE_PATH = "NasaKG/boundaries/boundaries.shp"


##############################
#  (1) Fetch Data
##############################
def fetch_nasa_cmr_all_pages(page_size=200, max_pages=None):
    """
    Fetches dataset 'collections' from NASA's CMR API.
    - page_size: results per page
    - max_pages: optionally limit total pages
    Returns a list of dataset entries.
    """
    cmr_url = "https://cmr.earthdata.nasa.gov/search/collections.json"
    all_data = []
    page_num = 1

    while True:
        params = {
            "page_size": page_size,
            "page_num": page_num
        }
        try:
            response = requests.get(cmr_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # If there's no valid data, stop
            if "feed" not in data or "entry" not in data["feed"] or not data["feed"]["entry"]:
                break

            entries = data["feed"]["entry"]
            all_data.extend(entries)

            print(f"Fetched page {page_num}, total datasets so far: {len(all_data)}")

            page_num += 1
            time.sleep(0.2)  # small delay to avoid rapid requests

            if max_pages and page_num > max_pages:
                break

        except requests.exceptions.Timeout:
            print("Request timed out. Ending fetch loop.")
            break
        except requests.exceptions.RequestException as e:
            print(f"Error fetching NASA CMR data: {e}")
            break

    return all_data


##############################
#  (2) Geometry Helpers
##############################
def extract_polygons(geom):
    """
    Ensure we only return Polygon or MultiPolygon.
    If 'geom' is a GeometryCollection, extract any polygons inside.
    Return None if there's nothing suitable.
    """
    if geom is None:
        return None

    gtype = geom.geom_type
    if gtype in ["Polygon", "MultiPolygon"]:
        return geom
    elif gtype == "GeometryCollection":
        # Extract polygon components
        polys = [g for g in geom.geoms if g.geom_type in ["Polygon", "MultiPolygon"]]
        if not polys:
            return None
        if len(polys) == 1:
            return polys[0]
        # Merge multiple polygons
        return unary_union(polys)
    else:
        # It's a Point, LineString, etc.
        return None


def parse_cmr_spatial(boxes=None, polygons=None, points=None):
    """
    Convert NASA CMR 'boxes', 'polygons', or 'points' into
    a single Polygon/MultiPolygon if possible.
    Skips or merges geometry as needed.
    """
    shapes = []

    # 1) Boxes -> Polygons
    if boxes:
        for b in boxes:
            coords = b.split()
            if len(coords) == 4:
                # [SouthLat, WestLon, NorthLat, EastLon]
                southLat, westLon, northLat, eastLon = map(float, coords)
                poly = Polygon([
                    (westLon, southLat),
                    (eastLon, southLat),
                    (eastLon, northLat),
                    (westLon, northLat),
                    (westLon, southLat),
                ])
                shapes.append(poly)

    # 2) Polygons
    if polygons:
        for poly_list in polygons:
            for poly_str in poly_list:
                coords = poly_str.split()
                if len(coords) < 6:
                    # Not enough coords for a polygon
                    continue
                pairs = []
                for i in range(0, len(coords), 2):
                    lat = float(coords[i])
                    lon = float(coords[i+1])
                    pairs.append((lon, lat))
                # Close polygon if needed
                if pairs and pairs[0] != pairs[-1]:
                    pairs.append(pairs[0])
                if len(pairs) > 2:
                    shapes.append(Polygon(pairs))

    # 3) Points -> (Skipping or buffer logic could go here; currently not used)

    # Merge into a single geometry, if possible
    if not shapes:
        return None
    if len(shapes) == 1:
        merged_geom = shapes[0]
    else:
        merged_geom = unary_union(shapes)

    # Extract only Polygon/MultiPolygon
    return extract_polygons(merged_geom)


##############################
#  (3) Classification Helpers
##############################
def classify_bbox_scope(rows_for_dataset):
    """
    Given a set of admin polygons (rows) intersecting a NASA dataset geometry,
    classify bounding box as 'city', 'country', 'continent', or 'global'.
    Return also sets of city/country/continent names found.
    """
    # Adjust column names to your shapefile fields
    CITY_COL = 'NAME_2'       # or something similar
    COUNTRY_COL = 'ADMIN'     # e.g. for country name
    CONTINENT_COL = 'CONTINENT'

    cities = set()
    countries = set()
    continents = set()

    for _, row in rows_for_dataset.iterrows():
        city_val = row.get(CITY_COL)
        country_val = row.get(COUNTRY_COL)
        continent_val = row.get(CONTINENT_COL)
        if city_val:
            cities.add(city_val)
        if country_val:
            countries.add(country_val)
        if continent_val:
            continents.add(continent_val)

    # Basic logic
    if len(cities) == 1 and len(countries) == 1:
        scope = 'city'
    elif len(countries) > 1 and len(continents) == 1:
        scope = 'continent'
    elif len(continents) > 1:
        scope = 'global'
    elif len(cities) > 1 or len(countries) == 1:
        scope = 'country'
    else:
        scope = 'global'  # fallback

    return {
        'scope': scope,
        'cities': list(cities),
        'countries': list(countries),
        'continents': list(continents)
    }


##############################
#  (4) Bulk Intersection
##############################
def bulk_find_admin_areas(nasa_gdf, admin_shapefile_path):
    """
    1) Reads admin shapefile once.
    2) Does a single spatial join with all NASA polygons in `nasa_gdf`.
    3) Returns a DataFrame that has columns from both NASA GDF and admin shapefile,
       including 'dataset_index' to identify each NASA polygon.
    """
    admin_gdf = gpd.read_file(admin_shapefile_path)

    # Ensure both GeoDataFrames share the same CRS
    if nasa_gdf.crs is None:
        nasa_gdf.set_crs(admin_gdf.crs, inplace=True)
    else:
        nasa_gdf = nasa_gdf.to_crs(admin_gdf.crs)

    # Perform a spatial join (intersects)
    joined = gpd.sjoin(nasa_gdf, admin_gdf, how="left", predicate="intersects")
    return joined


##############################
#  (5) Main Transformation
##############################
def transform_cmr_to_classes(all_entries):
    """
    Build final classification in a single pass:
      - Parse geometry for each dataset
      - Build a GeoDataFrame of all NASA polygons
      - Do a single spatial join with the admin shapefile
      - Group by dataset index to find city/country/continent sets
      - Classify scope
      - Return structured results + fail count

    Includes calculation of 'duration_days' from 'time_start' and 'time_end'.
    """
    # Prepare final output containers
    output = {
        "Dataset": [],
        "DataCategory": [],
        "DataFormat": [],
        "LocationCategory": [],
        "SpatialExtent": [],
        "Station": []
    }

    # 1) Parse geometry for each entry
    geoms = []
    fail_count = 0

    for idx, entry in enumerate(all_entries):
        # A) Dataset
        dataset_obj = {
            "short_name": entry.get("short_name", "N/A"),
            "title": entry.get("title", "N/A"),
            "links": entry.get("links", [])
        }
        output["Dataset"].append(dataset_obj)

        # B) DataCategory
        data_category_obj = {
            "summary": entry.get("summary", "N/A")
        }
        output["DataCategory"].append(data_category_obj)

        # C) DataFormat
        data_format_obj = {
            "original_format": entry.get("original_format", "N/A")
        }
        output["DataFormat"].append(data_format_obj)

        # D) Parse geometry
        boxes = entry.get("boxes", [])
        polygons = entry.get("polygons", [])
        points = entry.get("points", [])
        geometry = parse_cmr_spatial(boxes, polygons, points)

        # E) Compute time duration (in days) if possible
        time_start_str = entry.get("time_start")
        time_end_str = entry.get("time_end")
        duration_days = None
        if time_start_str and time_end_str:
            try:
                start_dt = pd.to_datetime(time_start_str)
                end_dt = pd.to_datetime(time_end_str)
                # Calculate difference in days (could also store in seconds)
                duration_days = (end_dt - start_dt).days
            except Exception:
                # If parsing fails, leave duration as None
                pass

        # F) SpatialExtent
        spatial_extent_obj = {
            "boxes": boxes,
            "polygons": polygons,
            "points": points,
            "place_names": [],
            "time_start": time_start_str,
            "time_end": time_end_str,
            "duration_days": duration_days
        }

        # G) Station
        station_obj = {
            "platforms": entry.get("platforms", [])
        }

        if geometry is None:
            fail_count += 1
            # Mark unclassified if we don't have a valid geometry
            output["LocationCategory"].append({"category": "unclassified"})
            output["SpatialExtent"].append(spatial_extent_obj)
            output["Station"].append(station_obj)
            continue

        # Valid geometry: store it for the bulk intersection
        geoms.append({"dataset_index": idx, "geometry": geometry})

        # Push placeholders for classification
        output["LocationCategory"].append({"category": None})
        output["SpatialExtent"].append(spatial_extent_obj)
        output["Station"].append(station_obj)

    # If no valid geometries, return now
    if not geoms:
        return output, fail_count

    # Build a GeoDataFrame from these geometries
    nasa_gdf = gpd.GeoDataFrame(geoms, geometry="geometry", crs="EPSG:4326")

    # Single bulk intersection with the admin shapefile
    joined = bulk_find_admin_areas(nasa_gdf, ADMIN_SHAPEFILE_PATH)

    # Group by dataset_index to gather all matching admin polygons
    grouped = joined.groupby("dataset_index")

    # Classify each dataset using the grouped admin polygons
    for dataset_index, rows in grouped:
        classification = classify_bbox_scope(rows)

        # Fill classification in the output
        output["LocationCategory"][dataset_index]["category"] = classification["scope"]

        place_names = (
            classification["cities"] +
            classification["countries"] +
            classification["continents"]
        )
        output["SpatialExtent"][dataset_index]["place_names"] = place_names

        # Handle the case of geometry not intersecting any admin shape
        if len(rows) == 1 and pd.isnull(rows.iloc[0]["index_right"]):
            output["LocationCategory"][dataset_index]["category"] = "unclassified"
            output["SpatialExtent"][dataset_index]["place_names"] = []

    return output, fail_count


##############################
#  (6) Main
##############################
def main():
    # 1) Fetch NASA CMR data
    all_data = fetch_nasa_cmr_all_pages(page_size=200, max_pages=None)
    print(f"Total collections fetched: {len(all_data)}")

    # 2) Transform & classify
    structured_data, fail_count = transform_cmr_to_classes(all_data)

    # 3) Save to JSON
    output_file = "cmr_final_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(structured_data, f, indent=2)
    print(f"Saved structured data to {output_file}")

    # 4) Print how many datasets had geometry issues
    print(f"{fail_count} datasets had invalid or unsupported geometry.")


if __name__ == "__main__":
    main()
