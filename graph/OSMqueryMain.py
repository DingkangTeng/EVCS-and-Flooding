import sys, datetime

sys.path.append(".")

from OSMquery_geopandas import *


if __name__ == "__main__":
    sys.path.append(".//.") # Set path to the roots
    tagBase = ["motorway",  "trunk", "primary", "secondary", "tertiary"]
    tagLink = [x + "_link" for x in tagBase]
    GetOSMData(
        OSMTagKey="highway",
        OSMTagValues= tagBase + tagLink,
        method="Define a bounding box",
        # extent=extent(118.37407865059036, 31.773696780762247, 119.08569162178092, 32.31036776671775),
        extent=extent(-180,-90,180,90),
        onlyGeo=True, # Ignore the attributes table
        referenceDate=datetime.datetime.now(),
        saveType=[False, True, False] # Point, Line, Polygon
    ).execute(
        "test" # Save path, gpkg in default
    )