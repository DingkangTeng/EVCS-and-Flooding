import os, time, psutil, gc
import geopandas as gpd

def roadDens(road: str, subdf: gpd.GeoDataFrame) -> gpd.pd.Series:
    i = 1
    while True:
        if psutil.virtual_memory().percent < 70 or i > 6: break
        else:
            time.sleep(10 * i)
            i += 1
            gc.collect()
            
    roadGpkg = gpd.read_file(road, layer="edges", encoding="utf-8")[["geometry", "length"]]
    
    # Check crs
    if roadGpkg.crs is None: raise RuntimeError("{} do not have crs.".format(road))
    if roadGpkg.crs != subdf.crs: subdf = subdf.to_crs(roadGpkg.crs)

    for row in subdf.itertuples():
        idx = getattr(row, "Index")
        geom = getattr(row, "geometry")
        area = getattr(row, "area")

        overlapping = roadGpkg[roadGpkg.intersects(geom)]
        subdf.at[idx, "roadDens"] = overlapping["length"].sum() / area

    del roadGpkg
    gc.collect()

    return subdf["roadDens"]