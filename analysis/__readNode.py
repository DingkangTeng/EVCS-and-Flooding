import pandas as pd
import geopandas as gpd

def readNode(
    path: str | tuple[str, str],
    minEvcsNum: int = 10,
    node: bool = False, ignoreUneffected: bool = True, ignoreIsolate: bool = True
) -> tuple[pd.DataFrame | gpd.GeoDataFrame, int, int]:
    if isinstance(path, str):
        fileType = path.split('.')[-1]
        df = pd.read_parquet(path) if fileType == "parquet" else pd.read_csv(path)
    else:
        df = gpd.read_file(path[0], layer=path[1], encoding="utf-8")
    
    n = df.shape[0]
    uniqueCountries = df["iso3"].unique()
    nc = len(uniqueCountries)
    print(f"The total num is {n}, with {nc} countries.")

    # Ignore city with no EVCS
    if not node:
        df = df[df["EVCSNum"] != 0]
        ignoreIsolate = False
        n = df.shape[0]

    # Ignore isolated data
    if ignoreIsolate:
        df = df[df["city"] != "Isolated"] if "city" in df.columns else df
        n = df.shape[0]

    # Filter by minimum EVCS number
    if minEvcsNum > 0:
        group = df.groupby("iso3")["EVCSNum"].sum()
        uniqueCountries = group[group >= minEvcsNum].index.unique()
        nc = len(uniqueCountries)
        df = df[df["iso3"].isin(uniqueCountries)]
        n = df.shape[0]
        print(f"After cleaning, The total num is {n}, with {nc} countries.")

    if ignoreUneffected:
        subdf = df[["iso3", "A_All", "A_All_After"]].copy()
        subdf["A"] = subdf["A_All_After"] - subdf["A_All"]
        group = subdf.groupby("iso3")["A"].sum()
        uniqueCountries = group[group != 0].index.unique()
        nc = len(uniqueCountries)
        df = df[df["iso3"].isin(uniqueCountries)]
        n = df.shape[0]
        print(f"After ignore unaffected country, The total num is {n}, with {nc} countries.")

    return df.copy(), n, nc