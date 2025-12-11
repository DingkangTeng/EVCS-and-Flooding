import pandas as pd
import geopandas as gpd

def readNode(
    path: str | tuple[str, str],
    minEvcsNum: int = 0, filterBy: str = "", # iso3 or city
    ignoreUneffected: bool = False, ignoreIsolate: bool = True
) -> tuple[pd.DataFrame | gpd.GeoDataFrame, int, int]:
    if isinstance(path, str):
        fileType = path.split('.')[-1]
        df = pd.read_parquet(path) if fileType == "parquet" else pd.read_csv(path)
    else:
        df = gpd.read_file(path[0], layer=path[1], encoding="utf-8")
    
    # Ignore city with no EVCS
    df = df[df["EVCSNum"] != 0]

    n = df.shape[0]
    uniqueCountries = df["iso3"].unique()
    nc = len(uniqueCountries)
    print(f"The total num is {n}, with {nc} countries.")

    # Ignore isolated data
    if ignoreIsolate:
        df = df[df["city"] != "Isolated"] if "city" in df.columns else df
        n = df.shape[0]

    # Filter by minimum EVCS number
    if minEvcsNum > 0:
        if filterBy in df.columns:
            group = df.groupby(filterBy)["EVCSNum"].sum()
            uniqueCities = group[group >= minEvcsNum].index.unique()
            df = df[df[filterBy].isin(uniqueCities)]
        else:
            df = df[df["EVCSNum"] >= minEvcsNum]
        nc = df["iso3"].unique().shape[0]

        n = df.shape[0]
        print(f"After cleaning, The total num is {n}, with {nc} countries.")

    if ignoreUneffected:
        incides = ["EVCSNum", "EVCSNum_After", "affected"] if "affected" in df.columns else ["EVCSNum", "EVCSNum_After"]
        groupby = "city" if "city" in df.columns else "iso3"
        incides.append(groupby)
        subdf = df[incides].copy()

        subdf["A"] = subdf["EVCSNum_After"] - subdf["EVCSNum"]
        group1 = subdf.groupby(groupby)["A"].sum()
        unique1 = group1[group1 != 0].index.unique()

        if "affected" in df.columns:
            group2 = subdf.groupby(groupby)["affected"].sum()
            unique2 = group2[group2 != 0].index.unique()
            unique = set(unique1) | set(unique2)
        else:
            unique = unique1
        
        df = df[df[groupby].isin(unique)]
        n = df.shape[0]
        nc = df["iso3"].unique().shape[0]
        print(f"After ignore unaffected cities, The total num is {n}, with {nc} countries.")

    return df.copy(), n, nc