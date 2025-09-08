import sys, os
import geopandas as gpd

sys.path.append(".") # Set path to the roots

from function.readFiles import readFiles

class findMaxFloodingInfluence:

    def __init__(self) -> None:
        pass

    def processOne(self, gpkg: str) -> None:
        # Skip the processed

        df = gpd.read_file(gpkg, layer="edges")
        processedDays = df.columns[df.columns.str.contains("DFO_")].to_numpy()
        for i in processedDays:
            df[i] = df[i] * df["length"]
        
        # Cal the sum of incident by cities boundary and mark the max influence incident
        # for i in boundary:
            subdf = df
            for j in processedDays:
                subdf[j].sum() #if max?
        
        return

    def processAll(self) -> None:
        pass

# Debug
if __name__ == "__main__":
    findMaxFloodingInfluence()