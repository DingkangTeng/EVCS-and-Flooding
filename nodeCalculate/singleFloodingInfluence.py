import sys, os, zipfile
import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import override

sys.path.append(".") # Set path to the roots

from _function.readFiles import readFiles, loadJsonRecord
from nodeCalculate.sumFloodingInfluence import allFloodingInfluence
from rasterCalculate.initRaster import initRaster

SPECIAL_REGION_DICT = {
    # "GIB": "ESP"  
    # "XKX": "SRB",
    "ESH": ["MAR", "DZA", "MRT"],
    "GLP": ["PRI", "VEN", "DOM", "GUY"],
    "BRB": ["PRI", "VEN", "DOM", "GUY"],
    "VCT": ["PRI", "VEN", "DOM", "GUY"],
    "DMA": ["PRI", "VEN", "DOM", "GUY"],
    "VIR": ["PRI", "VEN", "DOM", "GUY"],
    "MTQ": ["PRI", "VEN", "DOM", "GUY"],
    "AIA": ["PRI", "VEN", "DOM", "GUY"],
    "MSR": ["PRI", "VEN", "DOM", "GUY"],
    "MAF": ["PRI", "VEN", "DOM", "GUY"],
    "GRD": ["PRI", "VEN", "DOM", "GUY"],
    "VGB": ["PRI", "VEN", "DOM", "GUY"],
    "KNA": ["PRI", "VEN", "DOM", "GUY"],
    "BLM": ["PRI", "VEN", "DOM", "GUY"],
    "TTO": ["PRI", "VEN", "DOM", "GUY"],
    "LCA": ["PRI", "VEN", "DOM", "GUY"],
    "BES": ["PRI", "VEN", "DOM", "GUY"],
    "SXM": ["PRI", "VEN", "DOM", "GUY"],
    "ATG": ["PRI", "VEN", "DOM", "GUY"],
    "AND": ["FRA", "ESP"],
    "TCA": ["DOM", "HTI", "CUB", "BHS"],
    "BHR": ["SAU", "QAT", "IRN", "KWT"],
    "CYP": ["TUR", "SYR", "LBN", "ISR", "EGY", "GRC"],
    "SSD": ["SDN", "CAF", "COD", "UGA", "KEN", "ETH"],
    "GUF": ["SUR", "BRA"],
    "PSE": ["ISR", "EGY", "JOR", "LBN"],
    "IMN": ["GBR", "IRL"],
    "SMR": ["ITA"],
    "MNE": ["ALB", "SRB", "BIH", "HRV"],
    "CYM": ["CUB", "HTI", "COL", "PAN", "NIC", "HND", "BLZ", "GTM", "MEX"],
    "JAM": ["CUB", "HTI", "COL", "PAN", "NIC", "HND", "BLZ", "GTM", "MEX"],
    "MCO": ["FRA"],
    "BRN": ["MYS", "VNM", "PHL"]
}

class singleFloodingInfluenec(allFloodingInfluence):
    __slots__ = ["gpkgPath", "rasters", "rasterRoot", "decompressRasterPath"]

    def __init__(self, gpkgPath: str, rasterRoot: str, decompressPath: str) -> None:
        self.gpkgPath = gpkgPath

        self.rasters = readFiles(rasterRoot).allFolder()
        self.rasterRoot = rasterRoot
        self.decompressRasterPath = decompressPath

        return

    def processOneRaster(self, gdf: gpd.GeoDataFrame, raster: str, excutor: ProcessPoolExecutor, bar: tqdm | None = None) -> np.ndarray:
        rasterInfo = initRaster(raster)
        crs = rasterInfo.crs
        bounds = rasterInfo.bounds

        if gdf.crs != crs:
            tqdm.write("Projecting!")
            gdf.to_crs(crs, inplace=True)

        results = np.empty([gdf.shape[0], 2], dtype=np.uint32) # Max 4294967295, because fid will exceed 65535
        chunk = 10000
        numChunks = (gdf.shape[0] + chunk - 1) // chunk
        gdfs = [gdf.iloc[i * chunk : (i + 1) * chunk] for i in range(numChunks)]
        
        for df in gdfs:
            futures = []
            debugDict = {}
            for index, geom in zip(df.index, df.geometry):
                future = excutor.submit(self.processByFid, index, raster, geom, bounds)
                futures.append(future)
                debugDict[future] = index
            
            for future in as_completed(futures):
                index = debugDict[future]
                try:
                    fid, maxDays = future.result()
                except Exception as e:
                    raise RuntimeError("Error processing fid {}: {}".format(index, e))
                else:
                    results[index][0] = fid
                    results[index][1] = maxDays
                    if bar is not None:
                        bar.update(1)

        return results
    
    @override
    def calOneGpkg(self, gpkg: str, threadNum: int = 1, *agr, **agrs) -> bool:
        country = gpkg.split('.')[0]
        specialRegion = list(SPECIAL_REGION_DICT.keys())
        log = loadJsonRecord(os.path.join(self.gpkgPath, "log.json"), "Flooding_Road_By_Max_Influence", {})
        processedRaster = log.get(gpkg, [])
        if country not in self.rasters + specialRegion:
            tqdm.write("Do not found rasters for {}.".format(country))
            log.append({gpkg: ["Do not found rasters"]})
            log.save()
            return True
        
        # Read compressed raster files
        ## Special region
        if country in specialRegion:
            rasterRoots = [os.path.join(self.rasterRoot, x) for x in SPECIAL_REGION_DICT[country]]
        else:
            rasterRoots = [os.path.join(self.rasterRoot, country)]
        
        realTif = []
        for rasterRoot in rasterRoots:
            rasterZips = readFiles(rasterRoot).specificFile(["zip"])
            for file in rasterZips:
                zipPath = os.path.join(rasterRoot, file)
                z = zipfile.ZipFile(zipPath, 'r')
                for tif in z.namelist():
                    if tif.split('.')[-1] != "tif":
                        continue
                    elif tif not in processedRaster:
                        realTif.append(tif)
                z.close()
        
        if len(realTif) == 0:
            return True

        rasters = [os.path.join(self.decompressRasterPath, x+".tif") for x in realTif]
        gpkgPath = os.path.join(self.gpkgPath, gpkg)
        gdf = gpd.read_file(gpkgPath, layer="edges", encoding="utf-8")
        bar = tqdm(total=len(rasters) * (1 + gdf.shape[0]), desc="Processing {}".format(gpkg), unit="roads")
        excutor = ProcessPoolExecutor(max_workers=threadNum)

        for raster in rasters:
            rasterName = os.path.basename(raster).split('.')[0].replace('-','_')
            # rasterName = rasterName  + "_3" # Change rastername for test here
            bar.set_description("Processing {} in {}".format(rasterName, gpkg))

            result = self.processOneRaster(gdf, raster, excutor, bar)
            df = pd.DataFrame(result, columns=["fid", rasterName])
            if df[df[rasterName] != 0].shape[0] == 0:
                processedRaster.append(os.path.basename(raster)[:-4])
                log.append({gpkg: processedRaster})
                log.save()
                bar.update(1)
                continue  # Skip if no non-zero values found
            
            bar.set_description("Updating {} in {}".format(rasterName, gpkg))

            # Update data
            self.updateData(gpkgPath, df, rasterName)
            # Update log
            processedRaster.append(os.path.basename(raster)[:-4])    
            log.append({gpkg: processedRaster})
            log.save()
            bar.update(1)

        excutor.shutdown()
        bar.close()

        return True
    
    @override
    def calculateAll(self, threadNum: int = 1, *agr, **agrs) -> None:
        gpkgs = readFiles(self.gpkgPath).specificFile(["gpkg"])
        for gpkg in tqdm(gpkgs, desc="Processing all gpkgs", unit="gpkg"):
            self.calOneGpkg(gpkg, threadNum)

        return

# Debug
if __name__ == "__main__":
    singleFloodingInfluenec(r"C:\\0_PolyU\\roadsGraph", r"D:\\flooding", r"D:\\floodingAll_Days").calculateAll(16) # type: ignore 
    # singleFloodingInfluenec(r"C:\\0_PolyU\\test", r"D:\\flooding", r"D:\\floodingAll_Days").calculateAll(16) # type: ignore 