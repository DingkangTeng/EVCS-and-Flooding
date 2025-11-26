import sys
import numpy as np
from shapely.geometry.base import BaseGeometry
from rasterio.coords import BoundingBox
from rasterio.features import rasterize

sys.path.append(".") # Set path to the roots

from _function.rasterFunction import getRasterByRectangleBoundary

def getPixelValuesByLine(line: BaseGeometry, raster: str, rasterBounds: BoundingBox) -> np.ndarray:
    XMin, YMin, XMax, YMax = line.bounds
    # BoundingBox(left=-169.1190811765517, bottom=-50.125992853869306, right=178.55588323622712, top=73.08917730417457)
    # lineBound(135.5999513, 33.5009848, 135.6566354, 33.5105728)
    if (
        XMax < rasterBounds.left or    # XMin
        YMax < rasterBounds.bottom or  # YMin
        XMin > rasterBounds.right or   # XMax
        YMin > rasterBounds.top        # YMax
    ):
        return np.empty([0], dtype=np.uint8)
    
    with getRasterByRectangleBoundary(raster, XMin, YMin, XMax, YMax) as chopped:
        data, transform = chopped
        mask = np.zeros(data.shape, dtype=np.uint8)

        rasterize(
            [line],
            out=mask,
            transform=transform,
            default_value=1,
            all_touched=True
        )

        return data[mask == 1]

# Debug
if __name__ == "__main__":
    from shapely.geometry import LineString
    from rasterCalculate.initRaster import initRaster
    r = initRaster("D:\\flooding\\SumDays.tif")
    print(getPixelValuesByLine(LineString([(0,0),(1,1),(2,2)]), r.rasterPath, r.bounds))