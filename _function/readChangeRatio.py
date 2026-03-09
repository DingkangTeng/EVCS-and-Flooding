import pandas as pd
from dataclasses import dataclass

@dataclass
class COL:
    __col = ["A_All_change_{}", "A_POIAll_change_{}"] #, "A_2024_change_{}"
    all = [x.format("all") for x in __col]
    EVCS = [x.format("EVCSOnly") for x in __col]
    road = [x.format("RoadsOnly") for x in __col]
    cols = all + EVCS + road

def readChangeRatio(path: str, indexCity: bool = False) -> tuple[pd.DataFrame, type[COL]]:
    df = pd.read_csv(path, encoding="utf-8", usecols=["city"] + COL.cols).dropna() #  + COL_GINI.cols

    if indexCity:
        return df.set_index("city"), COL
    else:
        return df, COL