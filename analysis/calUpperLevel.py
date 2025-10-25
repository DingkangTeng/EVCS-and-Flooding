import os, sys

sys.path.append(".") # Set path to the roots

from analysis.readNode import readNode
from analysis import A, POP_DICT

class calUpperLevel:
    __slots__ = ["df", "n", "nc", "savePath"]
    
    def __init__(self, path: str, savePath: str, minEvcsNum: int = 0) -> None:
        self.df, self.n, self.nc = readNode(path, minEvcsNum)
        self.savePath = savePath

        self.df.fillna(0, inplace=True)
        for col in A: self.df[col] *= self.df[POP_DICT[col]]
        self.df.drop(columns=["osmid", "osmid_original", "affectedIncident", "affected"], inplace=True)

        return

    def agg(self, aggType: str) -> None:
        df = self.df.groupby(aggType).sum(numeric_only=True).reset_index()
        for col in A: df[col] /= df[POP_DICT[col]]

        if aggType == "city":
            cityDict = dict(self.df[["city", "iso3"]].drop_duplicates().values)
            df["iso3"] = df["city"].map(cityDict)

        df.to_csv(os.path.join(self.savePath, "{}.csv".format(aggType)), encoding="utf-8", index=False)

        return

if __name__ == "__main__":
    a = calUpperLevel(r"C:\\0_PolyU\\merge.parquet", r"C:\\0_PolyU\\test", 10)
    a.agg("city")
    a.agg("iso3")