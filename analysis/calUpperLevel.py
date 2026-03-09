import os, sys
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from analysis.__readNode import readNode
from analysis.__calRatio import calRatio
from analysis.__setting import A, POP_DICT, AColumns

class calUpperLevel:
    __slots__ = ["df", "n", "nc", "savePath"]
    
    def __init__(self, path: str, savePath: str, minEvcsNum: int = 10, filterBy: str = "city") -> None:
        self.df, self.n, self.nc = readNode(path, minEvcsNum, filterBy, ignoreUneffected=True)
        self.savePath = savePath
        columns = self.df.columns

        self.df.fillna(0, inplace=True)
        if "A_POIAll" in columns and "POI_POIAll" not in columns:
            self.df["POI_POIAll"] = self.df[["POI_1Num", "POI_2Num", "POI_3Num"]].sum(axis=1)

        for col in A:
            if col in columns: self.df[col] *= self.df[POP_DICT[col]]
        self.df.drop(columns=["osmid_original", "affectedIncident", "affected"], inplace=True)

        return

    def agg(self, aggType: str, maxThread: int = 1) -> None:
        df = self.df.groupby(["city", "iso3"] if aggType == "city" else aggType).sum(numeric_only=True).reset_index()
        df["EVCSChange"] = df["EVCSNum_After"] / df["EVCSNum"] * 100 - 100
        for col in A: 
            df[col] /= df[POP_DICT[col]]
            df = df.join(self.calculateGini(col, aggType, maxThread), on=aggType)

        if aggType == "city":
            cityDict = dict(self.df[["city", "iso3"]].drop_duplicates().values)
            df["iso3"] = df["city"].map(cityDict)

        savePath = os.path.join(self.savePath, "{}.csv".format(aggType))
        df.to_csv(savePath, encoding="utf-8", index=False)

        self.calculateChangeRatio(savePath, df)

        return
    
    def calculateGini(self, col: str, aggCol: str, maxThread: int = 1) -> pd.DataFrame:
        """
        Calculate the Gini coefficient to measure inequality in accessibility distribution weighted by population.
        """
        agg = self.df[aggCol].unique().tolist()
        bar = tqdm(total=len(agg), desc="Processing {}".format(col), unit=aggCol)
        result = pd.DataFrame(index=agg)
        resultCol = "{}_Gini".format(col)
        result[resultCol] = np.nan

        grouped = self.df.groupby(aggCol)

        futures = []
        futuresDict = {}
        with ProcessPoolExecutor(max_workers=maxThread) as executor:
            for i, group in grouped:
                # Either population or accessibility arrays are empty
                if group.empty:
                    bar.update()
                    continue

                acc = np.asarray(group[col].values)
                pop = np.asarray(group[POP_DICT[col]].values)

                # Filter out areas with zero population
                mask = pop > 0
                pop = pop[mask]
                acc = acc[mask]

                if len(acc) == 0 or len(pop) == 0:
                    bar.update()
                    continue
                
                future = executor.submit(self.gini, pop, acc)
                futures.append(future)
                futuresDict[future] = i
            
            for future in as_completed(futures):
                i = futuresDict[future]
                try: gini = future.result()
                except Exception as e:
                    raise RuntimeError(e)
                else:
                    # Ensure the result is within [0, 1] bounds
                    result.at[i, resultCol] = max(0, min(gini, 1))
                    bar.update()
        
        bar.close()

        return result
    
    @staticmethod
    def gini(pop: np.ndarray, acc:np.ndarray) -> np.floating:
        # Special case: if only one grid cell has nonzero accessibility
        # Return 0 if all population entries were filtered out
        if np.count_nonzero(acc) == 0: return np.float16(0)
        elif np.count_nonzero(acc) == 1: return np.float16(1)

        # Sort values by accessibility in ascending order
        sortedIndices = np.argsort(acc)
        sortedPop = pop[sortedIndices]
        sortedAcc = acc[sortedIndices]

        # Compute cumulative population and cumulative accessibility
        totalPop = sortedPop.sum()
        totalAcc = sortedAcc.sum()

        # Normalize cumulative population and accessibility (range 0–1)
        cumPop = np.cumsum(sortedPop) / totalPop
        cumAcc = np.cumsum(sortedAcc) / totalAcc
        ## Add 0
        lack0 = cumPop[0] != 0 or cumAcc[0] != 0
        cumPop = np.insert(cumPop, 0, 0) if lack0 else cumPop
        cumAcc = np.insert(cumAcc, 0, 0) if lack0 else cumAcc

        # Compute the Gini coefficient using the trapezoidal rule (Lorenz curve area)
        return np.float64(1 - np.trapezoid(cumAcc, cumPop))
    
    @staticmethod
    def calculateChangeRatio(path: str, df: pd.DataFrame | None = None) -> None:
        dfs = pd.read_csv(path, encoding="utf-8") if df is None else df
        savePath = os.path.dirname(path)

        result = []
        A_BEFORE, A_AFTERs = AColumns("all", "all", 3)
        sub = ["all", "EVCSOnly", "RoadsOnly"]
        for i, A_AFTER in enumerate(A_AFTERs):
            df, cols = calRatio(dfs[["iso3", "city", "EVCSChange"] + A_BEFORE + A_AFTER].copy(), A_BEFORE, A_AFTER, False, False)
            df.rename(columns={x: f"{x}_{sub[i]}" for x in cols}, inplace=True)
            result.append(df)

        result = pd.concat(result, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]
        result.to_csv(os.path.join(savePath, "changeRatio_result.csv"), encoding="utf-8", index=False)

        return

if __name__ == "__main__":
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    MERGE_RESULT = r"C:\0_PolyU\test\merge_3km.parquet"
    calUpperLevel(MERGE_RESULT, ANALY_RESULT, 10, "city").agg("city", 16)