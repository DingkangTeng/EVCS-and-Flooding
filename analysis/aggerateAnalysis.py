import sys, os
import pandas as pd
import numpy as np
import seaborn as sns

sys.path.append(".") # Set path to the roots

from _plot import plotSet, plt
from analysis.readNode import readNode
from analysis import A_AFTER, A_BEFORE, A

def aggerateAnalysis(path: str | tuple[str, str], savePath: str, saveName: str, minEvcsNum: int = 0) -> None:
    df, n, nc = readNode(path, minEvcsNum)
    df = df[A + ["iso3"]].fillna(0)

    ratio = np.ndarray([len(A_BEFORE)], dtype=object)
    results = np.ndarray([len(A_BEFORE)], dtype=pd.DataFrame)
    for i, a in enumerate(A_BEFORE):
        col = "{}_changeRatio".format(a)
        ratio[i] = col
        df[col] = (df[A_AFTER[i]] / df[a] - 1) * 100
        affected = df[col].drop(df.loc[(df[col] == 0) | df[col].isna()].index).shape[0]
        print(f"In {a}, {affected} nodes/region ({affected / n * 100:.2f}%) are affected by flooding globally.")

        # Statistic on the situation of different country
        df["zero"] = ((df[col] == 0) | df[col].isna())
        df["positive"] = ((df[col] != 0) & df[col].notna())
        result = df.groupby("iso3").agg({
            "zero": "sum",
            "positive": "sum"
        }).reset_index()
        result["ratio"] = result["positive"] / (result["positive"] + result["zero"]) * 100
        result.columns = ["iso3", "{}_diff_zero".format(a), "{}_positive".format(a), "{}_changeRatio".format(a)]
        results[i] = result.set_index("iso3")
    
    results = pd.concat(results, axis=1)
    results.to_csv(os.path.join(savePath, "affectedCount_{}.csv".format(saveName)), encoding="utf-8")

    df.replace(0, np.nan, inplace=True)

    # Plot different group (global)
    plotSet()
    fig, ax = plt.figure("D")
    df[ratio].plot.box(
        ax = ax,
        showmeans = True,
        meanprops = {"markerfacecolor":"lightgreen"}
    )
    plt.plot()
    plt.close()

    # Country base plot
    fig, ax = plt.figure("D")
    df.dropna(subset="A_All_changeRatio", inplace=True)
    order = df.groupby("iso3")["A_All_changeRatio"].median().sort_values().index
    print(f"There are {len(order)} countries ({len(order) / nc * 100:.2f}%) suffered decreasing in accessibility")
    sns.boxplot(
        ax = ax,
        data = df,
        x="A_All_changeRatio", y="iso3",
        order=order,
        showfliers = False
    )
    plt.plot()
    plt.close()

    return

# Debug
if __name__ == "__main__":
    # aggerateAnalysis(r"C:\\0_PolyU\\merge.parquet", r"C:\\0_PolyU\\test", "nodes", 10)
    # # aggerateAnalysis((r"C:\\0_PolyU\\merge.gpkg", "nodes"), r"C:\\0_PolyU\\test", 10)
    # aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", r"C:\\0_PolyU\\test", "city", 10)
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", r"C:\\0_PolyU\\test", "iso3", 10)