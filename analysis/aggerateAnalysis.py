import sys, os
import pandas as pd
import numpy as np
import seaborn as sns

sys.path.append(".") # Set path to the roots

from _plot import plotSet, plt
from analysis.__readNode import readNode
from analysis import A_POI, A_POP

def aggerateAnalysis(
    path: str | tuple[str, str], analysisType: str,
    savePath: str, saveName: str,
    accOrEquity : str = "accessibility",
    minEvcsNum: int = 0
) -> None:
    if analysisType == "POI":
        A_BEFORE = list(A_POI.before)
        A_AFTER = list(A_POI.after)
    else:
        A_BEFORE = list(A_POP.before)
        A_AFTER = list(A_POP.after)
    
    if accOrEquity == "equity":
        A_BEFORE = ["{}_Gini".format(x) for x in A_BEFORE]
        A_AFTER = ["{}_Gini".format(x) for x in A_AFTER]

    df, n, nc = readNode(path, minEvcsNum, ignoreUneffected=True)
    df = df[A_BEFORE + A_AFTER + ["iso3"]].fillna(0)

    # Ignore isolated vertices and connected components
    df.drop(df.loc[df["city"] == "Isolated"].index, inplace=True) if "city" in df.columns else None

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
    results.to_csv(os.path.join(savePath, f"affectedCount_{accOrEquity}_{saveName}.csv"), encoding="utf-8")

    df.replace(0, np.nan, inplace=True)

    # Plot different group (global)
    plotSet()
    fig, ax = plt.figure("D")
    df[ratio].plot.box(
        ax = ax,
        showfliers = False,
        showmeans = True,
        meanprops = {"markerfacecolor":"lightgreen"}
    )
    plt.plot()
    plt.close()

    # Country base plot
    for a in A_BEFORE:
        col = "{}_changeRatio".format(a)
        fig, ax = plt.figure("H")
        df.dropna(subset=col, inplace=True)
        order = df.groupby("iso3")[col].median().sort_values().index
        print(f"There are {len(order)} countries ({len(order) / nc * 100:.2f}%) suffered decreasing in {accOrEquity}")
        sns.boxplot(
            ax = ax,
            data = df,
            x=col, y="iso3",
            order=order,
            showfliers = False
        )
        plt.plot(os.path.join(savePath, f"{saveName}_{a}_{accOrEquity}"))
        plt.close()

    return

# Debug
if __name__ == "__main__":
    # aggerateAnalysis(r"C:\\0_PolyU\\merge.parquet", r"C:\\0_PolyU\\test", "nodes", minEvcsNum=10)
    # # aggerateAnalysis((r"C:\\0_PolyU\\merge.gpkg", "nodes"), r"C:\\0_PolyU\\test", minEvcsNum=10)

    # aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "pop", r"C:\\0_PolyU\\test", "city", "equity", minEvcsNum=10)
    # aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "POI", r"C:\\0_PolyU\\test", "city", "equity", minEvcsNum=10)
    
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "pop", r"C:\\0_PolyU\\test", "iso3", "equity", minEvcsNum=10)
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "POI", r"C:\\0_PolyU\\test", "iso3", "equity", minEvcsNum=10)