import sys, os
import pandas as pd
import numpy as np
import seaborn as sns

sys.path.append(".") # Set path to the roots

from _plot import plotSet, plt
from analysis.__readNode import readNode
from analysis import A_POI, A_POP

def EVCSAnalysis(path: str, minEvcsNum: int = 10) -> None:
    df, n, nc = readNode(path, minEvcsNum, ignoreUneffected=True)
    df = df[df["EVCSNum"] != 0] if "city" in path else df
    idx = "city" if "city" in path else "iso3"

    plotSet()

    # General condition
    fig, ax = plt.figure("D")
    df[["EVCSNum", "EVCSNum_After"]].plot.box(
        ax = ax,
        showfliers = False,
        showmeans = True,
        meanprops = {"markerfacecolor":"lightgreen"}
    )
    plt.plot()
    plt.close()

    # Change in percentage
    df["EVCSChange"] = df["EVCSNum_After"] / df["EVCSNum"] * 100 - 100
    df.sort_values(by="EVCSChange", inplace=True)
    print(df.shape[0])
    print(df[df["EVCSChange"] == -100].shape[0])
    print(df[df["EVCSChange"] == 0].shape[0])
    fig, ax = plt.figure("D")

    cleandf = df[df["EVCSChange"] != 0]
    sns.histplot(data=cleandf, x="EVCSChange", bins=50, kde=True, 
             color='skyblue', alpha=0.7, edgecolor='white', linewidth=0.5, ax=ax)
    
    # 5. 添加均值和百分位数线
    mean_val = cleandf["EVCSChange"].mean()
    p25 = cleandf["EVCSChange"].quantile(0.25)
    p75 = cleandf["EVCSChange"].quantile(0.75)

    ax.axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'mean: {mean_val:.3f}')
    ax.axvline(p25, color='orange', linestyle=':', linewidth=1.5, alpha=0.8, label=f'25%: {p25:.3f}')
    ax.axvline(p75, color='orange', linestyle=':', linewidth=1.5, alpha=0.8, label=f'75%: {p75:.3f}')

    # sns.violinplot(
    #     data=cleandf,
    #     x = "iso3",
    #     y = "EVCSChange",
    #     ax=ax,
    #     split=True
    # )
    # plt.barh(
    #     y = range(cleandf.shape[0]),
    #     width= cleandf["EVCSChange"],
    # )
    plt.plot()
    plt.close()

    return

# Debug
if __name__ == "__main__":
    EVCSAnalysis(r"C:\\0_PolyU\\test\\city.csv")
    # EVCSAnalysis(r"C:\\0_PolyU\\test\\iso3.csv")